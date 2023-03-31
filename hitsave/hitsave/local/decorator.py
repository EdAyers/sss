from dataclasses import asdict, dataclass, field
from datetime import datetime
import inspect
import pickle
from tempfile import SpooledTemporaryFile
from typing import Any, Callable, Generic, List, Optional, Set, TypeVar, overload
from uuid import uuid4
from hitsave.common import CODE_BINDING_KINDS
from hitsave.local.console import user_info
from hitsave.local.inspection.codegraph import Symbol, get_binding
from functools import update_wrapper

from hitsave.local.session import Session
from hitsave.local.inspection.symbol import symbol_of_object
from hitsave.local.console import logger, internal_error

from hitsave.common import CodeChangedError, Eval, EvalKey, EvalLookupError, Arg, Digest
import time
from hitsave.common import EvalStore, CodeChangedError
from typing_extensions import ParamSpec


# https://peps.python.org/pep-0612
P = ParamSpec("P")
R = TypeVar("R")


def create_arg(sig: inspect.Signature, bas: inspect.BoundArguments) -> list[Arg]:
    if bas.signature != sig:
        raise ValueError(f"Bad signature for {bas}")
    o: List[Arg] = []
    for param in sig.parameters.values():
        is_default = param.name not in bas.arguments
        value = param.default if is_default else bas.arguments[param.name]
        value_digest = Session.current().deephash(value)
        annotation = (
            param.annotation
            if param.annotation is not inspect.Parameter.empty
            else None
        )
        o.append(
            Arg(
                name=param.name,
                value_digest=value_digest,
                is_default=is_default,
                # annotation=annotation,
                kind=param.kind,
                docs=None,  # [todo]
            )
        )
    return o


@dataclass
class SavedFunction(Generic[P, R]):
    func: Callable[P, R]

    debug_mode: bool = field(default=True)
    """ In debug mode, exceptions thrown in HitSave will not be swallowed. """

    is_experiment: bool = field(default=False)
    """ An experiment is a variant of a SavedFunction which will not be deleted by the cache cleaning code. """

    local_only: bool = field(default=False)  # [todo] not used yet
    invocation_count: int = field(default=0)
    """ number of times that the function has been invoked for this session. """
    _fn_hashes_reported: Set[str] = field(default_factory=set)
    _cache: dict[EvalKey, Any] = field(default_factory=dict)  # [todo] use weakref? lru?

    def call_core(self, *args: P.args, **kwargs: P.kwargs) -> R:
        self.invocation_count += 1
        session = Session.current()
        sig = inspect.signature(self.func)
        ba = sig.bind(*args, **kwargs)
        args_digest = session.deephash(ba.arguments)
        pretty_args = create_arg(sig, ba)
        symbol = symbol_of_object(self.func)
        dependencies = session.fn_deps(symbol)
        key = EvalKey(args_digest=args_digest, **session.get_fn_digests(symbol))
        if key in self._cache:
            return self._cache[key]
        evalstore = session.eval_store
        try:
            result_digest = evalstore.get(key)
            with session.blobstore.open(result_digest) as f:
                value = pickle.load(f)
            msg = f"Found cached value for {symbol}."
            if self.invocation_count == 1:
                user_info(msg)
            else:
                logger.debug(msg)
            return value
        except pickle.UnpicklingError:
            logger.exception(f"unpickling failure")
        except EvalLookupError as e:
            if isinstance(e, CodeChangedError):
                if key.bindings_digest not in self._fn_hashes_reported:
                    user_info(f"dependencies changed for ", symbol)
                    self._fn_hashes_reported.add(key.bindings_digest)
                logger.debug(f"dependencies changed for {symbol}")
            logger.debug(f"store miss for {symbol}")
        # if we make it here in the code then we are
        # going to compute the function
        start_time = datetime.utcnow()
        start_process_time = time.process_time_ns()
        eval_id = uuid4()
        evalstore.start(
            key=key,
            id=eval_id,
            is_experiment=self.is_experiment,
            args=pretty_args,
            deps=dependencies,
            start_time=start_time,
            session_id=session.id,
        )
        # [todo] catch, log and rethrow errors raised by inner func.
        try:
            result = self.func(*args, **kwargs)
        except Exception as e:
            evalstore.reject(
                eval_id,
            )
            raise e
        end_process_time = time.process_time_ns()
        elapsed_process_time = end_process_time - start_process_time
        try:
            with SpooledTemporaryFile() as f:
                pickle.dump(result, f)
                f.seek(0)
                info = session.blobstore.add(f)
            evalstore.resolve(
                id=eval_id,
                elapsed_process_time=elapsed_process_time,
                result_digest=Digest(info.digest),
                result_length=info.content_length,
            )
            logger.debug(f"Computed value for {key}.")
            self._cache[key] = result
            return result
        except pickle.PicklingError:
            internal_error(f"pickle failure for {key}")
            evalstore.reject(eval_id)
            return result

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if self.debug_mode:
            return self.call_core(*args, **kwargs)
        try:
            return self.call_core(*args, **kwargs)
        except Exception as e:
            internal_error(
                "Unhandled exception, falling back to decorator-less behaviour.\n", e
            )
            return self.func(*args, **kwargs)

    @property
    def symbol(self):
        return symbol_of_object(self.func)


@overload
def memo(func: Callable[P, R]) -> SavedFunction[P, R]:
    ...


@overload
def memo(
    *, local_only: bool = False
) -> Callable[[Callable[P, R]], SavedFunction[P, R]]:
    ...


def memo(func=None, **kwargs):  # type: ignore
    """Memoise a function on the cloud."""
    if func == None:
        return lambda func: memo(func, **kwargs)
    if callable(func):
        g = update_wrapper(SavedFunction(func, **kwargs), func)
        return g
    raise TypeError(
        f"@{memo.__name__} requires that the given saved object {func} is callable."
    )


@overload
def experiment(func: Callable[P, R]) -> SavedFunction[P, R]:
    ...


@overload
def experiment() -> Callable[[Callable[P, R]], SavedFunction[P, R]]:
    ...


def experiment(func=None, **kwargs):  # type: ignore
    """Define an experiment that saves to the cloud.

    `@experiment` behaves the same as `@memo`, the difference is that experiments are never deleted
    from the server. Also, by default experiments track the creation of artefacts such as logs and runs.
    """
    return memo(func=func, is_experiment=True, **kwargs)  # type: ignore
