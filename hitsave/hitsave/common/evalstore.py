from dataclasses import fields
from datetime import datetime
from functools import reduce
import logging
import pickle
from typing import Any, Iterator, Literal, NewType, Optional, ClassVar
from uuid import UUID

from dxd import transaction, Table

from .symbol import Symbol
from .binding import Binding, BindingRecord
from .digest import Digest
from .eval import (
    CodeChangedError,
    Eval,
    EvalKey,
    EvalLookupError,
    EvalStatus,
    Arg,
)

logger = logging.getLogger("hitsave")


class EvalStore:
    bindings: Table[BindingRecord]
    evals: Table[Eval]

    # [todo] in-mem cache EvalKey → python value.
    def __init__(self, *, bindings, evals):
        self.bindings = bindings
        self.evals = evals

    def clean_started_evals(self):
        """Delete all the evals that have not finished.

        This is useful to clear out aborted evaluations."""
        self.evals.delete(where=Eval.status == EvalStatus.started)

    def len_evals(self):
        return len(self.evals)

    def __len__(self):
        """Returns number of evals in the table"""
        return self.len_evals()

    def get_symbols(self) -> Iterator[Symbol]:
        return self.evals.select(
            select=Eval.symbol,
            distinct=True,
        )

    def delete(self, id: UUID):
        self.evals.delete(where=Eval.id == id)

    def select(self, **kwargs):
        where = []
        for f in fields(EvalKey):
            v = kwargs.get(f.name, None)
            if v is not None:
                if not isinstance(v, f.type):
                    raise TypeError(
                        f"{f.name} must be {f.type.__name__}, not {type(v).__name__}"
                    )
                where.append(getattr(Eval, f.name) == v)
        if len(where) == 0:
            raise ValueError("No filters specified")
        where = reduce(lambda x, y: x & y, where)

        return self.evals.select(
            where=where,
            order_by=Eval.start_time,
        )

    def get(self, key: EvalKey) -> Digest:
        """Gets the latest Eval for a given key.

        If multiple evals exist with the same key, the most recently evaluated one is returned.

        Raises:
            EvalLookupError: if the eval does not exist.
        """
        # [todo]; in-mem caching goes here.
        symbol = key.symbol
        binding_digest = key.bindings_digest
        closure_digest = key.closure_digest
        args_digest = key.args_digest
        with transaction():
            es = self.evals.select(
                where={
                    Eval.symbol: symbol,
                    Eval.bindings_digest: binding_digest,
                    Eval.closure_digest: closure_digest,
                    Eval.args_digest: args_digest,
                    Eval.status: EvalStatus.resolved,  # [todo] rejected evals should reject immediately.
                },
                order_by=Eval.start_time,
                descending=True,
            )
            for e in es:
                rd = e.result_digest
                assert rd is not None
                return rd
            # find the most recent evals where the binding id doesn't match.
            x = self.evals.select_one(
                where={
                    Eval.symbol: symbol,
                    Eval.args_digest: args_digest,
                    Eval.status: EvalStatus.resolved,
                },
                order_by=Eval.start_time,
                descending=True,
            )

            if x is not None:
                assert isinstance(x.dependencies, dict)
                deps1 = {}
                for s, digest in x.dependencies.items():
                    # [todo] this should really be done by having a third table joining evals to bindings
                    # [todo] implement joins for tinyorm using pandas-style joins.
                    b = self.bindings.select_one(
                        where={
                            BindingRecord.digest: digest,
                        }
                    )
                    if b is None:
                        raise CodeChangedError()
                    if x is not None:
                        deps1[str(s)] = b.diffstr
                # [todo] different message when the _closure_ changes.
                raise CodeChangedError(old_deps=deps1, new_deps=None)

            e = self.evals.select_one(
                where={
                    Eval.symbol: symbol,
                    Eval.binding_digest: binding_digest,
                    Eval.status: EvalStatus.resolved,
                }
            )
            if e is not None:
                raise EvalLookupError(f"unseen arguments for {symbol}")
            else:
                raise EvalLookupError(f"no evaluations for {symbol} found")

    def start(
        self,
        key: EvalKey,
        id: UUID,
        *,
        is_experiment: bool = False,
        args: Optional[list[Arg]] = None,
        deps: dict[Symbol, Binding],
        start_time: Optional[datetime] = None,
    ) -> UUID:
        """Tell the evalstore that we have started a new evaluation."""
        # [todo] enforce this: deps is Dict[symbol, digest]
        # note: we don't bother storing args locally.
        symbol = key.symbol
        binding_digest = key.bindings_digest
        args_digest = key.args_digest
        closure_digest = key.closure_digest
        start_time = start_time or datetime.now()

        with transaction():
            bindings: list[BindingRecord] = [
                BindingRecord(
                    kind=b.kind, diffstr=b.diffstr, digest=b.digest, deps=b.deps
                )
                for bs, b in deps.items()
            ]
            self.bindings.insert_many(bindings, or_ignore=True)
            existing_evals = list(
                self.evals.select(
                    where={
                        Eval.symbol: symbol,
                        Eval.args_digest: args_digest,
                        Eval.bindings_digest: binding_digest,
                        Eval.closure_digest: closure_digest,
                        Eval.status: EvalStatus.started,
                    },
                    select=Eval.id,
                )
            )
            if len(existing_evals) > 0:
                """This can happen when:
                - The app was killed before rejecting the evals. In this case; delete the row.
                  On startup we should delete any started eval rows.
                - We are running with concurrency, and the function has already been entered.
                  In which case the other threads should block / await until it's done.

                We can tell these scenarios apart by checking the session-id of the eval.
                [todo] in this case,
                """
                logger.debug(f"eval already started: {str(key)}")
            dependencies = {k: v.digest for k, v in deps.items()}
            # args = args and [visualize_rec(x) for x in e.get("args", [])]
            self.evals.insert_one(
                Eval(
                    symbol=symbol,
                    bindings_digest=binding_digest,
                    closure_digest=closure_digest,
                    args_digest=args_digest,
                    dependencies=dependencies,
                    result_digest=None,
                    status=EvalStatus.started,
                    start_time=start_time,
                    is_experiment=is_experiment,
                    id=id,
                    args=args,
                ),
            )
            return id

    def get_running_eval(self, key: EvalKey) -> Optional[UUID]:
        return self.evals.select_one(
            where={
                Eval.symbol: key.symbol,
                Eval.bindings_digest: key.bindings_digest,
                Eval.args_digest: key.args_digest,
                Eval.closure_digest: key.closure_digest,
                Eval.status: EvalStatus.started,
            },
            select=Eval.id,
        )

    def resolve(
        self,
        id: UUID,
        *,
        result_digest: Digest,
        result_length: int,
        elapsed_process_time: int,
    ):
        """Tell the evalstore that the evaluation has completed."""
        # [todo] add result to in-mem cache here.
        assert isinstance(id, UUID)
        with transaction():
            i = self.evals.update(
                {
                    Eval.status: EvalStatus.resolved,
                    Eval.result_digest: result_digest,
                    Eval.result_length: result_length,
                    Eval.elapsed_process_time: elapsed_process_time,
                },
                where=Eval.id == id,
            )
            if i == 0:
                raise KeyError(f"Failed to find an evaluation with id {id}")

    def reject(
        self,
        id: UUID,
        *,
        reason: str = "rejected",
        elapsed_process_time: Optional[int] = None,
    ):
        """Tell the evalstore that the evaluation has been rejected."""
        assert isinstance(id, UUID)
        # [todo] store the error message
        n = self.evals.update(
            {
                Eval.status: EvalStatus.rejected,
                Eval.elapsed_process_time: elapsed_process_time,
            },
            where=Eval.id == id,
        )
        if n == 0:
            raise KeyError(f"Failed to find evaluation with id {id}")

    def clear(self):
        self.bindings.clear()
        self.evals.clear()
        # [todo] clear the caches too.

    # [todo] import_eval for when you download an eval from cloud. maybe all evals should be pulled at once.
    # [todo] if the HitSave local server is running, ping it saying there is new data.
