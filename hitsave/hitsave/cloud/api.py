from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from uuid import UUID
from blobular.api.persist import User, BlobularApiDatabase
from dxd import Table
from dxd import transaction
from hitsave.common import Session, Eval, BindingRecord, Symbol, EvalStatus
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter()

# remember: the difference between put and post is that PUT is idempotent.


@dataclass
class AppState(BlobularApiDatabase):
    sessions: Table[Session]
    bindings: Table[BindingRecord]
    evals: Table[Eval]

    def __init__(self):
        super().__init__()

    def connect(self):
        super().connect()
        engine = self.engine
        self.sessions = Session.create_table(
            "hitsave_sessions", engine, {Session.user_id: self.users}
        )
        self.bindings = BindingRecord.create_table(
            "hitsave_bindings",
            engine,
            {
                BindingRecord.user_id: self.users,
            },
        )
        self.evals = Eval.create_table(
            "hitsave_evals",
            engine,
            {
                Eval.user_id: self.users,
            },
        )


@router.put("/session")
def put_session(session: Session):
    """Start a new session. If the session id already exists, it will throw a 409."""
    ...


@router.put("/eval")
def put_eval(eval: Any, db: AppState, user: User):
    """Update an eval in the eval table.

    Raises:
      400:
        - The eval id is already in use (perhaps by a different user) or,
        - some other validation error
      401: if the user is not authenticated.
      403: if the user is not allowed to update the eval because it belongs to a different user
      409: the eval's update is an invalid state change or causes a conflict

    """
    # [todo] sketch of how it works below.
    # [todo] this is really gnarly, and it's just boilerplate.
    # There should be a way in dxd to just sync up rows to a cloud service.

    # validate the eval
    if eval.user_id != user.id:
        raise HTTPException(status_code=403)
    session = db.sessions.select_one(
        where={Session.id: eval.session_id, Session.user_id: user.id}
    )
    if session is None:
        raise HTTPException(
            status_code=400,
            message=f"Session not found. You need to start a new session by putting to /session. session_id: {eval.session_id}",
        )
    if eval.start_time > datetime.utcnow():
        raise HTTPException(
            status_code=400,
            message=f"Eval's start time is in the future. eval_id: {eval.id}",
        )
    # [todo] validate bindings etc
    # [todo] move validation logic to pydantic
    with transaction():
        old_eval = db.evals.select_one(
            where=(Eval.id == eval.id) & (Eval.user_id == eval.user_id)
        )
        if old_eval is None:
            # [todo] assert it's a total eval.
            db.evals.insert_one(eval)
            return  # [todo] 201 Created
        valid_transitions = [
            (EvalStatus.started, EvalStatus.resolved),
            (EvalStatus.started, EvalStatus.rejected),
        ]
        if (
            old_eval.status != eval.status
            or (old_eval.status, eval.status) not in valid_transitions
        ):
            raise HTTPException(
                status_code=409,
                message=f"invalid eval state change. eval_id: {eval.id}",
            )
        updatable_fields = [
            "result_digest",
            "result_length",
            "elapsed_process_time",
            "args",
            "status",
        ]
        update_dict = {}
        for k in updatable_fields:
            if hasattr(eval, k):
                if getattr(old_eval, k) is not None:
                    raise HTTPException(status_code=409, message=f"{k} is already set")
                update_dict[k] = getattr(eval, k)

        db.evals.update(
            update_dict, where=(Eval.id == eval.id) & (Eval.user_id == user.id)
        )
    raise NotImplementedError()


@router.get("/eval/{eval_id}")
def get_eval(eval_id: UUID):
    """Get the eval with the given id.

    Raises:
      400: invalid request
      401: if the user is not authenticated
      403: if the user is not allowed to get the eval because it belongs to a different user
      404: no eval with the given id exists or was deleted
    """
    raise NotImplementedError()


@router.get("/eval")
def get_evals(
    symbol: Optional[Symbol] = None,
    args_digest: Optional[str] = None,
    binding_digest: Optional[str] = None,
    closure_digest: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Get all the evals for the given symbol.

    Returns a list of eval objects that match with the provided parameters.
    Evaluations are ordered by their creation time with the most recent first.
    If nothing is found then an empty list is returned.

    Only evaluations that the user is authorized to see can be returned.

    Raises:
     400: invalid request
     401: if the user is not authenticated
    """
    raise NotImplementedError()
