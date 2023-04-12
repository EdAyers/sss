from dataclasses import dataclass
import sqlite3
from typing import Any
from uuid import uuid4, UUID
from blobular.store import (
    BlobContent,
    LocalFileBlobStore,
    CacheBlobStore,
    SizedBlobStore,
    OnDatabaseBlobStore,
    AbstractBlobStore,
)

from miniscutil.current import Current
from hitsave.common import (
    EvalStore,
    Eval,
    Binding,
    BindingRecord,
    digest_dictionary,
    Symbol,
    ValueBinding,
    Digest,
    CODE_BINDING_KINDS,
)
from hitsave.local.settings import Settings
from hitsave.local.inspection.codegraph import (
    CodeGraph,
    get_binding,
    value_binding_of_object,
)

from dxd import engine_context, Schema, col
from dxd.sqlite_engine import SqliteEngine


""" Everything to do with state in the HitSave local instance. """


@dataclass
class FakeUser(Schema):
    id: UUID = col(primary=True)


class Session(Current):
    id: UUID
    blobstore: AbstractBlobStore
    eval_store: EvalStore

    def __init__(self):
        cfg = Settings.current()
        self.id = uuid4()
        user_id = None
        self.local_db = sqlite3.connect(
            cfg.local_db_path, check_same_thread=False, timeout=10
        )
        engine = SqliteEngine(self.local_db)
        engine_context.set(engine)
        users = FakeUser.create_table()
        self.codegraph = CodeGraph()

        self.eval_store = EvalStore(
            bindings=BindingRecord.create_table(references={"user_id": users}),
            evals=Eval.create_table(references={"user_id": users}),
        )
        result_table = BlobContent.create_table("results", engine)
        blobspath = cfg.local_cache_dir / "blobs"
        blobspath.mkdir(parents=True, exist_ok=True)
        local_file_store = LocalFileBlobStore(blobspath)
        # [todo] add cloud blobstore
        self.blobstore = SizedBlobStore(
            OnDatabaseBlobStore(result_table), local_file_store
        )

        # [todo] ping cloud to say a session has started.

    @classmethod
    def default(cls):
        return cls()

    def fn_hash(self, s: Symbol):
        return digest_dictionary(
            {
                str(dep): get_binding(dep).digest
                for dep in self.codegraph.get_dependencies(s)
            }
        )

    def get_fn_digests(self, s: Symbol):
        dependencies = {
            str(dep): get_binding(dep) for dep in self.codegraph.get_dependencies(s)
        }
        code_dependencies = {
            k: str(v.digest)
            for k, v in dependencies.items()
            if v.kind in CODE_BINDING_KINDS
        }
        bindings_digest = digest_dictionary(code_dependencies)
        closure_dependencies = {
            k: str(v.digest)
            for k, v in dependencies.items()
            if v.kind not in CODE_BINDING_KINDS
        }
        closure_digest = digest_dictionary(closure_dependencies)
        return {
            "symbol": s,
            "bindings_digest": bindings_digest,
            "closure_digest": closure_digest,
        }

        return digest_dictionary(
            {
                str(dep): get_binding(dep).digest
                for dep in self.codegraph.get_dependencies(s)
            }
        )

    def fn_deps(self, s: Symbol) -> dict[Symbol, Binding]:
        """Returns a list of all bindings that the symbol depends on."""
        return {dep: get_binding(dep) for dep in self.codegraph.get_dependencies(s)}

    def deephash(self, obj: Any) -> Digest:
        """Returns a unique hash for the given object.

        If we have done our job right, the hash will be preserved across different Python interpreter sessions.
        It will traverse the whole object tree, so the hash is unique per snapshot of the object data.
        It is also able to hash callables and other typically unhashable objects.
        """
        b = value_binding_of_object(obj)
        d: set[Symbol] = set()
        for s in b.deps:
            d.add(s)
            for ss in self.codegraph.get_dependencies(s):
                d.add(ss)
        dep_dict = {str(s): str(get_binding(s).digest) for s in d}
        dep_dict["___SELF___"] = b.digest
        return digest_dictionary(dep_dict)
