from dataclasses import MISSING, Field, field
import logging
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
)
from .engine import engine_context
from miniscutil.ofdict import MyJsonEncoder, TypedJsonDecoder
from .expr import Expr, AbstractExpr

logger = logging.getLogger("dxd")

S = TypeVar("S")
R = TypeVar("R")


class Column(AbstractExpr):
    field: Field
    schema: Type  # : Schema # cyclic reference

    @property
    def name(self) -> str:
        return self.field.name

    @property
    def type(self) -> Type:
        return self.field.type

    @property
    def primary(self) -> bool:
        """Is it a primary key?"""
        return self.field.metadata.get("primary", False)

    def __init__(self, schema: Type, f: Field):
        self.schema = schema
        cls = f.metadata.get("cls", None)
        if cls is not None:
            assert self.__class__ == cls, f"Column {self.name} is not of type {cls}"
        self.field = f

    def adapt(self, value):
        """Adapt a python value to the sql-accepted value."""
        # [todo] get db protocol
        return engine_context.get().adapt(value)

    def restore(self, sql_value):
        """Get the python value from the sql-stored value."""
        # [todo] replace with adapter pattern.
        r = engine_context.get().restore(self.type, sql_value)
        return r

    @property
    def template(self) -> str:
        """Convert to an Expr template."""
        return self.name

    @property
    def values(self) -> list[Any]:
        """Convert to Expr values."""
        return []

    @property
    def precedence(self) -> int:
        return 100

    @property
    def foreign_key(self) -> Optional["Column"]:
        return self.field.metadata.get("foreign_key", None)

    def get_storage_type(self) -> str:
        """This is the type that you should use to make the column in SQL."""
        return engine_context.get().get_storage_type(self.type)

    @property
    def sql_schema(self):
        s = f"{self.name} {self.get_storage_type()}"
        return s

    def __repr__(self):
        return f"{self.schema.__name__}.{self.name}"

    def __hash__(self):
        return hash((self.schema.__name__, self.name))

    def __eq__(self, other):
        if isinstance(other, Column):
            return hash(self) == hash(other)
        return super().__eq__(other)

    @classmethod
    def of_field(cls, schema, f: Field) -> "Column":
        cls = f.metadata.get("cls", cls)
        return cls(schema, f)


class JsonCol(Column):
    """Column where the value is encoded in the database as JSON."""

    def adapt(self, value):
        return MyJsonEncoder().encode(value)

    def restore(self, sql_value):
        assert isinstance(sql_value, str)
        return TypedJsonDecoder(self.type).decode(sql_value)

    def get_storage_type(self) -> str:
        # [todo] postgres has a fancy type
        return "TEXT"


def col(
    primary=False,
    metadata={},
    default: Any = MISSING,
    default_factory: Union[Callable[[], Any], Literal[MISSING]] = MISSING,
    foreign_key: Any = None,
    encoding: Optional[Literal["json"]] = None,
) -> Any:
    """Create a dataclasses.field with extra column metadata."""
    if default is not MISSING:
        if default_factory is not MISSING:
            raise ValueError("Cannot set both default and default_factory.")
        default_factory = lambda: default
    # [todo] kwonly = true
    if encoding is not None:
        cls = JsonCol
    else:
        cls = Column
    if not any(
        [foreign_key is None, foreign_key is True, isinstance(foreign_key, Column)]
    ):
        raise TypeError("foreign_key must be None, True, or a Column")
    return field(
        metadata={
            **metadata,
            "primary": primary,
            "cls": cls,
            "foreign_key": foreign_key,
        },
        default_factory=default_factory,
    )
