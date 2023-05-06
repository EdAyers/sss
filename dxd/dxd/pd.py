from dxd.q import Expr, SelectStatement, SelectCore
from dxd.parser import run_parser
from typing import TypeVar, Optional, Literal, Union
from dataclasses import dataclass, replace, field

# initial over pandas-like expressions.

"""

Possible view moves:
- table[predicate] → View
- view.column → TableColumn
- view.select(pattern : P) → Iterable[P]
- table.groupby()
- aggregations; table.column.sum()
"""

S = TypeVar("S")  # bound to a schema


@dataclass
class TableView:
    """table or view over table"""

    _table_name: str
    _filters: list[Expr] = field(default_factory=list)
    _order_by: Optional[Expr] = field(default=None)

    def reduce(self):
        raise NotImplementedError()

    def __getitem__(self, key):
        if isinstance(key, Expr):
            return replace(self, _filters=self._filters + [key])

    def _as_select_statement(self, pattern):
        run_parser(SelectStatement, f"SELECT * FROM {self._table_name}")
        return SelectStatement(
            core=[
                SelectCore(
                    columns=["*"],
                )
            ]
        )

    # getattr with a column name returns a column view.

    # columns; which columns are available.


class Table(TableView):
    @property
    def name(self):
        return self._table_name


class Filtered(View):
    def __init__(self, view: View, predicate: Expr):
        self.view = view
        self.predicate = predicate

    def __repr__(self):
        return f"{repr(self.view)}[{repr(self.predicate)}]"


class Grouped:
    pass
