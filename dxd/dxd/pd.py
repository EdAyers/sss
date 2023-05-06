from dxd.q import Expr

# initial over pandas-like expressions.

"""

Possible view moves:
- table[predicate] → View
- table.column → TableColumn
- table.select(pattern : P) → Iterable[P]
- table.groupby()
- aggregations; table.column.sum()
"""


class View:
    """view over a table"""

    def reduce(self):
        raise NotImplementedError()

    def __get_item__(self, *key):
        raise NotImplementedError()

    # getattr with a column name returns a column view.

    # columns; which columns are available.


class Filtered(View):
    def __init__(self, view: View, predicate: Expr):
        self.view = view
        self.predicate = predicate

    def __repr__(self):
        return f"{repr(self.view)}[{repr(self.predicate)}]"


class Grouped:
    pass
