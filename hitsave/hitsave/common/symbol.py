from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Symbol:
    """A Symbol is a string that the Python interpreter uses to refer to a Python object."""

    module_name: str
    decl_name: Optional[str] = field(default=None)

    def __post_init__(self):
        # [todo] should emit a warning.
        assert isinstance(self.module_name, str)
        assert (self.decl_name is None) or isinstance(self.decl_name, str)
        assert ":" not in self.module_name
        assert self.decl_name is None or ":" not in self.decl_name
        assert self.module_name is not "__main__", "Unresolved __main__."

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if not isinstance(other, Symbol):
            return False
        return self.__str__() == other.__str__()

    def __str__(self):
        module_name = self.module_name
        if self.decl_name is None:
            return module_name
        else:
            return f"{module_name}:{self.decl_name}"

    def __rich__(self):
        """Pretty print with nice formatting."""
        module_name = self.module_name

        m = ".".join([f"[cyan]{n}[/]" for n in module_name.split(".")])
        d = (
            ".".join([f"[yellow]{n}[/]" for n in self.decl_name.split(".")])
            if self.decl_name is not None
            else ""
        )
        return f"{m}:{d}"

    @classmethod
    def of_str(cls, s: str):
        """Parse a Symbol from a string "module_name:decl_name"."""
        if ":" not in s:
            return cls(s, None)
        module_name, decl_name = s.split(":", 1)
        return cls(module_name, decl_name)

    def __todict__(self):
        return str(self)

    def __conform__(self, protocol):
        return str(self)

    @classmethod
    def __adapt__(cls, item):
        if isinstance(item, cls):
            return item
        elif isinstance(item, str):
            return cls.of_str(item)
        else:
            raise TypeError(f"Expected str, got {type(item)}")
