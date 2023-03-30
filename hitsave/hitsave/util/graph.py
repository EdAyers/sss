from typing import Dict, Generator, Generic, Iterator, Tuple, TypeVar, Set
from collections import deque

V = TypeVar("V")
E = TypeVar("E")


class DirectedGraph(Generic[V, E]):
    adj: Dict[V, Dict[V, E]]

    def __init__(self):
        self.adj = dict()

    def add_vertex(self, v: V):
        if v in self.adj:
            return
        self.adj[v] = {}

    def has_vertex(self, v: V):
        return v in self.adj

    def __iter__(self) -> Iterator[Tuple[V, V, E]]:
        for s, ts in self.adj.items():
            for t, e in ts.items():
                yield (s, t, e)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            (s, t) = key
            if s not in self.adj:
                return None
            ts = self.adj[s]
            if t not in ts:
                return None
            return ts[t]
        return self.adj[key]

    def filter_edges(self, pred):
        """Remove edges where `pred` is not True."""
        for s, tes in self.adj.items():
            for t, e in tes.items():
                if not pred(s, t, e):
                    tes.pop(t)
                if len(tes) == 0:
                    self.adj.pop(s)

    def set_edge(self, src: V, tgt: V, e: E):
        self.add_vertex(src)
        self.add_vertex(tgt)
        self.adj[src][tgt] = e

    def reachable_from(self, start: V) -> Iterator[V]:
        yield start
        if not self.has_vertex(start):
            return
        visited = set()
        front = deque()
        front.append(start)
        while len(front) > 0:
            x = front.popleft()
            if x in visited:
                continue
            assert self.has_vertex(x)
            yield x
            visited.add(x)
            front.extend(y for y in self.adj[x].keys() if y not in visited)
