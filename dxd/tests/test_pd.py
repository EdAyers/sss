from datetime import date
from uuid import UUID
from dxd import Schema, col
from dataclasses import dataclass


@dataclass(kw_only=True)
class User(Schema):
    id: UUID = col(primary=True)
    email: str
    birthday: date
