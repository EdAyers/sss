# A little ORM for sqlite and postgresql

Existing ORMs seem to be clunky overkill.
Status: experimental.

## Features

- Proper typing of table operations.
- Advanced and strongly-typed query patterning system.
- Just subclass `Table` on your dataclasses and it will work. You don't need special column settings.
- Works with plain Python types, no need to have special column types on everything.
- Works with pydantic BaseModel (WIP)
- Works with sqlite and postgresql (WIP)
- Async and sync execution calls (WIP)

## Other libraries

You should use one of these instead:

- [Peewee](http://docs.peewee-orm.com/en/latest/)
- [SQLAlchemy](https://www.sqlalchemy.org)
- [Piccolo](https://piccolo-orm.com)

Also injection attacks are only defended against user data.
We assume that the definitions of dataclasses and query constructions are trusted.
That is, if you give a field in your schema dataclass a name like "; DROP TABLE" it will execute the injection.


# Developing

```sh
pip install hatch

# build
hatch build

# tests
hatch run test:no-cov

```