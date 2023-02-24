# Monorepo for various python projects

At the moment these are:
- dxd: a post-ORM for postgres and sqlite
- uxu: a pure-python web framework similar in design to Pynecone.
- miniscutil: utility library

But there are more I want to add.

## Should I use these libraries?

No. They are experimental and I will be editing them a lot.



# Development

All development is done with the `hatch` build library

```sh
pip install hatch
```

Then go to a subdirectory.

```sh
cd dxd
# build
hatch build
# test
hatch run test:no-cov
```
