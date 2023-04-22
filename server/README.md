# server

Server component for blobular and hitsave.
Originally, they were going to have separate server libraries but really it's just much easier to
do them at the same time.

Make sure `hatch` is installed.

## Development

Make a `.env` file.

Start the dev server with

```sh
hatch run dev
```

## Production

Similar but run `hatch run prod`.
The difference is that logs are sent to a file and there is less debug logging.
And there is no live reloading.

## Db management

By default, the database is SQLite and file cache are stored in data/blobs.
It should be possible to use postgres instead by setting `database_mode` to `postgres`.

In my prod environment, I'm using sqlite and [litestream](https://litestream.io) to back it up.
It's just much faster and simpler than managing a postgres instance.

Small blobs are stored directly on the database, larger blobs are stored on S3, with a local file cache expanding to fill the local disk.
In the future I want to instead use a more robust and secure blob storage system like Infinitree.