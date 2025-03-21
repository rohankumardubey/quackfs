![CI](https://github.com/vinimdocarmo/quackfs/actions/workflows/ci.yml/badge.svg)

# QuackFS

QuackFS is a FUSE-based filesystem that uses PostgreSQL and S3 as a backend for persistence. Its storage model is based on Differential Storage, meaning that it only stores the differences between versions of a file, resulting in storage efficiency and version history.

## Purpose

QuackFS is specialized for DuckDB database files and their associated Write-Ahead Log (WAL) files. It is not intended as a general-purpose filesystem.

It was created for learning purposes and to explore the idea of Differential Storage. For more information see [this blog post](https://motherduck.com/blog/differential-storage-building-block-for-data-warehouse/) from MotherDuck.

> [!WARNING]
> This project is not production-ready. It is a proof of concept and should not be used in production environments.

## Features

- PostgreSQL for reliable metadata persistence
- S3 for data storage
- Differential storage for efficient version handling
- FUSE integration for seamless OS interaction

## Status

This project is currently in development. Some of planned features are:

- [x] Use PostgreSQL for metadata and data persistence
- [x] Use S3 for data storage instead of Postgres
- [ ] Time travel: be able to query the database from old versions
- [ ] Creating new databases from a specific point in time (sharing data with zero copy)
- [ ] Merging of snapshot layers
- [ ] Garbage collection of snapshot layers
- [ ] Fix -test.shuffle 1742466944778699921

## Getting Started

This project is setup using devcontainers. To get started, you can use one of the following options:

- VSCode
- Cursor
- Github Codespaces (quite slow if you use the free plan)

It should work with any other IDE that supports devcontainers, but I've only tested it with the above.

## Usage

To run the storage backend, you can use the following command:

```bash
$ make
```

In another terminal you can run DuckDB CLI to open/create a database in the FUSE mountpoint (default is `/tmp/fuse`):

```bash
$ duckdb /tmp/fuse/db.duckdb
```

Alternatively, you can run our "load" test script (make sure the storage backend is running), because this script will run against the Differential Storage implementation

```bash
$ make load
```

To run all the tests, you can use the following command:

```bash
$ make test
```

For other commands, check the Makefile.
