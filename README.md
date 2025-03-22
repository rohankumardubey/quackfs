![CI](https://github.com/vinimdocarmo/quackfs/actions/workflows/ci.yml/badge.svg)

# QuackFS

![QuackFS Logo](./quackfs-logo.png)

QuackFS is a FUSE-based filesystem that uses PostgreSQL and S3 as a backend for persistence. Its storage model is based on Differential Storage, meaning that it only stores the differences between write operations, which allows for time-travel, zero-copy sharing and efficient versioning.

## Purpose

QuackFS is specialized for DuckDB database files and their associated Write-Ahead Log (WAL) files. It is not intended as a general-purpose filesystem.

It was created for learning purposes and to explore the idea of Differential Storage. I was greatly inspired by [this blog post](https://motherduck.com/blog/differential-storage-building-block-for-data-warehouse/) from MotherDuck. 🦆

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
- [ ] Add proper database indexing
- [ ] Fix -test.shuffle 1742466944778699921

## Getting Started

This project is setup using [Dev Containers](https://containers.dev/). To get started, you can use one of the following editors:

- VSCode
- Cursor
- Github Codespaces (quite slow if you use the free plan)

Install the required extension first [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers). It should work with any other IDE that supports Dev Containers, but I've only tested it with the above.

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
