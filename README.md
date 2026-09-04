
# ScyllaDB Python-rs Driver

This is a client-side driver for [ScyllaDB] written as a thin wrapper around [Rust Driver].
Although optimized for ScyllaDB, the driver is also compatible with [Apache Cassandra®].

This project is in early devlopment. Not ready for production usage.

## Getting Started

### Setting Up Git Hooks

To ensure code quality, we recommend enabling the pre-commit hook that runs static checks and automatic fixes before each commit:

```bash
cp scripts/pre-commit.sh .git/hooks/pre-commit
```

## Examples

Nothing yet :(

## Features and Roadmap

No features for now.

## Getting Help

We invite you to discuss any issues and ask questions on the [ScyllaDB Forum] and the [ScyllaDB Slack].

## Version support

The driver is currently in very early development and not production ready by any means.
Its API will change very frequently, and without warning. There are no stability or quality guarantees.

## Python version support

Our intent is to support Python versions that are:
- Released, so prereleases are not guaranteed to work
- Supported, according to https://devguide.python.org/versions/

There may be a delay between a new Python version coming out and us supporting it.

## Reference Documentation

* [CQL binary protocol] specification version 4

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
- MIT license ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

## Third-party code

Portions of this project include code copied or adapted from the
[PyO3 project](https://github.com/PyO3/pyo3) (version 0.28.x, git commit `8fcf8fc63`),
specifically the coroutine implementation in `src/coroutine/`.

PyO3 is licensed under either of Apache-2.0 or MIT at your option.
Copyright (c) 2023-present PyO3 Project and Contributors.

[ScyllaDB Slack]: http://slack.scylladb.com/
[ScyllaDB Forum]: https://forum.scylladb.com/
[Apache Cassandra®]: https://cassandra.apache.org/
[CQL binary protocol]: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
[ScyllaDB]: https://www.scylladb.com/
[Rust Driver]: https://github.com/scylladb/scylla-rust-driver
