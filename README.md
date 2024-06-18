# lite_fs

[![Package Version](https://img.shields.io/hexpm/v/lite_fs)](https://hex.pm/packages/lite_fs)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/lite_fs/)

```sh
gleam add lite_fs
```
```gleam
import lite_fs
import gleam/io

pub fn main() {
  // localhost:20202/events
  // print events to console when you get them
  lite_fs.start(port: 20202, with: io.debug)
}
```

Further documentation can be found at <https://hexdocs.pm/lite_fs>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
