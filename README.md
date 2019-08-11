# seccampDB

## Build & Run
```
$ make; ./main
$ make test  # runs test
```

## Query commands
### Basic operations
| command | meaning |
|:--|:--|
| `set [key] [value]` | insert or update |
| `get [key]`         | read value of the `[key]` |
| `del [key]`         | delete `[key]` |
| `keys`              | get all existing keys |

### Transaction control
* `begin`
* `commit`
* `abort`
