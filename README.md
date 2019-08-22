# seccampDB

## Build & Run
```
$ make
$ ./main
```

`main` generates following 3 files:
* `.seccampDB_dump` : stores data for persistency
* `.seccampDB_log` : stores redo log
* `seccampDB_graph.dot` : keeps conflict graph of transaction history in dot format for visualization

### test
```
$ make test
```

## Visualization of conflict graph
[Graphviz](https://www.graphviz.org) is required.

```
$ dot -T png seccampDB_graph.dot -o seccampDB_graph.png
```
