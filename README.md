This demonstrates how to run the Wasm-compiled SQLite VM in the Go program and interact with it without CGO.

Note: this is intended to be used as a demonstration for [my talk at GopherCon 2022](https://www.gophercon.com/agenda/session/944206).


```shell
$ go run main.go

user: id=0, name='go'
user: id=1, name='zig'
user: id=2, name='whatever'
```
