### What is this?

This is the beginning of a Go client for [Apache Geode](http://github.com/apache/geode).
It uses Geode's new protobuf-based client protocol to communicate with locators and servers.

### Using it

Get the package:

    go get github.com/gemfire/geode-go-client

Write some client code:

```go
package main

import (
    "net"
    "github.com/gemfire/geode-go-client/connector"
    geode "github.com/gemfire/geode-go-client"
    "fmt"
)

func main() {
    var err error
    c, err := net.Dial("tcp", "localhost:40404")
    if err != nil {
        panic(err)
    }

    pool := connector.NewPool()
    pool.AddServer("localhost", 40404)
    // Optionally add user credentials
    pool.AddCredentials("jbloggs", "t0p53cr3t")
    
    conn := connector.NewConnector(pool)
    client := geode.NewGeodeClient(conn)
    if err != nil {
        panic(err)
    }
    fmt.Println("Connected....")

    // Add a primitive type
    client.Put("FOO", "A", 777)

    v, _ := client.Get("FOO", "A")

    // Type assert so that we can use the value
    vx := v.(int32)
    fmt.Printf("Value for A: %d\n", vx)

    client.Remove("FOO", "A")
}
```

Arbitrary structs are converted to JSON when they are `put` into a region:

```go
type MyStruct struct{
    Name string  `json:"name"`
    Age  int     `json:"age"`
}

v := &MyStruct{"Joe", 42}
client.Put("REGION", "Joe", v)
```

Similarly, to retrieve the data:

```go
v := &MyStruct{}
x := client.Get("REGION", "Joe", v)
```

v is optional for Get() and is only used if the data being retrieved is JSON. In the
above example, x (returned from Get()) ends up pointing to v and is thus redundant.

The API only supports manipulating data (get, getAll, put, putAll, size and remove).
It does not support managing regions or other Geode constructs.

Note that values returned will be of type `interface{}`. It is thus the responsibility
of the caller to type assert as appropriate.

#### Querying

OQL queries can be performed by creating a `Query` instance and then making a  call depending
the type of results which will be returned. For example:

```go
q := client.Query("select count(*) from /MYREGION")
val, err := client.QueryForListResult(q)
fmt.Printf("count(*) = %d\n", val[0])
```
(Perhaps counterintuitively, `count(*)` returns its result in a single element list and not as
a single result.)

When querying objects (returned as JSON) from Geode, you need to provide a reference type to the query:

```go
type Person struct{}

q := client.Query("select * from /Employees")
q.Reference = &Person{}
people, err := q.QueryForListResult(q)

// Type assert to get a usable instance
person := people[0].(*Person)
```

#### On the servers

To enable Geode's protobuf support, locators and servers must be started with the
option `geode.feature-protobuf-protocol`.
    
For example:

    $ gfsh start server --name=server1 --J=-Dgeode.feature-protobuf-protocol=true

### Developing

The Geode protobuf support is currently in very active development which means that
this code may not work if you are running against a local Geode build.

In order to update the protobuf bindings you will need the `protoc` tool installed
locally. Assuming you have checked out the [Apache Geode](http://github.com/apache/geode)
repository, set the environment variable `GEODE_CHECKOUT` to this location and
re-generate the bindings:

    $ export GEODE_CHECKOUT=/Users/jbloggs/workspace/geode
    $ go generate connector/protobuf.go
    
The `protobuf.go` file contains `go:generate` directives to do the actual work.

#### Testing

Tests are written in [Ginkgo](http://onsi.github.io/ginkgo/). There are both unit and integration tests.

Unit tests can be executed with:

```
$ ginkgo -r connector
```

Integration tests require a Geode product directory to work:

```
$ export GEODE_HOME=<path to the Geode product directory>
$ ginkgo -r -slowSpecThreshold 60 integration
```
