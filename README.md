### What is this?

This is the beginning of a Go client for [Apache Geode](http://github.com/apache/geode). It uses Geode's new protobuf-based client protocol
to communicate with locators and servers.

### Using it

Get the package:

    go get github.com/gemfire/geode-go-client

Write some client code:

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

        p := connector.NewPool(c)
        conn := connector.NewConnector(p)
        client := geode.NewGeodeClient(conn)
        err = client.Connect()
        if err != nil {
            panic(err)
        }
        fmt.Println("Connected....")

        client.Put("FOO", "A", 777)

        v, _ := client.Get("FOO", "A")

        // Type assert so that we can use the value
        vx := v.(int32)
        fmt.Printf("Value for A: %d\n", vx)

        client.Remove("FOO", "A")
	}

The API only supports manipulating data (get, getAll, put, putAll, size and remove). It does not support managing regions or other Geode constructs.

Note that values returned will be of type `interface{}`. It is thus the responsibility of the caller to type assert as appropriate.

To enable Geode's protobuf support, locators and servers must be started with the option `geode.feature-protobuf-protocol`.
    
For example:

    $ gfsh start server --name=server1 --J=-Dgeode.feature-protobuf-protocol=true

### Developing

The Geode protobuf support is currently in very active development which means that this code may not work if you are running against a local Geode build.

In order to update the protobuf bindings you will need to use the `protoc` tool. Assuming you have checked out this repository:

    protoc --proto_path=$PATH_TO_GEODE_CHECKOUT/geode-protobuf-messages/src/main/proto \
      --go_out=protobuf \
      handshake.proto
    protoc --proto_path=$PATH_TO_GEODE_CHECKOUT/geode-protobuf-messages/src/main/proto \
      --go_out=protobuf \
      v1/{basicTypes,clientProtocol,connection_API,locator_API,region_API}.proto

