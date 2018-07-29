package connector

import (
	"net"
	"fmt"
)

type serverConnectionProvider struct {
	host string
	port int
}

var _ ConnectionProvider = (*serverConnectionProvider)(nil)

func (this *serverConnectionProvider) GetGeodeConnection() *GeodeConnection {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", this.host, this.port))
	if err != nil {
		return nil
	}

	return &GeodeConnection{
		rawConn:            c,
		inUse:              false,
		handshakeDone:      false,
		authenticationDone: false,
	}
}
