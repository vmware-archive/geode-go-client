package connector

import (
	"net"
	"sync"
	"errors"
)

type AuthenticationError string

func (e AuthenticationError) Error() string {
	return string(e)
}

type ConnectionProvider interface {
	GetGeodeConnection() *GeodeConnection
}

type Pool struct {
	sync.RWMutex
	recentConnections     []*GeodeConnection
	providers             []ConnectionProvider
	authenticationEnabled bool
	username              string
	password              string
}

func NewPool(c net.Conn, handshakeDone bool) *Pool {
	p := &Pool{
		authenticationEnabled: false,
	}
	p.AddConnection(c, handshakeDone)

	return p
}

func (this *Pool) AddConnection(c net.Conn, handshakeDone bool) {
	gConn := &GeodeConnection{
		rawConn:            c,
		handshakeDone:      handshakeDone,
		authenticationDone: false,
		inUse:              false,
	}

	this.recentConnections = append(this.recentConnections, gConn)
}

func (this *Pool) AddLocator(host string, port int) {
}

func (this *Pool) AddServer(host string, port int) {
	this.providers = append(this.providers, &serverConnectionProvider{
		host,
		port,
	})
}

func (this *Pool) GetConnection() (*GeodeConnection, error) {
	var gConn *GeodeConnection
	var err error

	this.Lock()
	defer this.Unlock()

	// First let's check the recent connections
	for _, c := range this.recentConnections {
		if ! c.inUse {
			gConn = c
		}
	}

	if gConn == nil {
		return nil, errors.New("no connections available")
	}

	//var err error
	//for i := len(this.providers) - 1; i >= 0; i-- {
	//	gConn = this.providers[i].GetGeodeConnection()
	//	if gConn == nil {
	//		this.providers = append(this.providers[:i], this.providers[i+1:]...)
	//	}
	//}

	err = gConn.handshake()
	if err != nil {
		this.discardConnection(gConn)
		return nil, err
	}

	if this.authenticationEnabled {
		err = gConn.authenticate(this.username, this.password)
		if err != nil {
			this.discardConnection(gConn)
			return nil, err
		}
	}

	gConn.inUse = true

	return gConn, nil
}

func (this *Pool) ReturnConnection(gConn *GeodeConnection) {
	this.Lock()
	defer this.Unlock()

	gConn.inUse = false
}

// MUST hold the pool lock when calling
func (this *Pool) discardConnection(gConn *GeodeConnection) {
	for i, c := range this.recentConnections {
		if gConn == c {
			this.recentConnections = append(this.recentConnections[:i], this.recentConnections[i+1:]...)
			break
		}
	}

	gConn.rawConn.Close()
}

func (this *Pool) AddCredentials(username, password string) {
	this.username = username
	this.password = password
	this.authenticationEnabled = true
}
