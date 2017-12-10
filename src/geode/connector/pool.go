package connector

import "net"

type Pool struct {
	connection net.Conn
}

func NewPool(c net.Conn) *Pool {
	return &Pool{connection: c}
}

func (this *Pool) GetConnection() net.Conn {
	return this.connection
}

//func (this *Pool) AddServer(host string, port int) {
//}
//
//func (this *Pool) AddLocator(host string, port int) {
//}
