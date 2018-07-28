package connector

import (
	v1 "github.com/gemfire/geode-go-client/protobuf/v1"
	"net"
	"github.com/gemfire/geode-go-client/protobuf"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
)

type AuthenticationError string

func (e AuthenticationError) Error() string {
	return string(e)
}

type GeodeConnection struct {
	rawConn       net.Conn
	handshakeDone bool
}

type ConnectionProvider interface {
	GetGeodeConnection() *GeodeConnection
}

type Pool struct {
	providers           []ConnectionProvider
	needsAuthentication bool
	username            string
	password            string
}

func NewPool(c net.Conn, handshakeDone bool) *Pool {
	p := &Pool{
		needsAuthentication: false,
	}
	p.AddConnection(c, handshakeDone)

	return p
}

func (this *Pool) AddConnection(c net.Conn, handshakeDone bool) {
	gConn := &GeodeConnection{
		rawConn:       c,
		handshakeDone: handshakeDone,
	}
	p := &singleConnectionProvider{gConn}
	this.providers = append(this.providers, p)
}

func (this *Pool) AddLocator(host string, port int) {
}

func (this *Pool) AddServer(host string, port int) {
	this.providers = append(this.providers, &serverConnectionProvider{
		host,
		port,
	})
}

func (this *Pool) GetConnection() (net.Conn, error) {
	var gConn *GeodeConnection
	var err error
	for i := len(this.providers) - 1; i >= 0; i-- {
		gConn = this.providers[i].GetGeodeConnection()
		if gConn == nil {
			this.providers = append(this.providers[:i], this.providers[i+1:]...)
		}
	}

	if ! gConn.handshakeDone {
		err = handshake(gConn.rawConn)
		if err != nil {
			return nil, err
		}
		gConn.handshakeDone = true
	}

	if this.needsAuthentication {
		return this.authenticateConnection(gConn.rawConn)
	}

	return gConn.rawConn, nil
}

func (this *Pool) AddCredentials(username, password string) {
	this.username = username
	this.password = password
	this.needsAuthentication = true
}

func handshake(connection net.Conn) (err error) {
	request := &org_apache_geode_internal_protocol_protobuf.NewConnectionClientVersion{
		MajorVersion: MAJOR_VERSION,
		MinorVersion: MINOR_VERSION,
	}

	err = writeMessage(connection, request)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to write handshake: %s", err.Error()))
	}

	data, err := readRawMessage(connection)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to read handshake: %s", err.Error()))
	}

	p := proto.NewBuffer(data)
	ack := &org_apache_geode_internal_protocol_protobuf.VersionAcknowledgement{}

	if err := p.DecodeMessage(ack); err != nil {
		return err
	}

	if !ack.GetVersionAccepted() {
		return errors.New("handshake did not succeed")
	}

	return nil
}

func (this *Pool) authenticateConnection(connection net.Conn) (net.Conn, error) {
	creds := make(map[string]string)
	creds["security-username"] = this.username
	creds["security-password"] = this.password

	request := &v1.Message{
		MessageType: &v1.Message_HandshakeRequest{
			HandshakeRequest: &v1.HandshakeRequest{
				Credentials: creds,
			},
		},
	}

	response, err := doOperationWithConnection(connection, request)
	if err != nil {
		return nil, err
	}

	if !response.GetHandshakeResponse().GetAuthenticated() {
		return nil, AuthenticationError("connection not authenticated")
	}

	this.needsAuthentication = false

	return connection, nil
}
