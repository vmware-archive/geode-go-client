package connector

import (
	"net"
	v1 "github.com/gemfire/geode-go-client/protobuf/v1"
)

type AuthenticationError string

func (e AuthenticationError) Error() string {
	return string(e)
}

type Pool struct {
	connection net.Conn
	needsAuthentication bool
	username string
	password string
}

func NewPool(c net.Conn) *Pool {
	return &Pool{
		connection:          c,
		needsAuthentication: false,
	}
}

func (this *Pool) GetUnauthenticatedConnection() (net.Conn, error) {
	return this.connection, nil
}

func (this *Pool) GetConnection() (net.Conn, error) {
	if this.needsAuthentication {
		return this.authenticateConnection()
	}

	return this.connection, nil
}

func (this *Pool) authenticateConnection() (net.Conn, error) {
	creds := make(map[string]string)
	creds["security-username"] = this.username
	creds["security-password"] = this.password

	request := &v1.Message{
		MessageType: &v1.Message_AuthenticationRequest{
			AuthenticationRequest: &v1.AuthenticationRequest{
				Credentials: creds,
			},
		},
	}

	response, err := doOperationWithConnection(this.connection, request)
	if err != nil {
		return nil, err
	}

	if ! response.GetAuthenticationResponse().GetAuthenticated() {
		return nil, AuthenticationError("connection not authenticated")
	}

	this.needsAuthentication = false

	return this.connection, nil
}

func (this *Pool) AddCredentials(username, password string) {
	this.username = username
	this.password = password
	this.needsAuthentication = true
}

//func (this *Pool) AddServer(host string, port int) {
//}
//
//func (this *Pool) AddLocator(host string, port int) {
//}
