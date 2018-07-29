package connector

import (
	"net"
	"github.com/gemfire/geode-go-client/protobuf"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	v1 "github.com/gemfire/geode-go-client/protobuf/v1"
)

type GeodeConnection struct {
	rawConn            net.Conn
	handshakeDone      bool
	authenticationDone bool
	inUse              bool
}

func (this *GeodeConnection) GetRawConnection() net.Conn {
	return this.rawConn
}

func (this *GeodeConnection) handshake() (err error) {
	if this.handshakeDone {
		return nil
	}

	request := &org_apache_geode_internal_protocol_protobuf.NewConnectionClientVersion{
		MajorVersion: MAJOR_VERSION,
		MinorVersion: MINOR_VERSION,
	}

	err = writeMessage(this.rawConn, request)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to write handshake: %s", err.Error()))
	}

	data, err := readRawMessage(this.rawConn)
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

	this.handshakeDone = true

	return nil
}

func (this *GeodeConnection) authenticate(username, password string) error {
	if this.authenticationDone {
		return nil
	}

	creds := make(map[string]string)
	creds["security-username"] = username
	creds["security-password"] = password

	request := &v1.Message{
		MessageType: &v1.Message_HandshakeRequest{
			HandshakeRequest: &v1.HandshakeRequest{
				Credentials: creds,
			},
		},
	}

	response, err := doOperationWithConnection(this.rawConn, request)
	if err != nil {
		return err
	}

	if !response.GetHandshakeResponse().GetAuthenticated() {
		return AuthenticationError("connection not authenticated")
	}

	this.authenticationDone = true

	return nil
}
