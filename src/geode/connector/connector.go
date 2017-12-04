package connector

import (
	"net"
	v1 "geode/protobuf/v1"
	"github.com/golang/protobuf/proto"
	"errors"
	"fmt"
	"io"
)

type Connector struct {
	connection net.Conn
}

var MAJOR_VERSION int32 = 1
var MINOR_VERSION int32 = 1

func NewConnector(conn net.Conn) *Connector {
	return &Connector{
		connection: conn,
	}
}

func (this *Connector) Connect() (err error) {
	if this.connection == nil {
		panic("connection is nil")
	}

	// Select protobuf communication
	// Use version 1 of the Geode protobuf protocol definition
	_, err = this.connection.Write([]byte{0x6e, 0x01})
	if err != nil {
		return errors.New(fmt.Sprintf("unable to write magic bytes: %s", err.Error()))
	}

	request := &v1.Request{
		RequestAPI: &v1.Request_HandshakeRequest{
			HandshakeRequest: &v1.HandshakeRequest{
				MajorVersion: MAJOR_VERSION,
				MinorVersion: MINOR_VERSION,
			},
		},
	}

	err = this.writeRequest(request)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to write handshake: %s", err.Error()))
	}

	response, err := this.readResponse()
	if err != nil {
		return errors.New(fmt.Sprintf("unable to read handshake: %s", err.Error()))
	}

	if ! response.GetHandshakeResponse().GetHandshakePassed() {
		return errors.New("handshake did not succeed")
	}

	return nil
}

func (this *Connector) Put(region string, k, v interface{}) (err error) {
	key, err := EncodeValue(k)
	if err != nil {
		return err
	}

	value, err := EncodeValue(v)
	if err != nil {
		return err
	}

	put := &v1.Request{
		RequestAPI: &v1.Request_PutRequest{
			PutRequest: &v1.PutRequest{
				RegionName: region,
				Entry: &v1.Entry{
					Key:   key,
					Value: value,
				},
			},
		},
	}

	_, err = this.call(put)
	if err != nil {
		return err
	}

	return nil
}

func (this *Connector) Get(region string, k interface{}) (interface{}, error) {
	key, err := EncodeValue(k)
	if err != nil {
		return nil, err
	}

	get := &v1.Request{
		RequestAPI: &v1.Request_GetRequest{
			GetRequest: &v1.GetRequest{
				RegionName: region,
				Key:        key,
			},
		},
	}

	response, err := this.call(get)
	if err != nil {
		return nil, err
	}

	v := response.GetGetResponse().GetResult()

	decoded, err := DecodeValue(v)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

func (this *Connector) GetAll(region string, keys []interface{}) (*v1.GetAllResponse, error) {
	encodedKeys, err := EncodeValues(keys)
	if err != nil {
		return nil, err
	}

	getAll := &v1.Request{
		RequestAPI: &v1.Request_GetAllRequest{
			GetAllRequest: &v1.GetAllRequest{
				RegionName: region,
				Key: encodedKeys,
				CallbackArg: nil,
			},
		},
	}

	response, err := this.call(getAll)
	if err != nil {
		return nil, err
	}

	return response.GetGetAllResponse(), nil
}

func (this *Connector) call(request *v1.Request) (*v1.Response, error) {
	err := this.writeRequest(request)
	if err != nil {
		return nil, err
	}

	response, err := this.readResponse()
	if err != nil {
		return nil, err
	}

	if x := response.GetErrorResponse(); x != nil {
		return nil, errors.New(fmt.Sprintf("%s (%d)", x.GetError().Message, x.GetError().ErrorCode))
	}

	return response, nil
}

func (this *Connector) writeRequest(r *v1.Request) (err error) {
	message := &v1.Message{
		MessageType: &v1.Message_Request{
			Request: r,
		},
	}

	p := proto.NewBuffer(nil)
	err = p.EncodeMessage(message)
	if err != nil {
		return err
	}

	_, err = this.connection.Write(p.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (this *Connector) readResponse() (*v1.Response, error) {
	data := make([]byte, 4096)
	bytesRead, err := this.connection.Read(data)
	if err != nil {
		return nil, err
	}

	// Get the length of the message
	m, n := proto.DecodeVarint(data)
	messageLength := int(m) + n

	if messageLength > len(data) {
		t := make([]byte, len(data), messageLength)
		copy(t, data)
		data = t
	}

	for bytesRead < messageLength {
		n, err := io.ReadFull(this.connection, data[bytesRead:messageLength])
		if err != nil {
			return nil, err
		}

		bytesRead += n
	}

	p := proto.NewBuffer(data[0:bytesRead])
	response := &v1.Message{}

	if err := p.DecodeMessage(response); err != nil {
		return nil, err
	}

	return response.GetResponse(), nil
}

func EncodeValues(values []interface{}) ([]*v1.EncodedValue, error) {
	encodedValues := make([]*v1.EncodedValue, len(values))
	for _, k := range values {
		v, err := EncodeValue(k)
		if err != nil {
			return nil, err
		}
		encodedValues = append(encodedValues, v)
	}

	return encodedValues, nil
}

func EncodeValue(val interface{}) (*v1.EncodedValue, error) {
	ev := &v1.EncodedValue{}

	switch k := val.(type) {
	case int:
		ev.Value = &v1.EncodedValue_IntResult{int32(k)}
	case int16:
		ev.Value = &v1.EncodedValue_ShortResult{int32(k)}
	case int32:
		ev.Value = &v1.EncodedValue_IntResult{k}
	case int64:
		ev.Value = &v1.EncodedValue_LongResult{k}
	case byte:
		ev.Value = &v1.EncodedValue_ByteResult{int32(k)}
	case bool:
		ev.Value = &v1.EncodedValue_BooleanResult{k}
	case float64:
		ev.Value = &v1.EncodedValue_DoubleResult{k}
	case float32:
		ev.Value = &v1.EncodedValue_FloatResult{k}
	case []byte:
		ev.Value = &v1.EncodedValue_BinaryResult{k}
	case string:
		ev.Value = &v1.EncodedValue_StringResult{k}
	case *v1.CustomEncodedValue:
		ev.Value = &v1.EncodedValue_CustomEncodedValue{(*v1.CustomEncodedValue)(k)}
	default:
		return nil, errors.New(fmt.Sprintf("unable to encode type: %T", k))
	}

	return ev, nil
}

func DecodeValues(values []*v1.EncodedValue) ([]interface{}, error) {
	decodedValues := make([]interface{}, len(values))
	for _, k := range values {
		v, err := DecodeValue(k)
		if err != nil {
			return nil, err
		}
		decodedValues = append(decodedValues, v)
	}

	return decodedValues, nil
}

func DecodeValue(value *v1.EncodedValue) (interface{}, error) {
	var decodedValue interface{}

	switch v := value.GetValue().(type) {
	case *v1.EncodedValue_IntResult:
		decodedValue = v.IntResult
	case *v1.EncodedValue_ShortResult:
		decodedValue = v.ShortResult
	case *v1.EncodedValue_LongResult:
		decodedValue = v.LongResult
	case *v1.EncodedValue_ByteResult:
		// Protobuf seems to transmit bytes as int32
		decodedValue = uint8(v.ByteResult)
	case *v1.EncodedValue_BooleanResult:
		decodedValue = v.BooleanResult
	case *v1.EncodedValue_DoubleResult:
		decodedValue = v.DoubleResult
	case *v1.EncodedValue_FloatResult:
		decodedValue = v.FloatResult
	case *v1.EncodedValue_BinaryResult:
		decodedValue = v.BinaryResult
	case *v1.EncodedValue_StringResult:
		decodedValue = v.StringResult
	case *v1.EncodedValue_CustomEncodedValue:
		decodedValue = v.CustomEncodedValue
	default:
		return nil, errors.New(fmt.Sprintf("unable to decode type: %T", v))
	}

	return decodedValue, nil
}
