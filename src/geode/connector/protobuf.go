package connector

import (
	"net"
	v1 "geode/protobuf/v1"
	"github.com/golang/protobuf/proto"
	"errors"
	"fmt"
	"io"
	"reflect"
)

// A Protobuf connector provides the low-level interface between a Client and the backend Geode servers.
// It should not be used directly; rather the Client API should be used.
type Protobuf struct {
	pool *Pool
}

var MAJOR_VERSION int32 = 1
var MINOR_VERSION int32 = 1

func NewConnector(pool *Pool) *Protobuf {
	return &Protobuf{
		pool: pool,
	}
}

func (this *Protobuf) Handshake() (err error) {
	connection := this.pool.GetConnection()

	// Select protobuf communication
	// Use version 1 of the Geode protobuf protocol definition
	_, err = connection.Write([]byte{0x6e, 0x01})
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

	err = this.writeRequest(connection, request)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to write handshake: %s", err.Error()))
	}

	response, err := this.readResponse(connection)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to read handshake: %s", err.Error()))
	}

	if ! response.GetHandshakeResponse().GetHandshakePassed() {
		return errors.New("handshake did not succeed")
	}

	return nil
}

func (this *Protobuf) Put(region string, k, v interface{}) (err error) {
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

	_, err = this.doOperation(put)
	if err != nil {
		return err
	}

	return nil
}

func (this *Protobuf) Get(region string, k interface{}) (interface{}, error) {
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

	response, err := this.doOperation(get)
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

func (this *Protobuf) GetAll(region string, keys interface{}) (map[interface{}]interface{}, map[interface{}]error, error) {
	keySlice := reflect.ValueOf(keys)
	if keySlice.Kind() != reflect.Slice && keySlice.Kind() != reflect.Array {
		return nil, nil, errors.New("keys must be a slice or array")
	}

	encodedKeys := make([]*v1.EncodedValue, 0, keySlice.Len())
	for i := 0; i < keySlice.Len(); i++ {
		key, err := EncodeValue(keySlice.Index(i).Interface())
		if err != nil {
			return nil, nil, err
		}

		encodedKeys = append(encodedKeys, key)
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

	response, err := this.doOperation(getAll)
	if err != nil {
		return nil, nil, err
	}

	decodedEntries := make(map[interface{}]interface{})
	decodedFailures := make(map[interface{}]error)

	for _, entry := range response.GetGetAllResponse().Entries {
		key, err := DecodeValue(entry.Key)
		if err != nil {
			return nil, nil, errors.New(fmt.Sprintf("unable to decode GetAll response key: %s", err.Error()))
		}

		value, err := DecodeValue(entry.Value)
		if err != nil {
			decodedFailures[key] = errors.New(fmt.Sprintf("unable to decode GetAll value for key: %v: %s", key, err.Error()))
			continue
		}

		decodedEntries[key] = value
	}

	for _, failure := range response.GetGetAllResponse().Failures {
		key, err := DecodeValue(failure.Key)
		if err != nil {
			return nil, nil, errors.New(fmt.Sprintf("unable to decode GetAll failure response for key: %v: %s", failure.Key, err.Error()))
		}

		decodedFailures[key] = errors.New(fmt.Sprintf("%s (%d)", failure.Error.Message, failure.Error.ErrorCode))
	}

	if len(decodedFailures) == 0 {
		return decodedEntries, nil, nil
	}

	return decodedEntries, decodedFailures, nil
}

func (this *Protobuf) PutAll(region string, entries interface{}) (map[interface{}]error, error) {
	// Check if we have a map
	entriesMap := reflect.ValueOf(entries)
	if entriesMap.Kind() != reflect.Map {
		return nil, errors.New("entries must be a map")
	}

	encodedEntries := make([]*v1.Entry, 0)

	for _, k := range entriesMap.MapKeys() {
		key, err := EncodeValue(k.Interface())
		if err != nil {
			return nil, err
		}

		value, err := EncodeValue(entriesMap.MapIndex(k).Interface())
		if err != nil {
			return nil, err
		}

		e := &v1.Entry{
			Key: key,
			Value: value,
		}

		encodedEntries = append(encodedEntries, e)
	}

	putAll := &v1.Request{
		RequestAPI: &v1.Request_PutAllRequest{
			PutAllRequest: &v1.PutAllRequest{
				RegionName: region,
				Entry: encodedEntries,
			},
		},
	}

	r, err := this.doOperation(putAll)
	if err != nil {
		return nil, err
	}

	response := r.GetPutAllResponse()
	failures := make(map[interface{}]error)
	for _, k := range response.GetFailedKeys() {
		key, err := DecodeValue(k.Key)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("unable to decode failed PutAll response key: %s", err.Error()))
		}

		failures[key] = errors.New(fmt.Sprintf("%s (%d)", k.GetError().Message, k.GetError().ErrorCode))
	}

	if len(failures) == 0 {
		return nil, nil
	}

	return failures, nil
}

func (this *Protobuf) Remove(region string, k interface{}) error {
	key, err := EncodeValue(k)
	if err != nil {
		return err
	}

	remove := &v1.Request{
		RequestAPI: &v1.Request_RemoveRequest{
			RemoveRequest: &v1.RemoveRequest{
				RegionName: region,
				Key:        key,
			},
		},
	}

	_, err = this.doOperation(remove)

	return err
}

func (this *Protobuf) RemoveAll(region string, keys interface{}) error {
	keySlice := reflect.ValueOf(keys)
	if keySlice.Kind() != reflect.Slice && keySlice.Kind() != reflect.Array {
		return errors.New("keys must be a slice or array")
	}

	encodedKeys := make([]*v1.EncodedValue, 0, keySlice.Len())
	for i := 0; i < keySlice.Len(); i++ {
		key, err := EncodeValue(keySlice.Index(i).Interface())
		if err != nil {
			return err
		}

		encodedKeys = append(encodedKeys, key)
	}

	removeAll := &v1.Request{
		RequestAPI: &v1.Request_RemoveAllRequest{
			RemoveAllRequest: &v1.RemoveAllRequest{
				RegionName: region,
				Key: encodedKeys,
			},
		},
	}

	_, err := this.doOperation(removeAll)

	return err
}

func (this *Protobuf) doOperation(request *v1.Request) (*v1.Response, error) {
	connection := this.pool.GetConnection()

	err := this.writeRequest(connection, request)
	if err != nil {
		return nil, err
	}

	response, err := this.readResponse(connection)
	if err != nil {
		return nil, err
	}

	if x := response.GetErrorResponse(); x != nil {
		return nil, errors.New(fmt.Sprintf("%s (%d)", x.GetError().Message, x.GetError().ErrorCode))
	}

	return response, nil
}

func (this *Protobuf) writeRequest(connection net.Conn, r *v1.Request) (err error) {
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

	_, err = connection.Write(p.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (this *Protobuf) readResponse(connection net.Conn) (*v1.Response, error) {
	data := make([]byte, 4096)
	bytesRead, err := connection.Read(data)
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
		n, err := io.ReadFull(connection, data[bytesRead:messageLength])
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
	encodedValues := make([]*v1.EncodedValue, 0, len(values))
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
	case nil:
		decodedValue = nil
	default:
		return nil, errors.New(fmt.Sprintf("unable to decode type: %T", v))
	}

	return decodedValue, nil
}
