package geode

import "geode/connector"

// A Client provides the high-level API required to interact with a Geode cluster. The API
// supports the following key and value types:
//
//     int
//     int16
//     int32
//     int64
//     byte
//     bool
//     float32
//     float64
//     []byte
//     string
//     CustomEncodedValue
//
// In order to enable the protobuf protocol, the Geode servers must be started with the
// property:
//
//     geode.feature-protobuf-protocol=true
type Client struct {
	connector *connector.Protobuf
}

func NewGeodeClient(c *connector.Protobuf) *Client {
	return &Client{
		connector: c,
	}
}

// Connect attempts to connect with a Geode cluster using the protobuf protocol.
func (this *Client) Connect() error {
	return this.connector.Connect()
}

// Put data into a region. key and value must be a supported type.
func (this *Client) Put(region string, key, value interface{}) error {
	return this.connector.Put(region, key, value)
}

// Get an entry from a region using the specified key. It is the callers' responsibility
// to perform any type-assertion on the returned value.
func (this *Client) Get(region string, key interface{}) (interface{}, error) {
	return this.connector.Get(region, key)
}

// PutAll adds multiple key/value pairs to a single region. Entries must be in the form of
// a map. The returned values are either a map of individual keys and the associated error
// when attempting to add that key, or a single error which typically would be as a result
// of a key or value encoding error.
func (this *Client) PutAll(region string, entries interface{}) (map[interface{}]error, error) {
	return this.connector.PutAll(region, entries)
}

// GetAll returns the values of multiple keys. Keys must be passed as an array or slice.
// The returned values are a map of keys and values for those keys which were
// successfully retrieved, a map of keys and the relevant error for those keys which produced
// an error on retrieval and, finally, a single error which typically would be as a result of
// a key or value encoding error.
func (this *Client) GetAll(region string, keys interface{}) (map[interface{}]interface{}, map[interface{}]error, error) {
	return this.connector.GetAll(region, keys)
}

// Remove an entry for a region.
func (this *Client) Remove(region string, key interface{}) error {
	return this.connector.Remove(region, key)
}

// Remove many entries from a region. The keys must be passed as an array or slice.
func (this *Client) RemoveAll(region string, keys interface{}) error {
	return this.connector.RemoveAll(region, keys)
}
