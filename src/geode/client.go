package geode

import "geode/connector"

// A Client provides the high-level API required to interact with a Geode cluster.
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

// Put data into a region.
func (this *Client) Put(region string, key, value interface{}) error {
	return this.connector.Put(region, key, value)
}

func (this *Client) Get(region string, key interface{}) (interface{}, error) {
	return this.connector.Get(region, key)
}

func (this *Client) PutAll(region string, entries map[interface{}]interface{}) (map[interface{}]error, error) {
	return this.connector.PutAll(region, entries)
}

func (this *Client) GetAll(region string, keys []interface{}) (map[interface{}]interface{}, map[interface{}]error, error) {
	return this.connector.GetAll(region, keys)
}

func (this *Client) Remove(region string, key interface{}) error {
	return this.connector.Remove(region, key)
}

func (this *Client) RemoveAll(region string, keys interface{}) error {
	return this.connector.RemoveAll(region, keys)
}
