package geode

import "geode/connector"

type Client struct {
	connector *connector.Connector
}

func NewGeodeClient(c *connector.Connector) *Client {
	return &Client{
		connector: c,
	}
}

func (this *Client) Connect() error {
	return this.connector.Connect()
}

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
