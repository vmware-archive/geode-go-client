package geode_go_client

import (
	"github.com/gemfire/geode-go-client/connector"
	. "github.com/gemfire/geode-go-client/query"
)

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
//
type Client struct {
	connector *connector.Protobuf
}

func NewGeodeClient(c *connector.Protobuf) *Client {
	return &Client{
		connector: c,
	}
}

// Put data into a region. key and value must be a supported type.
func (this *Client) Put(region string, key, value interface{}) error {
	return this.connector.Put(region, key, value)
}

// Put data into a region if the key is not present. key and value must be a supported type.
func (this *Client) PutIfAbsent(region string, key, value interface{}) error {
	return this.connector.PutIfAbsent(region, key, value)
}

// Get an entry from a region using the specified key. It is the callers' responsibility
// to perform any type-assertion on the returned value. If a single, optional value is
// passed, the data retrieved from the region will be attempted to be unmarshalled as JSON
// into the supplied value.
func (this *Client) Get(region string, key interface{}, value ...interface{}) (interface{}, error) {
	if len(value) > 0 {
		return this.connector.Get(region, key, value[0])
	}
	return this.connector.Get(region, key, nil)
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
// Currently still being implemented in Geode.
//func (this *Client) RemoveAll(region string, keys interface{}) error {
//	return this.connector.RemoveAll(region, keys)
//}

// Size returns the number of entries in a region
func (this *Client) Size(region string) (int32, error) {
	return this.connector.Size(region)
}

// Execute a function on a region. This will execute on all members hosting the region and return a slice
// of results; one entry for each member.
func (this *Client) ExecuteOnRegion(functionId, region string, functionArgs interface{}, keyFilter []interface{}) ([]interface{}, error) {
	return this.connector.ExecuteOnRegion(functionId, region, functionArgs, keyFilter)
}

// Execute a function on a list of members, returning a slice of results, one entry for each member.
func (this *Client) ExecuteOnMembers(functionId string, members []string, functionArgs interface{}) ([]interface{}, error) {
	return this.connector.ExecuteOnMembers(functionId, members, functionArgs)
}

// Execute a function on a list of group. This will execute on each member associated with the groups;
// returning a slice of results, one entry for each member.
func (this *Client) ExecuteOnGroups(functionId string, groups []string, functionArgs interface{}) ([]interface{}, error) {
	return this.connector.ExecuteOnGroups(functionId, groups, functionArgs)
}

// Execute a query, returning a single result value.
func (this *Client) QueryForSingleResult(query *Query) (interface{}, error){
	return this.connector.QuerySingleResult(query)
}

// Execute a query, returning a list of results.
func (this *Client) QueryForListResult(query *Query) ([]interface{}, error){
	return this.connector.QueryListResult(query)
}

// Execute a query, returning a map of column (or field) names and the associated values for each column.
func (this *Client) QueryForTableResult(query *Query) (map[string][]interface{}, error){
	return this.connector.QueryTableResult(query)
}

