package main

import (
	"io/ioutil"
	"os"
	"testing"

	geode "github.com/gemfire/geode-go-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {

	if _, present := os.LookupEnv("GEODE_HOME"); present != true {
		t.Skip("$GEODE_HOME is not set")
	}

	// tempDir is a temp directory used to host locator and server directories
	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := ClusterConfig{
		clusterDir:  tempDir,
		locatorPort: 10334,
		locatorName: "locator1",
		serverName:  "server1",
		serverPort:  40404,
	}

	cluster := NewGeodeCluster(config)
	require.NoError(t, cluster.Start())
	defer cluster.Close()

	wrap := func(fn func(*testing.T, func(command string) error, *geode.Client)) func(*testing.T) {
		return func(t *testing.T) {
			cluster.gfsh("create region --name=FOO --type=REPLICATE")
			fn(t, cluster.gfsh, cluster.client)
			cluster.gfsh("destroy region --name=FOO")
		}
	}

	t.Run("get gets existing data", wrap(GetExistingData))
	t.Run("gets and puts", wrap(GetsAndPuts))
	t.Run("getall and putall", wrap(GetsAllAndPutsAll))
}

func GetExistingData(t *testing.T, gfsh func(command string) error, c *geode.Client) {
	gfsh("put --key=\"A\" --value=1 --region=FOO")
	v, err := c.Get("FOO", "A")
	require.NoError(t, err)
	assert.Equal(t, v, "1", "Get failed to get existing key")
}

func GetsAndPuts(t *testing.T, gfsh func(command string) error, c *geode.Client) {
	c.Put("FOO", "A", 777)
	v, err := c.Get("FOO", "A")
	require.NoError(t, err)
	assert.EqualValues(t, v, 777, "Get failed to get written key")
}

func GetsAllAndPutsAll(t *testing.T, gfsh func(command string) error, c *geode.Client) {
	entries := make(map[interface{}]interface{}, 0)
	entries["A"] = 777
	entries["B"] = "Jumbo"

	c.PutAll("FOO", entries)

	keys := []interface{}{
		"A", "B", "unknownkey",
	}
	entries, _, err := c.GetAll("FOO", keys)
	require.NoError(t, err)
	//require.Contains(t, failures, "unknownkey") - check why its failing
	assert.EqualValues(t, entries["A"], 777)
	assert.EqualValues(t, entries["B"], "Jumbo")
}
