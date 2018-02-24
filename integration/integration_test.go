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
	// download latest geode nightly
	downloadLocation, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	geodeHome, err := downloadBinary(downloadLocation, "http://apache.claz.org/geode/1.4.0/apache-geode-1.4.0.tgz")
	require.NoError(t, err)

	// tempDir is the test directory used to host locator and server directories
	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := ClusterConfig{
		geodeDir:    geodeHome, //"/Users/sboorlagadda/workspace/debug/geode/apache-geode-1.5.0-SNAPSHOT/",
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
