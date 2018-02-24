package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"

	"fmt"
	geode "github.com/gemfire/geode-go-client"
	"github.com/gemfire/geode-go-client/connector"
	"strings"
)

func geodeAddr2NetworkAddr(geodeAddr string) string {
	networkAddr := strings.Replace(geodeAddr, "[", ":", 1)
	networkAddr = strings.Replace(networkAddr, "]", "", 1)
	return networkAddr
}

func downloadBinary(downloadLocation string, url string) (string, error) {
	out, err := os.Create(downloadLocation + "/apache-geode.tgz")
	if err != nil {
		return "", err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return "", err
	}

	extract := exec.Command("tar", "xzf", out.Name())

	extract.Dir = downloadLocation
	extract.Stdout = os.Stdout
	extract.Stderr = os.Stderr

	return "", nil
}

type ClusterConfig struct {
	locatorPort int
	serverPort  int

	locatorName string
	serverName  string

	geodeDir   string
	clusterDir string
}

type GeodeCluster struct {
	ClusterConfig

	locatorAddr string
	serverAddr  []string

	client *geode.Client
}

func NewGeodeCluster(config ClusterConfig) *GeodeCluster {
	cluster := &GeodeCluster{
		ClusterConfig: config,
		locatorAddr:   fmt.Sprintf("%s[%d]", "localhost", 10334),
		serverAddr:    []string{fmt.Sprintf("%s[%d]", "localhost", 40404)},
	}

	return cluster
}

func (g *GeodeCluster) gfsh(command string) error {
	args := append([]string{"-e", "connect --locator=" + g.locatorAddr, "-e", command})
	gfsh := exec.Command(g.geodeDir+"/bin/gfsh", args...)

	gfsh.Dir = g.geodeDir
	gfsh.Stdout = os.Stdout
	gfsh.Stderr = os.Stderr

	return gfsh.Run()
}

func (g *GeodeCluster) StartLocatorOnly() error {
	locator := exec.Command(g.geodeDir+"/bin/gfsh",
		"start",
		"locator",
		"--name="+g.locatorName,
		"--J=-Dgeode.feature-protobuf-protocol=true",
	)
	locator.Dir = g.clusterDir
	locator.Stdout = os.Stdout
	locator.Stderr = os.Stderr

	if err := locator.Run(); err != nil {
		return err
	}
	return nil
}

func (g *GeodeCluster) Start() error {
	if err := g.StartLocatorOnly(); err != nil {
		return err
	}

	server := exec.Command(g.geodeDir+"/bin/gfsh",
		"start",
		"server",
		"--name="+g.serverName,
		"--locators="+g.locatorAddr,
		"--J=-Dgeode.feature-protobuf-protocol=true",
	)
	server.Dir = g.clusterDir
	server.Stdout = os.Stdout
	server.Stderr = os.Stderr
	if err := server.Run(); err != nil {
		return err
	}

	var err error
	c, err := net.Dial("tcp", geodeAddr2NetworkAddr(g.serverAddr[0]))
	if err != nil {
		panic(err)
	}
	pool := connector.NewPool(c)
	conn := connector.NewConnector(pool)
	g.client = geode.NewGeodeClient(conn)
	err = g.client.Connect()
	if err != nil {
		panic(err)
	}

	return nil
}

func (g *GeodeCluster) Close() {
	g.gfsh("shutdown --include-locators=true")
}
