package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"

	geode "github.com/gemfire/geode-go-client"
	"github.com/gemfire/geode-go-client/connector"
)

func geodeAddr2NetworkAddr(geodeAddr string) string {
	networkAddr := strings.Replace(geodeAddr, "[", ":", 1)
	networkAddr = strings.Replace(networkAddr, "]", "", 1)
	return networkAddr
}

type ClusterConfig struct {
	locatorPort int
	serverPort  int

	locatorName string
	serverName  string

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
		serverAddr:    []string{},
	}

	return cluster
}

func (g *GeodeCluster) gfsh(command string) error {
	args := append([]string{"-e", "connect --locator=" + g.locatorAddr, "-e", command})
	gfsh := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"), args...)

	gfsh.Dir = g.clusterDir
	gfsh.Stdout = os.Stdout
	gfsh.Stderr = os.Stderr

	return gfsh.Run()
}

func (g *GeodeCluster) StartLocator() error {
	locator := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"),
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

	g.locatorAddr = fmt.Sprintf("%s[%d]", "localhost", g.locatorPort)

	return nil
}

func (g *GeodeCluster) StartServer() error {
	server := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"),
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

	g.serverAddr = append(g.serverAddr, fmt.Sprintf("%s[%d]", "localhost", g.serverPort))
	return nil
}

func (g *GeodeCluster) Start() error {
	if err := g.StartLocator(); err != nil {
		return err
	}

	if err := g.StartServer(); err != nil {
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
