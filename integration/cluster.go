package integration

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
	LocatorPort int
	ServerPort  int

	LocatorName string
	ServerName  string

	ClusterDir  string

	username    *string
	password    *string
}

type GeodeCluster struct {
	ClusterConfig

	serverAddr  []string

	Client *geode.Client
}

func NewGeodeCluster(config ClusterConfig) *GeodeCluster {
	cluster := &GeodeCluster{
		ClusterConfig: config,
		serverAddr:    []string{},
	}

	return cluster
}

func (g *GeodeCluster) GetLocatorAddress() string {
	return fmt.Sprintf("%s[%d]", "localhost", g.LocatorPort)
}

func (g *GeodeCluster) Gfsh(command string) error {
	var connectCmd string
	if g.username == nil {
		connectCmd = "connect --locator=" + g.GetLocatorAddress()
	} else {
		connectCmd = fmt.Sprintf("connect --locator=%s --user=%s --password=%s", g.GetLocatorAddress(),*g.username, *g.password)
	}

	args := append([]string{"-e", connectCmd, "-e", command})

	gfsh := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"), args...)

	gfsh.Dir = g.ClusterDir
	gfsh.Stdout = os.Stdout
	gfsh.Stderr = os.Stderr

	return gfsh.Run()
}

func (g *GeodeCluster) StartLocator() error {
	args := []string{
		"start",
		"locator",
		"--name="+g.LocatorName,
		"--J=-Dgeode.feature-protobuf-protocol=true",
	}
	if g.username != nil {
		args = append(args,"--J=-Dgemfire.security-manager=org.apache.geode.examples.SimpleSecurityManager")
	}

	locator := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"), args...)
	locator.Dir = g.ClusterDir
	locator.Stdout = os.Stdout
	locator.Stderr = os.Stderr

	if err := locator.Run(); err != nil {
		return err
	}

	return nil
}

func (g *GeodeCluster) StartServer() error {
	args := []string{
		"start",
		"server",
		"--name="+g.ServerName,
		"--locators="+g.GetLocatorAddress(),
		"--J=-Dgeode.feature-protobuf-protocol=true",
	}

	if g.username != nil {
		args = append(args, "--J=-Dgemfire.security-manager=org.apache.geode.examples.SimpleSecurityManager",
			fmt.Sprintf("--user=%s", *g.username),
			fmt.Sprintf("--password=%s", *g.password))
	}

	server := exec.Command(os.ExpandEnv("$GEODE_HOME/bin/gfsh"), args...)
	server.Dir = g.ClusterDir
	server.Stdout = os.Stdout
	server.Stderr = os.Stderr

	if err := server.Run(); err != nil {
		return err
	}

	g.serverAddr = append(g.serverAddr, fmt.Sprintf("%s[%d]", "localhost", g.ServerPort))
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
	pool := connector.NewPool(c, false)

	if g.username != nil {
		pool.AddCredentials(*g.username, *g.password)
	}

	conn := connector.NewConnector(pool)
	g.Client = geode.NewGeodeClient(conn)

	return nil
}

func (g *GeodeCluster) WithSecurity(username, password string) (*GeodeCluster) {
	g.username = &username
	g.password = &password

	return g
}

func (g *GeodeCluster) Close() {
	g.Gfsh("shutdown --include-locators=true")
}
