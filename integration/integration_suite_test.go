package integration_test

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestIntegration(t *testing.T) {
	if _, present := os.LookupEnv("GEODE_HOME"); present != true {
		t.Skip("$GEODE_HOME is not set, skipping integration tests......!")
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}
