package integration_test

import (
	"github.com/onsi/ginkgo/config"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestIntegration(t *testing.T) {
	if _, present := os.LookupEnv("GEODE_HOME"); present != true {
		t.Skip("$GEODE_HOME is not set, skipping integration tests......!")
	}

	config.DefaultReporterConfig.SlowSpecThreshold = 60

	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "Integration Suite", []Reporter{NewGeodeLogReporter()})
}
