package geode_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGeodeClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GeodeClient Suite")
}
