package integration_test

import (
	"encoding/json"
	"fmt"
	"github.com/gemfire/geode-go-client/integration"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	"github.com/onsi/ginkgo/types"
	"io"
	"os"
	"path"
)

type GeodeLogReporter struct {

}

var _ ginkgo.Reporter = (*GeodeLogReporter)(nil)

func NewGeodeLogReporter() *GeodeLogReporter {
	return &GeodeLogReporter{}
}

func (this *GeodeLogReporter) SpecSuiteWillBegin(config config.GinkgoConfigType, summary *types.SuiteSummary) {
}

func (this *GeodeLogReporter) BeforeSuiteDidRun(setupSummary *types.SetupSummary) {
}

func (this *GeodeLogReporter) SpecWillRun(specSummary *types.SpecSummary) {
}

func (this *GeodeLogReporter) SpecDidComplete(specSummary *types.SpecSummary) {
	if specSummary.HasFailureState() {
		var config = integration.ClusterConfig{}
		err := json.Unmarshal([]byte(specSummary.CapturedOutput), &config)

		if err != nil {
			fmt.Printf("Failed to unmarshal ClusterCconfig %s\n", err)
			return
		}

		this.dumpGeodeLog(config.ClusterDir, config.LocatorName)
		this.dumpGeodeLog(config.ClusterDir, config.ServerName)
	}
}

func (this *GeodeLogReporter) AfterSuiteDidRun(setupSummary *types.SetupSummary) {
}

func (this *GeodeLogReporter) SpecSuiteDidEnd(summary *types.SuiteSummary) {
}

func (this *GeodeLogReporter) dumpGeodeLog(directory, memberName string) {
	name := fmt.Sprintf("%s.log", memberName)
	logFile := path.Join(directory, memberName, name)

	f, err := os.Open(logFile)
	if err != nil {
		fmt.Printf("Unable to open log file %s - %s\n", logFile, err)
		return
	}

	fmt.Printf("------------------------------------- START %s ------------------------------------------\n", name)
	_, _ = io.Copy(os.Stdout, f)
	fmt.Printf("------------------------------------- END %s ------------------------------------------\n", name)
}
