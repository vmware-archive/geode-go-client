package connector_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/gemfire/geode-go-client/connector"
)

var _ = Describe("Encode and Decode", func() {
	Context("encoding", func() {
		It("can encode []int", func() {
			input := make([]int, 2)
			input[0] = 1
			input[1] = 2

			rawResult, err := connector.EncodeList(input)

			Expect(err).To(BeNil())
			Expect(len(rawResult)).To(Equal(2))
		})

		It("can encode []int to value list for queries", func() {
			input := make([]int, 2)
			input[0] = 1
			input[1] = 2

			rawResult, err := connector.EncodeValueList(input)

			Expect(err).To(BeNil())
			Expect(len(rawResult.GetElement())).To(Equal(2))
		})
	})
})
