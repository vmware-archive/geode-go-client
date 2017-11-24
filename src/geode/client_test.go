package geode_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "geode/protobuf/v1"
	"geode"
	"geode/connector/connectorfakes"
	"geode/connector"
	"github.com/golang/protobuf/proto"
)

var _ = Describe("Client", func() {

	var client *geode.Client
	var fakeConn *connectorfakes.FakeConn

	BeforeEach(func() {
		fakeConn = new(connectorfakes.FakeConn)
		connector := connector.NewConnector(fakeConn)
		client = geode.NewGeodeClient(connector)
	})

	Context("Connect", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_Response{
						Response: &v1.Response{
							ResponseAPI: &v1.Response_HandshakeResponse{
								HandshakeResponse: &v1.HandshakeResponse{
									ServerMajorVersion: 1,
									ServerMinorVersion: 1,
									HandshakePassed: true,
								},
							},
						},
					},
				}
				in, _ := proto.Marshal(response)
				n := copy(b, in)
				return n, nil
			}

			Expect(client.Connect()).To(BeNil())
			Expect(fakeConn.WriteCallCount()).To(Equal(3))
		})
	})

	Context("Put", func() {
		It("does not return an error", func() {
			Expect(client.Put("foo", "A", "B")).To(BeNil())
		})

		It("does not accept an unknown type", func() {
			Expect(client.Put("foo", "A", struct{}{})).To(MatchError("unable to encode type: struct {}"))
		})
	})
})
