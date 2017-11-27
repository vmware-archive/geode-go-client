package geode_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "geode/protobuf/v1"
	"geode"
	"geode/geodefakes"
	"geode/connector"
	"github.com/golang/protobuf/proto"
)

//go:generate counterfeiter net.Conn

var _ = Describe("Client", func() {

	var client *geode.Client
	var fakeConn *geodefakes.FakeConn

	BeforeEach(func() {
		fakeConn = new(geodefakes.FakeConn)
		connector := connector.NewConnector(fakeConn)
		client = geode.NewGeodeClient(connector)
	})

	Context("Connect", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_HandshakeResponse{
						HandshakeResponse: &v1.HandshakeResponse{
							ServerMajorVersion: 1,
							ServerMinorVersion: 1,
							HandshakePassed:    true,
						},
					},
				}
				return writeResponse(response, b)
			}

			Expect(client.Connect()).To(BeNil())
			Expect(fakeConn.WriteCallCount()).To(Equal(2))
		})
	})

	Context("Put", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{}
				return writeResponse(response, b)
			}

			Expect(client.Put("foo", "A", "B")).To(BeNil())
		})

		It("handles errors correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_ErrorResponse{
						ErrorResponse: &v1.ErrorResponse{
							Error: &v1.Error{
								ErrorCode: 1,
								Message: "error from fake",
							},
						},
					},
				}
				return writeResponse(response, b)
			}

			Expect(client.Put("foo", "A", "B")).To(MatchError("error from fake"))
		})

		It("does not accept an unknown type", func() {
			Expect(client.Put("foo", "A", struct{}{})).To(MatchError("unable to encode type: struct {}"))
		})
	})
})

func writeResponse(r *v1.Response, b []byte) (int, error) {
	response := &v1.Message{
		MessageType: &v1.Message_Response{
			Response: r,
		},
	}
	p := proto.NewBuffer(nil)
	p.EncodeMessage(response)
	n := copy(b, p.Bytes())

	return n, nil
}
