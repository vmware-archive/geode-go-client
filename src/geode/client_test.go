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
				return writeFakeResponse(response, b)
			}

			Expect(client.Connect()).To(BeNil())
			Expect(fakeConn.WriteCallCount()).To(Equal(2))
		})
	})

	Context("Put", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{}
				return writeFakeResponse(response, b)
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
				return writeFakeResponse(response, b)
			}

			Expect(client.Put("foo", "A", "B")).To(MatchError("error from fake"))
		})

		It("does not accept an unknown type", func() {
			Expect(client.Put("foo", "A", struct{}{})).To(MatchError("unable to encode type: struct {}"))
		})
	})

	Context("Get", func() {
		It("decodes values correctly", func() {
			var callCount = 0
			var v *v1.EncodedValue
			fakeConn.ReadStub = func(b []byte) (int, error) {
				switch callCount {
				case 0:
					// Implicit int()
					v, _ = connector.EncodeValue(1)
				case 1:
					v, _ = connector.EncodeValue(int16(2))
				case 2:
					v, _ = connector.EncodeValue(int32(3))
				case 3:
					v, _ = connector.EncodeValue(int64(4))
				case 4:
					v, _ = connector.EncodeValue(byte(5))
				case 5:
					v, _ = connector.EncodeValue(true)
				case 6:
					v, _ = connector.EncodeValue(float64(6))
				case 7:
					v, _ = connector.EncodeValue(float32(7))
				case 8:
					v, _ = connector.EncodeValue([]byte{8})
				case 9:
					v, _ = connector.EncodeValue("9")
				case 10:
					v, _ = connector.EncodeValue(&v1.CustomEncodedValue{
						EncodingType: 10,
						Value: []byte{1,2,3},
					})
				}
				callCount += 1

				response := &v1.Response{
					ResponseAPI: &v1.Response_GetResponse{
						GetResponse: &v1.GetResponse{
							Result: v,
						},
					},
				}
				return writeFakeResponse(response, b)
			}

			Expect(client.Get("foo", "A")).To(Equal(int32(1)))
			Expect(client.Get("foo", "A")).To(Equal(int32(2)))
			Expect(client.Get("foo", "A")).To(Equal(int32(3)))
			Expect(client.Get("foo", "A")).To(Equal(int64(4)))
			Expect(client.Get("foo", "A")).To(Equal(byte(5)))
			Expect(client.Get("foo", "A")).To(Equal(true))
			Expect(client.Get("foo", "A")).To(Equal(float64(6)))
			Expect(client.Get("foo", "A")).To(Equal(float32(7)))
			Expect(client.Get("foo", "A")).To(Equal([]byte{8}))
			Expect(client.Get("foo", "A")).To(Equal("9"))

			x, _ := client.Get("foo", "A")
			encoded := x.(*v1.CustomEncodedValue)
			Expect(int(encoded.EncodingType)).To(Equal(10))
			Expect(encoded.Value).To(Equal([]byte{1, 2, 3}))
		})
	})

	Context("GetAll", func() {
		It("decodes values correctly", func() {
			var callCount = 0
			var v *v1.EncodedValue
			fakeConn.ReadStub = func(b []byte) (int, error) {
				switch callCount {
				case 0:
					// Implicit int()
					v, _ = connector.EncodeValue(1)
				case 1:
					v, _ = connector.EncodeValue(int16(2))
				case 2:
					v, _ = connector.EncodeValue(int32(3))
				case 3:
					v, _ = connector.EncodeValue(int64(4))
				case 4:
					v, _ = connector.EncodeValue(byte(5))
				case 5:
					v, _ = connector.EncodeValue(true)
				case 6:
					v, _ = connector.EncodeValue(float64(6))
				case 7:
					v, _ = connector.EncodeValue(float32(7))
				case 8:
					v, _ = connector.EncodeValue([]byte{8})
				case 9:
					v, _ = connector.EncodeValue("9")
				case 10:
					v, _ = connector.EncodeValue(&v1.CustomEncodedValue{
						EncodingType: 10,
						Value: []byte{1,2,3},
					})
				}
				callCount += 1

				response := &v1.Response{
					ResponseAPI: &v1.Response_GetResponse{
						GetResponse: &v1.GetResponse{
							Result: v,
						},
					},
				}
				return writeFakeResponse(response, b)
			}

			keys := []interface{} {
				"A", 11,
			}
			Expect(client.GetAll("foo", keys)).To(Equal(int32(1)))
		})
	})
})

func writeFakeResponse(r *v1.Response, b []byte) (int, error) {
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
