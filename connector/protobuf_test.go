package connector_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "github.com/gemfire/geode-go-client/protobuf/v1"
	"github.com/gemfire/geode-go-client/protobuf"
	"github.com/gemfire/geode-go-client/connector"
	"github.com/golang/protobuf/proto"
	"github.com/gemfire/geode-go-client/connector/connectorfakes"
)

//go:generate counterfeiter net.Conn

var _ = Describe("Client", func() {

	var connection *connector.Protobuf
	var fakeConn *connectorfakes.FakeConn

	BeforeEach(func() {
		fakeConn = new(connectorfakes.FakeConn)
		pool := connector.NewPool(fakeConn)
		connection = connector.NewConnector(pool)
	})

	Context("Connect", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				ack := &org_apache_geode_internal_protocol_protobuf.HandshakeAcknowledgement{
					ServerMajorVersion: 1,
					ServerMinorVersion: 1,
					HandshakePassed:    true,
				}
				return writeFakeMessage(ack, b)
			}

			Expect(connection.Handshake()).To(BeNil())
			Expect(fakeConn.WriteCallCount()).To(Equal(1))
		})
	})

	Context("Put", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_PutResponse{
						PutResponse: &v1.PutResponse{},
					},
				}
				return writeFakeResponse(response, b)
			}

			Expect(connection.Put("foo", "A", "B")).To(BeNil())
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

			Expect(connection.Put("foo", "A", "B")).To(MatchError("error from fake (1)"))
		})

		It("does not accept an unknown type", func() {
			Expect(connection.Put("foo", "A", struct{}{})).To(MatchError("unable to encode type: struct {}"))
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

			Expect(connection.Get("foo", "A")).To(Equal(int32(1)))
			Expect(connection.Get("foo", "A")).To(Equal(int32(2)))
			Expect(connection.Get("foo", "A")).To(Equal(int32(3)))
			Expect(connection.Get("foo", "A")).To(Equal(int64(4)))
			Expect(connection.Get("foo", "A")).To(Equal(byte(5)))
			Expect(connection.Get("foo", "A")).To(Equal(true))
			Expect(connection.Get("foo", "A")).To(Equal(float64(6)))
			Expect(connection.Get("foo", "A")).To(Equal(float32(7)))
			Expect(connection.Get("foo", "A")).To(Equal([]byte{8}))
			Expect(connection.Get("foo", "A")).To(Equal("9"))

			x, _ := connection.Get("foo", "A")
			encoded := x.(*v1.CustomEncodedValue)
			Expect(int(encoded.EncodingType)).To(Equal(10))
			Expect(encoded.Value).To(Equal([]byte{1, 2, 3}))
		})
	})

	Context("PutAll", func() {
		It("encodes values correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_PutAllResponse{
						PutAllResponse: &v1.PutAllResponse{
							FailedKeys: make([]*v1.KeyedError, 0),
						},
					},
				}

				return writeFakeResponse(response, b)
			}

			entries := make(map[interface{}]interface{}, 0)
			response, err := connection.PutAll("foo", entries)
			Expect(err).To(BeNil())
			Expect(response).To(BeNil())
		})

		It("reports protobuf encoding errors correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_PutAllResponse{
						PutAllResponse: &v1.PutAllResponse{
							FailedKeys: nil,
						},
					},
				}

				return writeFakeResponse(response, b)
			}

			var entries = map[int]struct{}{0: {}}

			_, err := connection.PutAll("foo", entries)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("unable to encode type: struct {}"))
		})

		It("reports correctly failing entries", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				failedKeys := make([]*v1.KeyedError, 0)
				failedKey, _ := connector.EncodeValue(77)
				failedKeys = append(failedKeys, &v1.KeyedError{
					Key: failedKey,
					Error: &v1.Error{
						ErrorCode: 1,
						Message:   "test error",
					},
				})
				response := &v1.Response{
					ResponseAPI: &v1.Response_PutAllResponse{
						PutAllResponse: &v1.PutAllResponse{
							FailedKeys: failedKeys,
						},
					},
				}

				return writeFakeResponse(response, b)
			}

			entries := make(map[interface{}]interface{})
			entries[77] = "yabba dabba doo"

			failures, err := connection.PutAll("foo", entries)

			Expect(err).To(BeNil())
			Expect(failures[int32(77)]).NotTo(BeNil())
			Expect(failures[int32(77)].Error()).To(Equal("test error (1)"))
		})
	})

	Context("GetAll", func() {
		It("responds correctly with empty results", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				entries := make([]*v1.Entry, 0)
				failures := make([]*v1.KeyedError, 0)

				response := &v1.Response{
					ResponseAPI: &v1.Response_GetAllResponse{
						GetAllResponse: &v1.GetAllResponse{
							Entries: entries,
							Failures: failures,
						},
					},
				}
				return writeFakeResponse(response, b)
			}

			keys := []interface{} {
				"A", 11,
			}
			entries, failures, err := connection.GetAll("foo", keys)

			Expect(err).To(BeNil())
			Expect(len(entries)).To(Equal(0))
			Expect(len(failures)).To(Equal(0))
		})

		It("responds with correctly decoded results", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				entries := make([]*v1.Entry, 0)
				k, _ := connector.EncodeValue("A")
				v, _:= connector.EncodeValue(888)
				entries = append(entries, &v1.Entry{
					Key: k,
					Value: v,
				})

				failures := make([]*v1.KeyedError, 0)
				k2, _ := connector.EncodeValue(11)
				failures = append(failures, &v1.KeyedError{
					Key: k2,
					Error: &v1.Error{
						ErrorCode: 1,
						Message: "getall failure",
					},
				})

				response := &v1.Response{
					ResponseAPI: &v1.Response_GetAllResponse{
						GetAllResponse: &v1.GetAllResponse{
							Entries: entries,
							Failures: failures,
						},
					},
				}
				return writeFakeResponse(response, b)
			}

			keys := []interface{} {
				"A", 11,
			}
			entries, failures, err := connection.GetAll("foo", keys)

			Expect(err).To(BeNil())
			Expect(len(entries)).To(Equal(1))
			Expect(entries["A"]).To(Equal(int32(888)))
			Expect(len(failures)).To(Equal(1))
			Expect(failures[int32(11)].Error()).To(Equal("getall failure (1)"))
		})
	})

	Context("Remove", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_RemoveResponse{
						RemoveResponse: &v1.RemoveResponse{},
					},
				}
				return writeFakeResponse(response, b)
			}

			Expect(connection.Remove("foo", "A")).To(BeNil())
		})

		It("returns error on invalid key type", func() {
			errResult := connection.Remove("foo", struct{}{})

			Expect(errResult).ToNot(BeNil())
			Expect(errResult.Error()).To(Equal("unable to encode type: struct {}"))
		})
	})

	// This functionality is coming back...
	//Context("RemoveAll", func() {
	//	It("does not return an error", func() {
	//		fakeConn.ReadStub = func(b []byte) (int, error) {
	//			response := &v1.Response{
	//				ResponseAPI: &v1.Response_RemoveAllResponse{
	//					RemoveAllResponse: &v1.RemoveAllResponse{},
	//				},
	//			}
	//			return writeFakeResponse(response, b)
	//		}
	//
	//		Expect(connection.Remove("foo", "A")).To(BeNil())
	//	})
	//
	//	It("returns error on invalid key type", func() {
	//		var x = []interface{} {struct{}{}}
	//
	//		errResult := connection.RemoveAll("foo", x)
	//
	//		Expect(errResult).ToNot(BeNil())
	//		Expect(errResult.Error()).To(Equal("unable to encode type: struct {}"))
	//	})
	//})

	Context("Size", func() {
		It("returns the correct region size", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Response{
					ResponseAPI: &v1.Response_GetRegionResponse{
						GetRegionResponse: &v1.GetRegionResponse{
							Region: &v1.Region{
								Size: 77,
							},
						},
					},
				}
				return writeFakeResponse(response, b)
			}

			size, err := connection.Size("foo")

			Expect(err).To(BeNil())
			var expected int64 = 77
			Expect(size).To(Equal(expected))
		})
	})
})

func writeFakeResponse(r *v1.Response, b []byte) (int, error) {
	response := &v1.Message{
		MessageType: &v1.Message_Response{
			Response: r,
		},
	}

	return writeFakeMessage(response, b)
}

func writeFakeMessage(m proto.Message, b []byte) (int, error) {
	p := proto.NewBuffer(nil)
	p.EncodeMessage(m)
	n := copy(b, p.Bytes())

	return n, nil
}
