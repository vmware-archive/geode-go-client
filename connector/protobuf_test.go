package connector_test

import (
	"github.com/gemfire/geode-go-client/connector"
	"github.com/gemfire/geode-go-client/connector/connectorfakes"
	"github.com/gemfire/geode-go-client/protobuf"
	v1 "github.com/gemfire/geode-go-client/protobuf/v1"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"github.com/gemfire/geode-go-client/query"
)

//go:generate counterfeiter net.Conn

type TestStruct struct {
	Value int32
	Message string
}

var _ = Describe("Client", func() {

	var connection *connector.Protobuf
	var fakeConn *connectorfakes.FakeConn
	var pool *connector.Pool

	BeforeEach(func() {
		fakeConn = new(connectorfakes.FakeConn)
		pool = connector.NewPool(fakeConn)
		connection = connector.NewConnector(pool)
	})

	Context("Connect", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				ack := &org_apache_geode_internal_protocol_protobuf.VersionAcknowledgement{
					ServerMajorVersion: 1,
					ServerMinorVersion: 1,
					VersionAccepted:    true,
				}
				return writeFakeMessage(ack, b)
			}

			Expect(connection.Handshake()).To(BeNil())
			Expect(fakeConn.WriteCallCount()).To(Equal(1))
		})

		It("authenticates correctly", func() {
			pool.AddCredentials("cluster", "cluster")

			var ack proto.Message
			var callCount = 0
			fakeConn.ReadStub = func(b []byte) (int, error) {
				switch callCount {
				case 0:
					ack = &v1.Message{
						MessageType: &v1.Message_HandshakeResponse{
							HandshakeResponse: &v1.HandshakeResponse{
								Authenticated: true,
							},
						},
					}
				case 1:
					ack = &v1.Message{
						MessageType: &v1.Message_PutResponse{
							PutResponse: &v1.PutResponse{},
						},
					}
				}
				callCount += 1

				return writeFakeMessage(ack, b)
			}

			err := connection.Put("foo", "a", 1)
			Expect(err).To(BeNil())
			Expect(fakeConn.WriteCallCount()).To(Equal(2))
		})

		It("returns an error on authentication failure", func() {
			pool.AddCredentials("cluster", "bad")

			fakeConn.ReadStub = func(b []byte) (int, error) {
				ack := &v1.Message{
					MessageType: &v1.Message_HandshakeResponse{
						HandshakeResponse: &v1.HandshakeResponse{
							Authenticated: false,
						},
					},
				}

				return writeFakeMessage(ack, b)
			}

			_, err := connection.Get("foo", "a", nil)
			Expect(err).ToNot(BeNil())
			Expect(err).To(BeAssignableToTypeOf(connector.AuthenticationError("")))

		})
	})

	Context("Put", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_PutResponse{
						PutResponse: &v1.PutResponse{},
					},
				}
				return writeFakeMessage(response, b)
			}

			Expect(connection.Put("foo", "A", "B")).To(BeNil())
		})

		It("handles errors correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_ErrorResponse{
						ErrorResponse: &v1.ErrorResponse{
							Error: &v1.Error{
								ErrorCode: 1,
								Message:   "error from fake",
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			Expect(connection.Put("foo", "A", "B")).To(MatchError("error from fake (1)"))
		})

		It("can put an anonymous struct", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_PutResponse{
						PutResponse: &v1.PutResponse{},
					},
				}
				return writeFakeMessage(response, b)
			}

			json := struct{ A int }{1}
			Expect(connection.Put("foo", "A", json)).To(BeNil())
		})
	})

	Context("PutIfAbsent", func() {
		It("does not return an error", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_PutIfAbsentRequest{
						PutIfAbsentRequest: &v1.PutIfAbsentRequest{},
					},
				}
				return writeFakeMessage(response, b)
			}

			Expect(connection.PutIfAbsent("foo", "A", "B")).To(BeNil())
		})

		It("handles errors correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_ErrorResponse{
						ErrorResponse: &v1.ErrorResponse{
							Error: &v1.Error{
								ErrorCode: 1,
								Message:   "error from fake",
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			Expect(connection.PutIfAbsent("foo", "A", "B")).To(MatchError("error from fake (1)"))
		})

		It("can putIfAbsent an anonymous struct", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_PutIfAbsentRequest{
						PutIfAbsentRequest: &v1.PutIfAbsentRequest{},
					},
				}
				return writeFakeMessage(response, b)
			}

			json := struct{ A int }{1}
			Expect(connection.PutIfAbsent("foo", "A", json)).To(BeNil())
		})
	})

	Context("Get", func() {
		It("decodes values correctly", func() {
			var callCount = 0
			var v *v1.EncodedValue
			testStruct := &TestStruct{
				Value: 7,
				Message: "Hello World",
			}
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
					v, _ = connector.EncodeValue(testStruct)
				case 11:
					v, _ = connector.EncodeValue(nil)
				}
				callCount += 1

				response := &v1.Message{
					MessageType: &v1.Message_GetResponse{
						GetResponse: &v1.GetResponse{
							Result: v,
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			Expect(connection.Get("foo", "A", nil)).To(Equal(int32(1)))
			Expect(connection.Get("foo", "A", nil)).To(Equal(int32(2)))
			Expect(connection.Get("foo", "A", nil)).To(Equal(int32(3)))
			Expect(connection.Get("foo", "A", nil)).To(Equal(int64(4)))
			Expect(connection.Get("foo", "A", nil)).To(Equal(byte(5)))
			Expect(connection.Get("foo", "A", nil)).To(Equal(true))
			Expect(connection.Get("foo", "A", nil)).To(Equal(float64(6)))
			Expect(connection.Get("foo", "A", nil)).To(Equal(float32(7)))
			Expect(connection.Get("foo", "A", nil)).To(Equal([]byte{8}))
			Expect(connection.Get("foo", "A", nil)).To(Equal("9"))

			ref := &TestStruct{}
			x, err := connection.Get("foo", "A", ref)
			Expect(err).To(BeNil())
			Expect(ref).To(Equal(testStruct))
			Expect(x).To(Equal(testStruct))

			Expect(connection.Get("foo", "A", nil)).To(BeNil())
		})
	})

	Context("PutAll", func() {
		It("encodes values correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_PutAllResponse{
						PutAllResponse: &v1.PutAllResponse{
							FailedKeys: make([]*v1.KeyedError, 0),
						},
					},
				}

				return writeFakeMessage(response, b)
			}

			entries := make(map[interface{}]interface{}, 0)
			entries["A"] = 777
			entries[7] = "Jumbo"
			entries[struct{}{}] = 0

			response, err := connection.PutAll("foo", entries)
			Expect(err).To(BeNil())
			Expect(response).To(BeNil())
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
				response := &v1.Message{
					MessageType: &v1.Message_PutAllResponse{
						PutAllResponse: &v1.PutAllResponse{
							FailedKeys: failedKeys,
						},
					},
				}

				return writeFakeMessage(response, b)
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

				response := &v1.Message{
					MessageType: &v1.Message_GetAllResponse{
						GetAllResponse: &v1.GetAllResponse{
							Entries:  entries,
							Failures: failures,
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			keys := []interface{}{
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
				v, _ := connector.EncodeValue(888)
				entries = append(entries, &v1.Entry{
					Key:   k,
					Value: v,
				})

				failures := make([]*v1.KeyedError, 0)
				k2, _ := connector.EncodeValue(11)
				failures = append(failures, &v1.KeyedError{
					Key: k2,
					Error: &v1.Error{
						ErrorCode: 1,
						Message:   "getall failure",
					},
				})

				response := &v1.Message{
					MessageType: &v1.Message_GetAllResponse{
						GetAllResponse: &v1.GetAllResponse{
							Entries:  entries,
							Failures: failures,
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			keys := []interface{}{
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
				response := &v1.Message{
					MessageType: &v1.Message_RemoveResponse{
						RemoveResponse: &v1.RemoveResponse{},
					},
				}
				return writeFakeMessage(response, b)
			}

			Expect(connection.Remove("foo", "A")).To(BeNil())
		})

		It("does not return an error for struct{} key type", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_RemoveResponse{
						RemoveResponse: &v1.RemoveResponse{},
					},
				}
				return writeFakeMessage(response, b)
			}
			errResult := connection.Remove("foo", struct{}{})

			Expect(errResult).To(BeNil())
		})
	})

	Context("Size", func() {
		It("returns the correct region size", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				response := &v1.Message{
					MessageType: &v1.Message_GetSizeResponse{
						GetSizeResponse: &v1.GetSizeResponse{
							Size: 77,
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			size, err := connection.Size("foo")

			Expect(err).To(BeNil())
			var expected int32 = 77
			Expect(size).To(Equal(expected))
		})
	})

	Context("Function", func() {
		It("processes onRegion function arguments correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				v_1, _ := connector.EncodeValue(777)
				v_2, _ := connector.EncodeValue("Hello World")
				response := &v1.Message{
					MessageType: &v1.Message_ExecuteFunctionOnRegionResponse{
						ExecuteFunctionOnRegionResponse: &v1.ExecuteFunctionOnRegionResponse{
							Results: []*v1.EncodedValue{
								v_1,
								v_2,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			result, err := connection.ExecuteOnRegion("foo", "bar", nil, nil)

			Expect(err).To(BeNil())
			var expected int32 = 777
			Expect(result[0]).To(Equal(expected))
			Expect(result[1]).To(Equal("Hello World"))
		})

		It("processes onMember function arguments correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				v_1, _ := connector.EncodeValue(777)
				v_2, _ := connector.EncodeValue("Hello World")
				response := &v1.Message{
					MessageType: &v1.Message_ExecuteFunctionOnMemberResponse{
						ExecuteFunctionOnMemberResponse: &v1.ExecuteFunctionOnMemberResponse{
							Results: []*v1.EncodedValue{
								v_1,
								v_2,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			result, err := connection.ExecuteOnMembers("foo", []string{"bar"}, nil)

			Expect(err).To(BeNil())
			var expected int32 = 777
			Expect(result[0]).To(Equal(expected))
			Expect(result[1]).To(Equal("Hello World"))
		})

		It("processes onGroup function arguments correctly", func() {
			fakeConn.ReadStub = func(b []byte) (int, error) {
				v_1, _ := connector.EncodeValue(777)
				v_2, _ := connector.EncodeValue("Hello World")
				response := &v1.Message{
					MessageType: &v1.Message_ExecuteFunctionOnGroupResponse{
						ExecuteFunctionOnGroupResponse: &v1.ExecuteFunctionOnGroupResponse{
							Results: []*v1.EncodedValue{
								v_1,
								v_2,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			result, err := connection.ExecuteOnGroups("foo", []string{"bar"}, nil)

			Expect(err).To(BeNil())
			var expected int32 = 777
			Expect(result[0]).To(Equal(expected))
			Expect(result[1]).To(Equal("Hello World"))
		})
	})

	Context("Query for a single result", func() {
		It("returns a single value result", func() {
			sumOfBinds := new(int32)
			*sumOfBinds = 0

			fakeConn.ReadStub = func(b []byte) (int, error) {
				v, _ := connector.EncodeValue(*sumOfBinds)
				response := &v1.Message{
					MessageType: &v1.Message_OqlQueryResponse{
						OqlQueryResponse: &v1.OQLQueryResponse{
							Result: &v1.OQLQueryResponse_SingleResult{
								SingleResult: v,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			fakeConn.WriteStub = func(b []byte) (int, error) {
				p := proto.NewBuffer(b)
				response := &v1.Message{}
				if err := p.DecodeMessage(response); err != nil {
					return 0, err
				}

				request := response.GetOqlQueryRequest()

				for _, param := range request.BindParameter {
					val, _ := connector.DecodeValue(param, nil)
					*sumOfBinds += val.(int32)
				}

				return len(b), nil
			}

			// A little hacky perhaps, but the result of this query is simply the addition of all the bind values
			q := query.NewQuery("select foo", 1, 2, 3, 5)
			result, err := connection.QuerySingleResult(q)

			Expect(err).To(BeNil())
			var expected int32 = 11
			Expect(result).To(Equal(expected))
		})
	})

	Context("Query for a list result", func() {
		It("returns a list of results", func() {
			listOfBinds := make([]interface{}, 0)

			fakeConn.ReadStub = func(b []byte) (int, error) {
				v, _ := connector.EncodeValueList(listOfBinds)
				response := &v1.Message{
					MessageType: &v1.Message_OqlQueryResponse{
						OqlQueryResponse: &v1.OQLQueryResponse{
							Result: &v1.OQLQueryResponse_ListResult{
								ListResult: v,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			fakeConn.WriteStub = func(b []byte) (int, error) {
				p := proto.NewBuffer(b)
				response := &v1.Message{}
				if err := p.DecodeMessage(response); err != nil {
					return 0, err
				}

				request := response.GetOqlQueryRequest()

				for _, param := range request.BindParameter {
					val, _ := connector.DecodeValue(param, nil)
					listOfBinds = append(listOfBinds, val)
				}

				return len(b), nil
			}

			// A little hacky perhaps, but the result of this query is simply the bind values
			// returned as a the result.
			q := query.NewQuery("select foo", 1, "hey")
			result, err := connection.QueryListResult(q)

			Expect(err).To(BeNil())
			var one int32 = 1
			Expect(result[0]).To(Equal(one))
			Expect(result[1]).To(Equal("hey"))
		})
	})

	Context("Query for a list result", func() {
		It("returns a struct when a reference is also provided", func() {
			listOfBinds := make([]interface{}, 0)

			fakeConn.ReadStub = func(b []byte) (int, error) {
				v, _ := connector.EncodeValueList(listOfBinds)
				response := &v1.Message{
					MessageType: &v1.Message_OqlQueryResponse{
						OqlQueryResponse: &v1.OQLQueryResponse{
							Result: &v1.OQLQueryResponse_ListResult{
								ListResult: v,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			fakeConn.WriteStub = func(b []byte) (int, error) {
				p := proto.NewBuffer(b)
				response := &v1.Message{}
				if err := p.DecodeMessage(response); err != nil {
					return 0, err
				}

				request := response.GetOqlQueryRequest()

				for _, param := range request.BindParameter {
					val, _ := connector.DecodeValue(param, nil)
					listOfBinds = append(listOfBinds, &TestStruct{Value: val.(int32)})
				}

				return len(b), nil
			}

			// A little hacky perhaps, but the result of this query is simply the bind values
			// returned as the result.
			q := query.NewQuery("select foo", 1, 2)
			q.Reference = &TestStruct{}
			result, err := connection.QueryListResult(q)

			Expect(err).To(BeNil())
			Expect(result[0]).To(Equal(&TestStruct{Value: 1}))
			Expect(result[1]).To(Equal(&TestStruct{Value: 2}))
		})
	})

	Context("Query for a table result", func() {
		It("returns a map of results", func() {
			listOfBinds := make(map[string][]interface{}, 0)

			fakeConn.ReadStub = func(b []byte) (int, error) {
				v, _ := connector.EncodeTable(listOfBinds)
				response := &v1.Message{
					MessageType: &v1.Message_OqlQueryResponse{
						OqlQueryResponse: &v1.OQLQueryResponse{
							Result: &v1.OQLQueryResponse_TableResult{
								TableResult: v,
							},
						},
					},
				}
				return writeFakeMessage(response, b)
			}

			fakeConn.WriteStub = func(b []byte) (int, error) {
				p := proto.NewBuffer(b)
				response := &v1.Message{}
				if err := p.DecodeMessage(response); err != nil {
					return 0, err
				}

				request := response.GetOqlQueryRequest()

				idx := 0
				for _, param := range request.BindParameter {
					val, _ := connector.DecodeValue(param, nil)
					listOfBinds[strconv.Itoa(idx)] = []interface{}{val}
					idx += 1
				}

				return len(b), nil
			}

			// A little hacky perhaps, but the result of this query is simply the bind values
			// returned as the result.
			q := query.NewQuery("select foo", 1, "hey")
			result, err := connection.QueryTableResult(q)

			Expect(err).To(BeNil())
			var one int32 = 1
			Expect(result["0"][0]).To(Equal(one))
			Expect(result["1"][0]).To(Equal("hey"))
		})
	})
})

func writeFakeMessage(m proto.Message, b []byte) (int, error) {
	p := proto.NewBuffer(nil)
	p.EncodeMessage(m)
	n := copy(b, p.Bytes())

	return n, nil
}
