package integration_test

import (
	"encoding/json"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/gemfire/geode-go-client/integration"
	"fmt"
	"github.com/gemfire/geode-go-client/query"
	"time"
)

func logToGinkgo(format string, args ...interface{}) {
	GinkgoWriter.Write([]byte(fmt.Sprintf(format, args...)))
}

var _ = Describe("Client", func() {

	type Address struct {
		Street string `json:"street"`
	}

	type Person struct {
		Id      int      `json:"id"`
		Name    string   `json:"name"`
		Address *Address `json:"address"`
	}

	var (
		tempDir    string
		tempDirErr error
		cluster    *GeodeCluster
		config     *ClusterConfig
	)

	BeforeSuite(func() {
		tempDir, tempDirErr = ioutil.TempDir("", "")
		Expect(tempDirErr).To(BeNil())

		config = &ClusterConfig{
			ClusterDir:  tempDir,
			LocatorPort: 10334,
			LocatorName: "locator1",
			ServerName:  "server1",
			ServerPort:  40404,
		}

		cluster = NewGeodeCluster(config).WithSecurity("cluster,data", "cluster,data")
		err := cluster.Start()
		Expect(err).To(BeNil())
	})

	BeforeEach(func() {
		configJson, err := json.Marshal(config)
		Expect(err).To(BeNil())
		logToGinkgo("%s\n", configJson)

		err = cluster.Gfsh("create region --name=FOO --type=REPLICATE")
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		cluster.Gfsh("destroy region --name=FOO")
	})

	AfterSuite(func() {
		cluster.Close()
		os.RemoveAll(tempDir)
	})

	Describe("Get", func() {
		It("should get existing data", func() {
			// use gfsh to put a key/value
			cluster.Gfsh("put --key=\"A\" --value=1 --region=FOO")
			v, err := cluster.Client.Get("FOO", "A")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(Equal("1"))
		})

		It("should return nil for a non-existent key", func() {
			v, err := cluster.Client.Get("FOO", "UNKNOWN")
			Expect(err).To(BeNil())
			Expect(v).To(BeNil())
		})

		It("should return nil for a non-existent key when using a reference", func() {
			ref := &Person{}
			v, err := cluster.Client.Get("FOO", "UNKNOWN", ref)
			Expect(err).To(BeNil())
			Expect(v).To(BeNil())
		})
	})

	Describe("GetAll", func() {
		It("should get existing data", func() {
			// use gfsh to put some key/values
			cluster.Gfsh("put --key=\"A\" --value=\"Apple\" --region=FOO")
			cluster.Gfsh("put --key=\"B\" --value=\"Ball\" --region=FOO")

			keys := []interface{}{
				"A", "B", "unknownkey",
			}

			entries, _, err := cluster.Client.GetAll("FOO", keys)
			Expect(err).To(BeNil())
			Expect(entries).Should(ContainElement(BeEquivalentTo("Apple")))
			Expect(entries).Should(ContainElement(BeEquivalentTo("Ball")))
		})
	})

	Describe("Put", func() {
		It("should write data to region", func() {
			cluster.Client.Put("FOO", "A", 777)
			v, err := cluster.Client.Get("FOO", "A")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(BeEquivalentTo(777))
		})
	})

	Describe("PutAll", func() {
		It("should write data to region", func() {
			entries := make(map[interface{}]interface{}, 0)
			entries["A"] = 777
			entries["B"] = "Jumbo"

			_, err := cluster.Client.PutAll("FOO", entries)
			Expect(err).To(BeNil())

			keys := []interface{}{
				"A", "B", "unknownkey",
			}

			entries, _, err = cluster.Client.GetAll("FOO", keys)
			Expect(err).To(BeNil())
			Expect(entries).Should(ContainElement(BeEquivalentTo(777)))
			Expect(entries).Should(ContainElement(BeEquivalentTo("Jumbo")))
		})
	})

	Describe("PutIfAbsent", func() {
		It("should write data to region only if absent", func() {
			// putIfAbsent actually puts if absent
			cluster.Client.PutIfAbsent("FOO", "A", 777)
			v, err := cluster.Client.Get("FOO", "A")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(BeEquivalentTo(777))

			// putIfAbsent should not overwrite existing value
			cluster.Client.PutIfAbsent("FOO", "A", 888)
			v, err = cluster.Client.Get("FOO", "A")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(BeEquivalentTo(777))
		})
	})

	Describe("PutStruct", func() {
		It("should write and read a struct as JSON", func() {
			p := &Person{
				Id:   77,
				Name: "Joe Bloggs",
			}
			cluster.Client.Put("FOO", "joe", p)

			r := &Person{}
			cluster.Client.Get("FOO", "joe", r)
			Expect(r).To(Equal(p))
		})

		It("should write and read a struct as JSON when structs are nested", func() {
			a := &Address{Street: "Main Street"}

			p := &Person{
				Id:      77,
				Name:    "Joe Bloggs",
				Address: a,
			}
			cluster.Client.Put("FOO", 77, p)

			r := &Person{}
			cluster.Client.Get("FOO", 77, r)
			Expect(r).To(Equal(p))
		})
	})

	Describe("Querying", func() {
		It("should return a list of values", func() {
			for i := 0; i < 20; i++ {
				p := &Person{
					Id:   i,
					Name: fmt.Sprintf("Mr. Roboto %d", i),
				}
				cluster.Client.Put("FOO", i, p)
			}

			q1 := query.NewQuery("select count(*) from /FOO")
			result, err := cluster.Client.QueryForListResult(q1)
			Expect(err).To(BeNil())
			var expected int32 = 20
			Expect(result[0]).To(Equal(expected))

			q2 := query.NewQuery("select * from /FOO where id = 1")
			q2.Reference = &Person{
				Id: 1,
				Name: "Mr. Roboto 1",
			}
			another, err := cluster.Client.QueryForListResult(q2)
			Expect(err).To(BeNil())
			Expect(another[0].(*Person)).To(Equal(q2.Reference))
		})

	})

	Describe("Querying", func() {
		XIt("should return a single value for UNDEFINED results", func() {
			for i := 0; i < 20; i++ {
				p := &Person{
					Id:   i,
					Name: fmt.Sprintf("Mr. Roboto %d", i),
				}
				cluster.Client.Put("FOO", i, p)
			}

			q1 := query.NewQuery("select f.NONEXISTENT from /FOO as f where id = 1")
			//q1.Reference = &Person{
			//	Id: 1,
			//	Name: "Mr. Roboto 1",
			//}
			another, err := cluster.Client.QueryForListResult(q1)
			Expect(err).To(BeNil())
			Expect(another).To(Equal(q1.Reference))
		})

	})

	Describe("Reconnecting", func() {
		// Unfortunately, it seems that the only way to adjust the default is by
		// setting it on the actual CacheServer object: cacheServer.setMaximumTimeBetweenPings()
		It("should be able to work even after the default client idle timeout has passed", func () {
			cluster.Client.Put("FOO", "AAA", 777)

			time.Sleep(65 * time.Second)

			v, err := cluster.Client.Get("FOO", "AAA")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(BeEquivalentTo(777))
		})
	})
})
