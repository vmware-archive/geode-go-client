package integration

import (
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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
	)

	BeforeSuite(func() {
		tempDir, tempDirErr = ioutil.TempDir("", "")
		Expect(tempDirErr).To(BeNil())

		config := ClusterConfig{
			clusterDir:  tempDir,
			locatorPort: 10334,
			locatorName: "locator1",
			serverName:  "server1",
			serverPort:  40404,
		}

		cluster = NewGeodeCluster(config).WithSecurity("cluster,data", "cluster,data")
		err := cluster.Start()
		Expect(err).To(BeNil())
	})

	BeforeEach(func() {
		err := cluster.gfsh("create region --name=FOO --type=REPLICATE")
		Expect(err).To(BeNil())

	})

	AfterEach(func() {
		cluster.gfsh("destroy region --name=FOO")

	})

	AfterSuite(func() {
		cluster.Close()
		os.RemoveAll(tempDir)
	})

	Describe("Get", func() {
		It("should get existing data", func() {
			// use gfsh to put a key/value
			cluster.gfsh("put --key=\"A\" --value=1 --region=FOO")
			v, err := cluster.client.Get("FOO", "A")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(Equal("1"))
		})

		It("should return nil for a non-existent key", func() {
			// use gfsh to put a key/value
			v, err := cluster.client.Get("FOO", "UNKNOWN")
			Expect(err).To(BeNil())
			Expect(v).To(BeNil())
		})
	})

	Describe("GetAll", func() {
		It("should get existing data", func() {
			// use gfsh to put some key/values
			cluster.gfsh("put --key=\"A\" --value=\"Apple\" --region=FOO")
			cluster.gfsh("put --key=\"B\" --value=\"Ball\" --region=FOO")

			keys := []interface{}{
				"A", "B", "unknownkey",
			}

			entries, _, err := cluster.client.GetAll("FOO", keys)
			Expect(err).To(BeNil())
			Expect(entries).Should(ContainElement(BeEquivalentTo("Apple")))
			Expect(entries).Should(ContainElement(BeEquivalentTo("Ball")))
		})
	})

	Describe("Put", func() {
		It("should write data to region", func() {
			cluster.client.Put("FOO", "A", 777)
			v, err := cluster.client.Get("FOO", "A")
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

			_, err := cluster.client.PutAll("FOO", entries)
			Expect(err).To(BeNil())

			keys := []interface{}{
				"A", "B", "unknownkey",
			}

			entries, _, err = cluster.client.GetAll("FOO", keys)
			Expect(err).To(BeNil())
			Expect(entries).Should(ContainElement(BeEquivalentTo(777)))
			Expect(entries).Should(ContainElement(BeEquivalentTo("Jumbo")))
		})
	})

	Describe("PutIfAbsent", func() {
		It("should write data to region only if absent", func() {
			// putIfAbsent actually puts if absent
			cluster.client.PutIfAbsent("FOO", "A", 777)
			v, err := cluster.client.Get("FOO", "A")
			Expect(err).To(BeNil())
			Expect(v).ToNot(BeNil())
			Expect(v).To(BeEquivalentTo(777))

			// putIfAbsent should not overwrite existing value
			cluster.client.PutIfAbsent("FOO", "A", 888)
			v, err = cluster.client.Get("FOO", "A")
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
			cluster.client.Put("FOO", "joe", p)

			r := &Person{}
			cluster.client.Get("FOO", "joe", r)
			Expect(r).To(Equal(p))
		})

		It("should write and read a struct as JSON when structs are nested", func() {
			a := &Address{Street: "Main Street"}

			p := &Person{
				Id:      77,
				Name:    "Joe Bloggs",
				Address: a,
			}
			cluster.client.Put("FOO", 77, p)

			r := &Person{}
			cluster.client.Get("FOO", 77, r)
			Expect(r).To(Equal(p))
		})
	})
})
