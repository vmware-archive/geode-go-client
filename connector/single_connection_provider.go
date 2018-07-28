package connector

type singleConnectionProvider struct {
	connection *GeodeConnection
}

var _ ConnectionProvider = (*singleConnectionProvider)(nil)

func (this *singleConnectionProvider) GetGeodeConnection() *GeodeConnection {
	return this.connection
}

