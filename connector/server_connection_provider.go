package connector

type serverConnectionProvider struct {
	host       string
	port       int
}

var _ ConnectionProvider = (*serverConnectionProvider)(nil)

func (this *serverConnectionProvider) GetGeodeConnection() *GeodeConnection {
	return nil
}
