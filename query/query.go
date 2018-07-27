package query

type Query struct {
	QueryString    string
	BindParameters []interface{}
	Reference      interface{}
}

// Create a Query object which can be used to perform a query. If the query returns some type of struct then a
// reference type must be passed by setting the query.Reference parameter to use as a reference for the returned
// types.
func NewQuery(queryString string, bindParameters ...interface{}) *Query {
	return &Query {
		QueryString: queryString,
		BindParameters: bindParameters,
	}
}