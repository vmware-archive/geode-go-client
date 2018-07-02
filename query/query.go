package query

type Query struct {
	QueryString    string
	BindParameters []interface{}
	Reference      interface{}
}

func NewQuery(queryString string, bindParameters ...interface{}) *Query {
	return &Query {
		QueryString: queryString,
		BindParameters: bindParameters,
	}
}

func (this *Query) WithReference(ref interface{}) {
	this.Reference = ref
}
