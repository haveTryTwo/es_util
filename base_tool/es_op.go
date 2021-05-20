package base_tool

type BaseEsOp interface {
	Get(uri string) ([]byte, error)
	Put(uri string, params string) ([]byte, error)
	Post(uri string, params string) ([]byte, error)
}
