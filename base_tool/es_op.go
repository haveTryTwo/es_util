// base tool of es
package basetool

// interface for operating es
type BaseEsOp interface {
	Get(uri string) ([]byte, error)
	Put(uri string, params string) ([]byte, error)
	Post(uri string, params string) ([]byte, error)
}
