// Package basetool implements a tool of es
package basetool

// BaseEsOp interface for operating es
type BaseEsOp interface {
	Get(uri string) ([]byte, error)
	Put(uri string, params string) ([]byte, error)
	Post(uri string, params string) ([]byte, error)
	Delete(uri string) ([]byte, error)
}
