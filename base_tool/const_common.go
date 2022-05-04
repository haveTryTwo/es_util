// Package basetool implements a tool of es
package basetool

const (
	Green  = "green"
	Yellow = "yellow"
	Red    = "red"

	GET    = "GET"
	PUT    = "PUT"
	POST   = "POST"
	DELETE = "DELETE"
	HEAD   = "HEAD"
	PATCH  = "PATCH"
	TRACE  = "TRACE"

	Open  = "open"
	Close = "close"

	Cmd            = "Cmd"
	CommonFile     = "CommonFile"
	RequestProto   = "RequestProto"
	ClientCertFile = "ClientCertFile"
	ClientKeyFile  = "ClientKeyFile"
	CaCertFile     = "CaCertFile"
	IPPort         = "IPPort"
	Http           = "http"
	Https          = "https"
	ClusterName    = "ClusterName"
	IndexName      = "IndexName"
	IndicesPath    = "IndicesPath"
	SettingsPath   = "SettingsPath"
	WaitSeconds    = "WaitSeconds" // 等待时间，默认为10s
	MappingPath    = "MappingPath"
	OpDirection    = "OpDirection"
	OpIndexNum     = "OpIndexNum"

	GetClusterHealth              = "GetClusterHealth"
	GetClusterSettings            = "GetClusterSettings"
	CheckClusterName              = "CheckClusterName"
	GetIndiceStatus               = "GetIndiceStatus"
	SetIndiceAllocationOnAndOff   = "SetIndiceAllocationOnAndOff"
	CreateIndices                 = "CreateIndices"
	GetUnhealthIndicesWithNoClose = "GetUnhealthIndicesWithNoClose"
	GetCloseIndices               = "GetCloseIndices"
	GetWholeIndices               = "GetWholeIndices"
	RecoveryUnhealthIndices       = "RecoveryUnhealthIndices"
	SetIndiceSettings             = "SetIndiceSettings"
	GetIndiceSettings             = "GetIndiceSettings"
	GetIndiceMapping              = "GetIndiceMapping"
	SetIndiceMapping              = "SetIndiceMapping"
	DataSink                      = "DataSink"
	GetCurrentRecovery            = "GetCurrentRecovery"
	CloseIndices                  = "CloseIndices"
	OpenIndices                   = "OpenIndices"
	DeleteClosedIndices           = "DeleteClosedIndices"

	// 并行恢复index时选择的方向
	Positive      = 0
	Reverse       = 1
	BiDirectional = 2

	MaxConcurrentIndexNum = 4 // 同一时刻最多并行4个index恢复
)
