package base_tool

const (
	Green  = "green"
	Yellow = "yellow"
	Red    = "Red"

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

	GetClusterHealth              = "GetClusterHealth"
	CheckClusterName              = "CheckClusterName"
	GetIndiceStatus               = "GetIndiceStatus"
	SetIndiceAllocationOnAndOff   = "SetIndiceAllocationOnAndOff"
	CreateIndices                 = "CreateIndices"
	GetUnhealthIndicesWithNoClose = "GetUnhealthIndicesWithNoClose"
	GetCloseIndices               = "GetCloseIndices"
	RecoveryUnhealthIndices       = "RecoveryUnhealthIndices"
	SetIndiceSettings             = "SetIndiceSettings"
	GetIndiceSettings             = "GetIndiceSettings"
	GetIndiceMapping              = "GetIndiceMapping"
	SetIndiceMapping              = "SetIndiceMapping"
	DataSink                      = "DataSink"
	GetCurrentRecovery            = "GetCurrentRecovery"
	CloseIndices                  = "CloseIndices"
	OpenIndices                   = "OpenIndices"
)
