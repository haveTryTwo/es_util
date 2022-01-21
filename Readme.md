## NOTE
提供ES相关的操作，方便对ES自动化运营管理

## 编译
1. `download`源码后，根目录执行 `go mod init es_util`
2. 执行`go build src/es_tool.go`生成对应es_tool的命令行

## 操作
操作命令: `./es_tool -cfg  config_path`，其中`config_path`说明：
- 对应实际的操作命令的路径
- 采用 `./conf/project/real_cmd.cfg` 方式，其中 `conf`是配置的主路径；`project`是每个集群名称，为了区分不同的集群；`real_cmd.cfg`是每个命令的配置；

样例：`./es_tool -cfg ./conf/HaveTryTwo_First_One/GetClusterHealth.cfg`

## 工具说明
当前为了保证工具的 简单性 和 安全性，目前做的处理：

- 简单性方面：将每个命令分离成不同的文件，并且每个命令只用关注本身需要变化的信息，操作时只用关注本次变更 涉及文件命令
- 安全性方面：
  - 写操作会校验集群名，避免误操作；
  - 配置更新等操作会存储 操作前后的配置信息，以便出现问题时方便查询前后变更信息，以便进行回滚操作
  - 配置更新会凸显本次的变化的信息，验证变更符合预期


下面会列出当前的命令
- 说明：

### 1.1 GetClusterHealth
说明：获取集群的`health`信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetClusterHealth.cfg

```

### 1.1.1 GetClusterSettings
说明：获取集群的`settings`信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetClusterSettings.cfg

```

### 1.2 CheckClusterName
说明：检查集群名称是否为期望的值
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/CheckClusterName.cfg
```

### 1.3 GetIndiceStatus
说明：获取指定索引`health`和`status`信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetIndiceStatus.cfg
```

### 1.4 SetIndiceAllocationOnAndOff
说明：先打开索引`allocation`，直到所有`shard`创建完毕后，再关闭`allocation`
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/SetIndiceAllocationOnAndOff.cfg
```

### 1.5 CreateIndices
说明：创建索引，并且会分配索引`shard`
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/CreateIndices.cfg
```

### 1.6 GetUnhealthIndicesWithNoClose
说明：获取Unhealth（包括yellow和red）的索引，这里不考虑close索引
```
./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg
```

### 1.7 GetCloseIndices
说明：获取已经 close 的索引
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
```

### 1.7.1 GetWholeIndices
说明：获取集群中所有的索引
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetWholeIndices.cfg
```

### 1.8 RecoveryUnhealthIndices
说明：恢复Unhealth的索引
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/RecoveryUnhealthIndices.cfg
```

### 1.9 SetIndiceSettings
说明：设置多个索引的setting信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/SetIndiceSettings.cfg
```

### 1.9.1 GetIndiceSettings
说明：获取多个索引的setting信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetIndiceSettings.cfg
```


### 1.10 GetIndiceMapping
说明：获取多个索引的mapping信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetIndiceMapping.cfg
```

### 1.11 SetIndiceMapping
说明：设置多个索引的mapping信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/SetIndiceMapping.cfg
```

### 1.12 GetCurrentRecovery
说明：获取当前集群的恢复信息
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/GetCurrentRecovery.cfg
```

### 1.13 DataSink
说明：设置下沉索引的zone信息，然后进行下沉
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/DataSink.cfg
```

### 1.14 CloseIndices
说明：关闭一批索引
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/CloseIndices.cfg
```

### 1.15 OpenIndices
说明：打开一批索引
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/OpenIndices.cfg
```

### 1.15 DeleteClosedIndices
说明：删除一批关闭的索引
```
./es_tool -cfg ./conf/HaveTryTwo_First_One/DeleteClosedIndices.cfg
```
