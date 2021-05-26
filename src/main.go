package main

import (
	"es_util/base_tool"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	cmdCfgPath        string
	execStatusSuccess bool
)

// GetClusterHealth http/https ip:port  http/https clusterName indexName
func init() {
	flag.StringVar(&cmdCfgPath, "cfgpath", "", "指定业务命令的配置路径")
	flag.Parse()
}

func printStats() {
	if execStatusSuccess {
		log.Printf("[Success]")
	} else {
		log.Printf("[Fail]")
		os.Exit(-1)
	}
}

func main() { // {{{
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	execStatusSuccess = false

	defer printStats()

	if "" == cmdCfgPath {
		log.Println("cfgpath 路径不能为空")
		return
	}
	//    log.Printf("cfgpath:%v\n", cmdCfgPath)

	cmdCfgDir, fileName := filepath.Split(cmdCfgPath)
	log.Printf("paths:%v, fileName:%v", cmdCfgDir, fileName)
	isDir, err := base_tool.IsDir(cmdCfgDir)
	if err != nil {
		log.Printf("err:%v", err)
		return
	}
	if isDir == false {
		log.Printf("Not dir:%v", cmdCfgDir)
		return
	}

	// 读取当前的命令文件
	configs, err := base_tool.ReadCfgFile(cmdCfgPath)
	if err != nil {
		log.Printf("err:%v", err)
		return
	}
	//    log.Printf("configs:%v", configs)

	// 读取当前请求命令
	cmd, ok := configs[base_tool.Cmd]
	if !ok {
		log.Printf("Not exist:%v", base_tool.Cmd)
		return
	}
	//    log.Printf("cmd:%v", cmd)

	commonFile, ok := configs[base_tool.CommonFile]
	if !ok {
		log.Printf("Not exist:%v", base_tool.CommonFile)
		return
	}

	// 读取通用的配置文件
	var commonPath string
	if strings.Index(strings.Trim(commonFile, " "), "./") == 0 {
		commonPath = cmdCfgDir + commonFile // 使用相对路径
	} else if strings.Index(strings.Trim(commonFile, " "), "/") == 0 {
		commonPath = commonFile // 使用绝对路径
	} else {
		log.Printf("Invalid path:%v", commonFile)
	}
	commonConfigs, err := base_tool.ReadCfgFile(commonPath)
	if err != nil {
		log.Printf("Failed to read:%v", commonPath)
		return
	}
	//    log.Printf("commonConfigs:%v", commonConfigs)

	// http 或者 https 请求
	requestProto, ok := commonConfigs[base_tool.RequestProto]
	if !ok {
		log.Printf("Not exist:%v", base_tool.RequestProto)
		return
	}
	ipport, ok := commonConfigs[base_tool.IPPort]
	if !ok {
		log.Printf("Not exist:%v", base_tool.IPPort)
		return
	}

	var baseEsOp base_tool.BaseEsOp
	switch requestProto {
	case base_tool.Http:
		baseEsOp = &base_tool.EsOpNoTls{IpPort: ipport}
	case base_tool.Https:
		clientCertFile, ok := commonConfigs[base_tool.ClientCertFile]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClientCertFile)
			return
		}
		clientKeyFile, ok := commonConfigs[base_tool.ClientKeyFile]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClientKeyFile)
			return
		}
		caCertFile, ok := commonConfigs[base_tool.CaCertFile]
		if !ok {
			log.Printf("Not exist:%v", base_tool.CaCertFile)
			return
		}

		baseEsOp = &base_tool.EsOpWithTls{IpPort: ipport, ClientCertFile: clientCertFile,
			ClientKeyFile: clientKeyFile, CaCertFile: caCertFile}
	default:
		log.Printf("Invalid requestProto:%v", requestProto)
		return
	}

	compositeOp := base_tool.Create(baseEsOp)

	err = execCmd(cmd, compositeOp, configs, commonConfigs, cmdCfgDir)
	if err != nil {
		log.Printf("Exec cmd:%v failed! err:%v", cmd, err)
		return
	}

	execStatusSuccess = true
} // }}}

func execCmd(cmd string, compositeOp *base_tool.CompositeOp, cmdConfigs, commonConfigs map[string]string, cmdCfgDir string) error { // {{{
	// 执行命令
	switch cmd {
	case base_tool.GetClusterHealth:
		_, respJson, err := compositeOp.GetClusterHealth()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("%v", respJson)
	case base_tool.CheckClusterName:
		// 读取当前请求命令
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}
		isExist, err := compositeOp.CheckClusterName(clusterName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		if isExist {
			log.Printf("[Equal] cluster name equal to %v", clusterName)
		} else {
			log.Printf("[Not Equal] cluster name not equal to %v", clusterName)
		}
	case base_tool.GetIndiceStatus:
		// 读取当前请求命令
		indexName, ok := cmdConfigs[base_tool.IndexName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndexName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndexName}
		}
		indiceInfo, err := compositeOp.GetIndice(indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		log.Printf("%v", indiceInfo)
	case base_tool.SetIndiceAllocationOnAndOff:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[base_tool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", base_tool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return base_tool.Error{Code: base_tool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to set allocaion on and off index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.SetIndiceAllocationOnAndOff(clusterName, indiceName, waitSeconds)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to set allocaion on and off index:%v of cluster:%v\n", indiceName, clusterName)
		}
	case base_tool.CreateIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[base_tool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", base_tool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return base_tool.Error{Code: base_tool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to create index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.CreateIndice(clusterName, indiceName, waitSeconds)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to create index:%v of cluster:%v\n", indiceName, clusterName)
		}
	case base_tool.GetUnhealthIndicesWithNoClose:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		nohealthIndices := make([]base_tool.IndiceInfo, 0)
		for _, indiceInfo := range indicesInfo {
			if indiceInfo.Status != base_tool.Close && indiceInfo.Health != base_tool.Green {
				nohealthIndices = append(nohealthIndices, indiceInfo)
			}
		}

		printStr := make([]string, 0)
		for _, nohealthIndice := range nohealthIndices {
			tmp := fmt.Sprintf("%v\n", nohealthIndice)
			printStr = append(printStr, tmp)
		}
		log.Printf("\n%s", printStr)
	case base_tool.GetCloseIndices:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		closeIndices := make([]base_tool.IndiceInfo, 0)
		for _, indiceInfo := range indicesInfo {
			if indiceInfo.Status == base_tool.Close {
				closeIndices = append(closeIndices, indiceInfo)
			}
		}

		printStr := make([]string, 0)
		for _, closeIndice := range closeIndices {
			tmp := fmt.Sprintf("%v\n", closeIndice)
			printStr = append(printStr, tmp)
		}
		log.Printf("\n%s", printStr)
	case base_tool.RecoveryUnhealthIndices:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		nohealthIndices := make([]base_tool.IndiceInfo, 0)
		for _, indiceInfo := range indicesInfo {
			if indiceInfo.Status != base_tool.Close && indiceInfo.Health != base_tool.Green {
				nohealthIndices = append(nohealthIndices, indiceInfo)
			}
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[base_tool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", base_tool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return base_tool.Error{Code: base_tool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
		}

		for _, nohealthIndice := range nohealthIndices {
			log.Printf("[Begin] to recovery unhealth index:%v of cluster:%v\n", nohealthIndice.Name, clusterName)
			err = compositeOp.SetIndiceAllocationOnAndOff(clusterName, nohealthIndice.Name, waitSeconds)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to recovery unhealth index:%v of cluster:%v\n", nohealthIndice.Name, clusterName)
		}
    case base_tool.GetIndiceSettings:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to get settings of index:%v \n", indiceName)
            _, mappingStr, err := compositeOp.GetIndexSettings(indiceName)
            if err != nil {
                log.Printf("err:%v", err)
                return err
            }

            log.Printf("settings of %v is:\n%v", indiceName, mappingStr)

			log.Printf("[End] to get settings of index:%v\n", indiceName)
		}
	case base_tool.SetIndiceSettings:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 获取待处理的配置信息
		settingsFile, ok := cmdConfigs[base_tool.SettingsPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.SettingsPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.SettingsPath}
		}
		var settingsPath string
		if strings.Index(strings.Trim(settingsFile, " "), "./") == 0 {
			settingsPath = cmdCfgDir + settingsFile // 使用相对路径
		} else if strings.Index(strings.Trim(settingsFile, " "), "/") == 0 {
			settingsPath = settingsFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", settingsFile)
		}
		settingsContent, err := base_tool.ReadWholeFile(settingsPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to set settings of index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.SetIndiceSettings(clusterName, indiceName, string(settingsContent))
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to set settings of index:%v of cluster:%v\n", indiceName, clusterName)
		}
	case base_tool.GetIndiceMapping:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to get mapping of index:%v \n", indiceName)
            _, mappingStr, err := compositeOp.GetIndexMapping(indiceName)
            if err != nil {
                log.Printf("err:%v", err)
                return err
            }

            log.Printf("mapping of %v is:\n%v", indiceName, mappingStr)

			log.Printf("[End] to get mapping of index:%v\n", indiceName)
		}
	case base_tool.SetIndiceMapping:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 获取待处理的mapping信息
		mappingFile, ok := cmdConfigs[base_tool.MappingPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.MappingPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.SettingsPath}
		}
		var mappingPath string
		if strings.Index(strings.Trim(mappingFile, " "), "./") == 0 {
			mappingPath = cmdCfgDir + mappingFile // 使用相对路径
		} else if strings.Index(strings.Trim(mappingFile, " "), "/") == 0 {
			mappingPath = mappingFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", mappingFile)
		}
		mappingContent, err := base_tool.ReadWholeFile(mappingPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to set mapping of index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.SetIndiceMapping(clusterName, indiceName, string(mappingContent))
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to set mapping of index:%v of cluster:%v\n", indiceName, clusterName)
		}
	case base_tool.GetCurrentRecovery:
		_, respJson, err := compositeOp.GetRecoveryInfo()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("%v", respJson)

	case base_tool.DataSink:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		if len(indiceLines) == 0 {
			log.Printf("No indice to sink\n")
			return nil
		}

		// 获取待处理的配置信息
		settingsFile, ok := cmdConfigs[base_tool.SettingsPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.SettingsPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.SettingsPath}
		}
		var settingsPath string
		if strings.Index(strings.Trim(settingsFile, " "), "./") == 0 {
			settingsPath = cmdCfgDir + settingsFile // 使用相对路径
		} else if strings.Index(strings.Trim(settingsFile, " "), "/") == 0 {
			settingsPath = settingsFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", settingsFile)
		}
		settingsContent, err := base_tool.ReadWholeFile(settingsPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[base_tool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", base_tool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return base_tool.Error{Code: base_tool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to sink of index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.SetIndiceSettings(clusterName, indiceName, string(settingsContent))
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}

			err = compositeOp.SetIndiceAllocationOnAndOff(clusterName, indiceName, waitSeconds)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to sink of index:%v of cluster:%v\n", indiceName, clusterName)
		}

	case base_tool.CloseIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to close index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.CloseIndice(clusterName, indiceName)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to close index:%v of cluster:%v\n", indiceName, clusterName)
		}
	case base_tool.OpenIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[base_tool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", base_tool.IndicesPath)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + base_tool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
//		indicesContent, err := base_tool.ReadWholeFile(indicesPath)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}
//		indiceLines, err := base_tool.GetLines(indicesContent)
//		if err != nil {
//			log.Printf("err:%v", err)
//			return err
//		}

		indiceLines, err := base_tool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[base_tool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", base_tool.ClusterName)
			return base_tool.Error{Code: base_tool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to open index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.OpenIndice(clusterName, indiceName)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to open index:%v of cluster:%v\n", indiceName, clusterName)
		}
	default:
		log.Printf("Invalid cmd:%v", cmd)
		return base_tool.Error{Code: base_tool.ErrInvalidParam, Message: "Invalid cmd" + cmd}
	}

	return nil
} // }}}
