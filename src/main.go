package main

import (
	basetool "es_util/base_tool"
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
	esToolVersion     bool
)

// GetClusterHealth http/https ip:port  http/https clusterName indexName
func init() { // {{{
	flag.StringVar(&cmdCfgPath, "cfgpath", "", "指定业务命令的配置路径")
	flag.StringVar(&cmdCfgPath, "cfg", "", "指定业务命令的配置路径, cfgpath简写")
	flag.BoolVar(&esToolVersion, "version", false, "获取当前es tool 版本信息")
	flag.Parse()
} // }}}

func printStats() { // {{{
	if execStatusSuccess {
		log.Printf("[Success]")
	} else {
		log.Printf("[Fail]")
		os.Exit(-1)
	}
} // }}}

func main() { // {{{
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	execStatusSuccess = false

	defer printStats()

	if esToolVersion {
		fmt.Printf("ES Tool version:%v\n", basetool.ESToolVersion)
		os.Exit(0)
	}

	if "" == cmdCfgPath {
		log.Println("cfgpath 路径不能为空! 可以采用简化 cfg 指定路径")
		return
	}
	//    log.Printf("cfgpath:%v\n", cmdCfgPath)

	cmdCfgDir, fileName := filepath.Split(cmdCfgPath)
	log.Printf("paths:%v, fileName:%v", cmdCfgDir, fileName)
	isDir, err := basetool.IsDir(cmdCfgDir)
	if err != nil {
		log.Printf("err:%v", err)
		return
	}
	if isDir == false {
		log.Printf("Not dir:%v", cmdCfgDir)
		return
	}

	// 读取当前的命令文件
	configs, err := basetool.ReadCfgFile(cmdCfgPath)
	if err != nil {
		log.Printf("err:%v", err)
		return
	}
	//    log.Printf("configs:%v", configs)

	// 读取当前请求命令
	cmd, ok := configs[basetool.Cmd]
	if !ok {
		log.Printf("Not exist:%v", basetool.Cmd)
		return
	}
	//    log.Printf("cmd:%v", cmd)

	commonFile, ok := configs[basetool.CommonFile]
	if !ok {
		log.Printf("Not exist:%v", basetool.CommonFile)
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
	commonConfigs, err := basetool.ReadCfgFile(commonPath)
	if err != nil {
		log.Printf("Failed to read:%v", commonPath)
		return
	}
	//    log.Printf("commonConfigs:%v", commonConfigs)

	// http 或者 https 请求
	requestProto, ok := commonConfigs[basetool.RequestProto]
	if !ok {
		log.Printf("Not exist:%v", basetool.RequestProto)
		return
	}
	ipport, ok := commonConfigs[basetool.IPPort]
	if !ok {
		log.Printf("Not exist:%v", basetool.IPPort)
		return
	}

	var baseEsOp basetool.BaseEsOp
	switch requestProto {
	case basetool.Http:
		baseEsOp = &basetool.EsOpNoTls{IpPort: ipport}
	case basetool.Https:
		clientCertFile, ok := commonConfigs[basetool.ClientCertFile]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClientCertFile)
			return
		}
		clientKeyFile, ok := commonConfigs[basetool.ClientKeyFile]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClientKeyFile)
			return
		}
		caCertFile, ok := commonConfigs[basetool.CaCertFile]
		if !ok {
			log.Printf("Not exist:%v", basetool.CaCertFile)
			return
		}

		baseEsOp = &basetool.EsOpWithTls{IpPort: ipport, ClientCertFile: clientCertFile,
			ClientKeyFile: clientKeyFile, CaCertFile: caCertFile}
	default:
		log.Printf("Invalid requestProto:%v", requestProto)
		return
	}

	compositeOp := basetool.Create(baseEsOp)

	err = execCmd(cmd, compositeOp, configs, commonConfigs, cmdCfgDir)
	if err != nil {
		log.Printf("Exec cmd:%v failed! err:%v", cmd, err)
		return
	}

	execStatusSuccess = true
} // }}}

func execCmd(cmd string, compositeOp *basetool.CompositeOp, cmdConfigs, commonConfigs map[string]string, cmdCfgDir string) error { // {{{
	// 执行命令
	switch cmd {
	case basetool.GetClusterHealth:
		_, respJson, err := compositeOp.GetClusterHealth()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("%v", respJson)
	case basetool.CheckClusterName:
		// 读取当前请求命令
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
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
	case basetool.GetIndiceStatus:
		// 读取当前请求命令
		indexName, ok := cmdConfigs[basetool.IndexName]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndexName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndexName}
		}
		indiceInfo, err := compositeOp.GetIndice(indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		log.Printf("%v", indiceInfo)
	case basetool.SetIndiceAllocationOnAndOff:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[basetool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", basetool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return basetool.Error{Code: basetool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
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
	case basetool.CreateIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[basetool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", basetool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return basetool.Error{Code: basetool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
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
	case basetool.GetUnhealthIndicesWithNoClose:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		nohealthIndices := make([]basetool.IndiceInfo, 0)
		for _, indiceInfo := range indicesInfo {
			if indiceInfo.Status != basetool.Close && indiceInfo.Health != basetool.Green {
				nohealthIndices = append(nohealthIndices, indiceInfo)
			}
		}

		printStr := make([]string, 0)
		for _, nohealthIndice := range nohealthIndices {
			tmp := fmt.Sprintf("%v\n", nohealthIndice)
			printStr = append(printStr, tmp)
		}
		log.Printf("\n%s", printStr)
	case basetool.GetCloseIndices:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		closeIndices := make([]basetool.IndiceInfo, 0)
		for _, indiceInfo := range indicesInfo {
			if indiceInfo.Status == basetool.Close {
				closeIndices = append(closeIndices, indiceInfo)
			}
		}

		printStr := make([]string, 0)
		for _, closeIndice := range closeIndices {
			tmp := fmt.Sprintf("%v\n", closeIndice)
			printStr = append(printStr, tmp)
		}
		log.Printf("\n%s", printStr)
	case basetool.GetWholeIndices:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		printStr := make([]string, 0)
		for _, indice := range indicesInfo {
			tmp := fmt.Sprintf("%v\n", indice)
			printStr = append(printStr, tmp)
		}
		log.Printf("\n%s", printStr)
	case basetool.RecoveryUnhealthIndices:
		indicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		nohealthIndices := make([]basetool.IndiceInfo, 0)
		for _, indiceInfo := range indicesInfo {
			if indiceInfo.Status != basetool.Close && indiceInfo.Health != basetool.Green {
				nohealthIndices = append(nohealthIndices, indiceInfo)
			}
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[basetool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", basetool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return basetool.Error{Code: basetool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
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
	case basetool.GetIndiceSettings:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
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
	case basetool.SetIndiceSettings:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 获取待处理的配置信息
		settingsFile, ok := cmdConfigs[basetool.SettingsPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.SettingsPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.SettingsPath}
		}
		var settingsPath string
		if strings.Index(strings.Trim(settingsFile, " "), "./") == 0 {
			settingsPath = cmdCfgDir + settingsFile // 使用相对路径
		} else if strings.Index(strings.Trim(settingsFile, " "), "/") == 0 {
			settingsPath = settingsFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", settingsFile)
		}
		settingsContent, err := basetool.ReadWholeFile(settingsPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
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
	case basetool.GetIndiceMapping:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}
		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
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
	case basetool.SetIndiceMapping:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 获取待处理的mapping信息
		mappingFile, ok := cmdConfigs[basetool.MappingPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.MappingPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.SettingsPath}
		}
		var mappingPath string
		if strings.Index(strings.Trim(mappingFile, " "), "./") == 0 {
			mappingPath = cmdCfgDir + mappingFile // 使用相对路径
		} else if strings.Index(strings.Trim(mappingFile, " "), "/") == 0 {
			mappingPath = mappingFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", mappingFile)
		}
		mappingContent, err := basetool.ReadWholeFile(mappingPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
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
	case basetool.GetCurrentRecovery:
		_, respJson, err := compositeOp.GetRecoveryInfo()
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("%v", respJson)

	case basetool.DataSink:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		if len(indiceLines) == 0 {
			log.Printf("No indice to sink\n")
			return nil
		}

		// 获取待处理的配置信息
		settingsFile, ok := cmdConfigs[basetool.SettingsPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.SettingsPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.SettingsPath}
		}
		var settingsPath string
		if strings.Index(strings.Trim(settingsFile, " "), "./") == 0 {
			settingsPath = cmdCfgDir + settingsFile // 使用相对路径
		} else if strings.Index(strings.Trim(settingsFile, " "), "/") == 0 {
			settingsPath = settingsFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", settingsFile)
		}
		settingsContent, err := basetool.ReadWholeFile(settingsPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[basetool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", basetool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err := strconv.Atoi(waitSecondsString)
		if err != nil {
			return basetool.Error{Code: basetool.ErrAtoiFailed, Message: "wait seconds not int: " + waitSecondsString}
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

	case basetool.CloseIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
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
	case basetool.OpenIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
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
	case basetool.DeleteClosedIndices:
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err := basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		// 读取集群名称
		clusterName, ok := cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
		}

		// 处理每一个索引
		for _, indiceName := range indiceLines {
			log.Printf("[Begin] to delete closed index:%v of cluster:%v\n", indiceName, clusterName)
			err = compositeOp.DeleteClosedIndice(clusterName, indiceName)
			if err != nil {
				log.Printf("err:%v", err)
				return err
			}
			log.Printf("[End] to delete closed index:%v of cluster:%v\n", indiceName, clusterName)
		}
	default:
		log.Printf("Invalid cmd:%v", cmd)
		return basetool.Error{Code: basetool.ErrInvalidParam, Message: "Invalid cmd" + cmd}
	}

	return nil
} // }}}
