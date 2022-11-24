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

func getConfig(needIndices bool, needClusterName bool, needWaitSeconds bool, cmdConfigs map[string]string,
	cmdCfgDir string) ([]string, string, int, error) { // {{{
	var indiceLines []string
	var err error
	if needIndices {
		// 获取待处理的索引列表
		indicesFile, ok := cmdConfigs[basetool.IndicesPath]
		if !ok {
			log.Printf("Not exist:%v", basetool.IndicesPath)
			return nil, "", 0, basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndicesPath}
		}
		var indicesPath string
		if strings.Index(strings.Trim(indicesFile, " "), "./") == 0 {
			indicesPath = cmdCfgDir + indicesFile // 使用相对路径
		} else if strings.Index(strings.Trim(indicesFile, " "), "/") == 0 {
			indicesPath = indicesFile // 使用绝对路径
		} else {
			log.Printf("Invalid path:%v", indicesFile)
		}

		indiceLines, err = basetool.ReadAllLinesInFile(indicesPath)
		if err != nil {
			log.Printf("err:%v", err)
			return nil, "", 0, err
		}
	}

	var clusterName string = ""
	var ok bool
	if needClusterName {
		// 读取集群名称
		clusterName, ok = cmdConfigs[basetool.ClusterName]
		if !ok {
			log.Printf("Not exist:%v", basetool.ClusterName)
			return nil, "", 0, basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + clusterName}
		}
	}

	var waitSeconds int = 0
	if needWaitSeconds {
		// 读取等待时间
		waitSecondsString, ok := cmdConfigs[basetool.WaitSeconds]
		if !ok {
			log.Printf("Not exist:%v, then using 10 second as default", basetool.WaitSeconds)
			waitSecondsString = "10"
		}

		waitSeconds, err = strconv.Atoi(waitSecondsString)
		if err != nil {
			return nil, "", 0, basetool.Error{Code: basetool.ErrAtoiFailed,
				Message: "wait seconds not int: " + waitSecondsString}
		}
	}

	return indiceLines, clusterName, waitSeconds, nil
} // }}}

func getSettingOrMapping(pathKey string, cmdConfigs map[string]string, cmdCfgDir string) ([]byte, error) { // {{{
	// 获取待处理的配置信息
	confFile, ok := cmdConfigs[pathKey]
	if !ok {
		log.Printf("Not exist:%v", pathKey)
		return nil, basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + pathKey}
	}
	var confPath string
	if strings.Index(strings.Trim(confFile, " "), "./") == 0 {
		confPath = cmdCfgDir + confFile // 使用相对路径
	} else if strings.Index(strings.Trim(confFile, " "), "/") == 0 {
		confPath = confFile // 使用绝对路径
	} else {
		log.Printf("Invalid path:%v", confFile)
	}
	confContent, err := basetool.ReadWholeFile(confPath)
	if err != nil {
		log.Printf("err:%v", err)
		return nil, err
	}

	if len(confContent) == 0 {
		log.Printf("conf of %v is empty!", pathKey)
		return nil, basetool.Error{Code: basetool.ErrInvalidContent, Message: "Empty of " + pathKey}
	}

	return confContent, nil
} // }}}

type execCmdInternalHandler func(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error

var execCmdMapHandlers = map[string]execCmdInternalHandler{
	basetool.GetClusterHealth:              getClusterHealthHandler,
	basetool.CheckClusterName:              checkClusterNameHandler,
	basetool.GetClusterSettings:            getClusterSettingsHandler,
	basetool.GetIndiceStatus:               getIndiceStatusHandler,
	basetool.SetIndiceAllocationOnAndOff:   setIndiceAllocationOnAndOffHandler,
	basetool.CreateIndices:                 createIndicesHandler,
	basetool.GetUnhealthIndicesWithNoClose: getUnhealthIndicesWithNoCloseHandler,
	basetool.GetCloseIndices:               getCloseIndicesHandler,
	basetool.GetWholeIndices:               getWholeIndicesHandler,
	basetool.RecoveryUnhealthIndices:       recoveryUnhealthIndicesHandler,
	basetool.GetIndiceSettings:             getIndiceSettingsHandler,
	basetool.SetIndiceSettings:             setIndiceSettingsHandler,
	basetool.GetIndiceMapping:              getIndiceMappingHandler,
	basetool.SetIndiceMapping:              setIndiceMappingHandler,
	basetool.GetCurrentRecovery:            getCurrentRecoveryHandler,
	basetool.DataSink:                      sinkDataHandler,
	basetool.CloseIndices:                  closeIndicesHandler,
	basetool.OpenIndices:                   openIndicesHandler,
	basetool.DeleteClosedIndices:           deleteClosedIndicesHandler,
}

func getClusterHealthHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	_, respJson, err := compositeOp.GetClusterHealth()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	log.Printf("%v", respJson)
	return nil
}

func checkClusterNameHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
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
	return nil
}

func getClusterSettingsHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	_, respJson, err := compositeOp.GetClusterSettings()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	log.Printf("%v", respJson)
	return nil
}

func getIndiceStatusHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	// 读取当前请求命令
	indexName, ok := cmdConfigs[basetool.IndexName]
	if !ok {
		log.Printf("Not exist:%v", basetool.IndexName)
		return basetool.Error{Code: basetool.ErrNotFound, Message: "Not found " + basetool.IndexName}
	}
	indexInfo, err := compositeOp.GetIndice(indexName)
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}

	log.Printf("%v", indexInfo)
	return nil
}

func setIndiceAllocationOnAndOffHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, clusterName, waitSeconds, err := getConfig(true, true, true, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	err = setAllocationOnAndOffInternal(compositeOp, cmdConfigs, indiceLines, clusterName, waitSeconds)
	if err != nil {
		log.Printf("Failed to setAllocationOnAndOffInternal, err:%v", err)
		return err
	}

	return nil
}

func createIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, clusterName, waitSeconds, err := getConfig(true, true, true, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to create index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.CreateIndice(clusterName, indexName, waitSeconds)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to create index:%v of cluster:%v\n", indexName, clusterName)
	}
	return nil
}

func getUnhealthIndicesWithNoCloseHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indicesInfo, err := compositeOp.GetIndices()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	nohealthIndices := make([]basetool.IndiceInfo, 0)
	for _, indexInfo := range indicesInfo {
		if indexInfo.Status != basetool.Close && indexInfo.Health != basetool.Green {
			nohealthIndices = append(nohealthIndices, indexInfo)
		}
	}

	printStr := make([]string, 0)
	for _, nohealthIndice := range nohealthIndices {
		tmp := fmt.Sprintf("%v\n", nohealthIndice)
		printStr = append(printStr, tmp)
	}
	log.Printf("\n%s", printStr)

	return nil
}

func getCloseIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indicesInfo, err := compositeOp.GetIndices()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	closeIndices := make([]basetool.IndiceInfo, 0)
	for _, indexInfo := range indicesInfo {
		if indexInfo.Status == basetool.Close {
			closeIndices = append(closeIndices, indexInfo)
		}
	}

	printStr := make([]string, 0)
	for _, closeIndice := range closeIndices {
		tmp := fmt.Sprintf("%v\n", closeIndice)
		printStr = append(printStr, tmp)
	}
	log.Printf("\n%s", printStr)
	return nil
}

func getWholeIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indicesInfo, err := compositeOp.GetIndices()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	printStr := make([]string, 0)
	for _, index := range indicesInfo {
		tmp := fmt.Sprintf("%v\n", index)
		printStr = append(printStr, tmp)
	}
	log.Printf("\n%s", printStr)

	return nil
}

func recoveryUnhealthIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indicesInfo, err := compositeOp.GetIndices()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	// nohealthIndices := make([]basetool.IndiceInfo, 0)
	nohealthIndices := make([]string, 0)
	for _, indexInfo := range indicesInfo {
		if indexInfo.Status != basetool.Close && indexInfo.Health != basetool.Green {
			nohealthIndices = append(nohealthIndices, indexInfo.Name)
		}
	}

	_, clusterName, waitSecond, err := getConfig(false, true, true, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	err = setAllocationOnAndOffInternal(compositeOp, cmdConfigs, nohealthIndices, clusterName, waitSecond)
	if err != nil {
		log.Printf("Failed to setAllocationOnAndOffInternal, err:%v", err)
		return err
	}

	return nil
}

func getIndiceSettingsHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, _, _, err := getConfig(true, false, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to get settings of index:%v \n", indexName)
		_, mappingStr, err := compositeOp.GetIndexSettings(indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		log.Printf("settings of %v is:\n%v", indexName, mappingStr)

		log.Printf("[End] to get settings of index:%v\n", indexName)
	}

	return nil
}

func setIndiceSettingsHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	settingsContent, err := getSettingOrMapping(basetool.SettingsPath, cmdConfigs, cmdCfgDir)
	if err != nil {
		return nil
	}

	indiceLines, clusterName, _, err := getConfig(true, true, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to set settings of index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.SetIndiceSettings(clusterName, indexName, string(settingsContent))
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to set settings of index:%v of cluster:%v\n", indexName, clusterName)
	}

	return nil
}

func getIndiceMappingHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, _, _, err := getConfig(true, false, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to get mapping of index:%v \n", indexName)
		_, mappingStr, err := compositeOp.GetIndexMapping(indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		log.Printf("mapping of %v is:\n%v", indexName, mappingStr)

		log.Printf("[End] to get mapping of index:%v\n", indexName)
	}

	return nil
}

func setIndiceMappingHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	mappingContent, err := getSettingOrMapping(basetool.MappingPath, cmdConfigs, cmdCfgDir)
	if err != nil {
		return nil
	}

	indiceLines, clusterName, _, err := getConfig(true, true, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}
	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to set mapping of index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.SetIndiceMapping(clusterName, indexName, string(mappingContent))
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to set mapping of index:%v of cluster:%v\n", indexName, clusterName)
	}

	return nil
}

func getCurrentRecoveryHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	_, respJson, err := compositeOp.GetRecoveryInfo()
	if err != nil {
		log.Printf("err:%v", err)
		return err
	}
	log.Printf("%v", respJson)

	return nil
}

func sinkDataHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	settingsContent, err := getSettingOrMapping(basetool.SettingsPath, cmdConfigs, cmdCfgDir)
	if err != nil {
		return nil
	}

	indiceLines, clusterName, waitSeconds, err := getConfig(true, true, true, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}
	if len(indiceLines) == 0 {
		log.Printf("No indice to sink\n")
		return nil
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to sink of index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.SetIndiceSettings(clusterName, indexName, string(settingsContent))
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}

		err = compositeOp.SetIndiceAllocationOnAndOff(clusterName, indexName, waitSeconds)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to sink of index:%v of cluster:%v\n", indexName, clusterName)
	}

	return nil
}

func closeIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, clusterName, _, err := getConfig(true, true, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to close index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.CloseIndice(clusterName, indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to close index:%v of cluster:%v\n", indexName, clusterName)
	}

	return nil
}

func openIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, clusterName, _, err := getConfig(true, true, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to open index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.OpenIndice(clusterName, indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to open index:%v of cluster:%v\n", indexName, clusterName)
	}

	return nil
}

func deleteClosedIndicesHandler(cmd string, compositeOp *basetool.CompositeOp,
	cmdConfigs map[string]string, cmdCfgDir string) error {
	indiceLines, clusterName, _, err := getConfig(true, true, false, cmdConfigs, cmdCfgDir)
	if err != nil {
		return err
	}

	// 处理每一个索引
	for _, indexName := range indiceLines {
		log.Printf("[Begin] to delete closed index:%v of cluster:%v\n", indexName, clusterName)
		err = compositeOp.DeleteClosedIndice(clusterName, indexName)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to delete closed index:%v of cluster:%v\n", indexName, clusterName)
	}

	return nil
}

func execCmd(cmd string, compositeOp *basetool.CompositeOp, cmdConfigs, commonConfigs map[string]string,
	cmdCfgDir string) error {

    if handler, ok := execCmdMapHandlers[cmd]; ok {
        return handler(cmd, compositeOp, cmdConfigs, cmdCfgDir)
    } else {
		log.Printf("Invalid cmd:%v", cmd)
		return basetool.Error{Code: basetool.ErrInvalidParam, Message: "Invalid cmd" + cmd}
    }

    return nil
}


func setAllocationOnAndOffInternal(compositeOp *basetool.CompositeOp, cmdConfigs map[string]string,
	indicesName []string, clusterName string, waitSeconds int) error { // {{{

	// 读取批量恢复索引时的方向
	opDirectionString, ok := cmdConfigs[basetool.OpDirection]
	if !ok {
		log.Printf("Not exist:%v, then using Positive(0) as default", basetool.OpDirection)
		opDirectionString = "0"
	}

	opDirection, err := strconv.Atoi(opDirectionString)
	if err != nil {
		return basetool.Error{Code: basetool.ErrAtoiFailed, Message: "op direction not int: " + opDirectionString}
	}

	// 读取批量恢复索引时并发个数
	opIndexNumString, ok := cmdConfigs[basetool.OpIndexNum]
	if !ok {
		log.Printf("Not exist:%v, then using 1 as default", basetool.OpIndexNum)
		opIndexNumString = "1"
	}

	opIndexNum, err := strconv.Atoi(opIndexNumString)
	if err != nil {
		return basetool.Error{Code: basetool.ErrAtoiFailed, Message: "op index num not int: " + opIndexNumString}
	}

	if opIndexNum > basetool.MaxConcurrentIndexNum || opIndexNum < 1 {
		return basetool.Error{Code: basetool.ErrInvalidNumber, Message: "Invalid Concurrent number " +
			strconv.Itoa(opIndexNum) + " exceed " + strconv.Itoa(basetool.MaxConcurrentIndexNum) + " or < 0"}
	}

	err = basetool.SortStringArr(indicesName, opDirection)
	if err != nil {
		log.Printf("Failed to sort arr, err:%v", err)
		return err
	}

	// for _, nohealthIndice := range nohealthIndices
	for i := 0; i < len(indicesName); {
		var batchIndicesName []string
		var j = 0
		for ; (i+j) < len(indicesName) && j < opIndexNum; j++ {
			batchIndicesName = append(batchIndicesName, indicesName[i+j])
		}
		i += j

		log.Printf("[Begin] to recover unhealthy index:%v of cluster:%v\n", batchIndicesName, clusterName)
		err = compositeOp.SetBatchIndiceAllocationOnAndOff(clusterName, batchIndicesName, waitSeconds)
		if err != nil {
			log.Printf("err:%v", err)
			return err
		}
		log.Printf("[End] to recover unhealthy index:%v of cluster:%v\n", batchIndicesName, clusterName)
	}

	return nil
} // }}}
