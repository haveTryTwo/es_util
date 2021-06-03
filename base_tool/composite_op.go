// Package basetool implements a tool of es
package basetool

import (
	json "github.com/json-iterator/go"
	"io"
	"log"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// operation using http or https
type CompositeOp struct {
	EsOp BaseEsOp
}

// indice information including health status
type IndiceInfo struct {
	Health string // green, yellow, red
	Status string // open, close
	Name   string
	Uuid   string
}

// Get health information of cluster
func (compositeOp *CompositeOp) GetClusterHealth() (map[string]interface{}, string, error) { // {{{
	var err error
	respByte, err := compositeOp.EsOp.Get("_cluster/health?pretty")
	if err != nil {
		log.Printf("Failed to get _cluster/health?pretty, err:%v\n", err.Error())
		return nil, "", err
	}
	//    log.Printf("_cluster/health:%v", string(respByte))

	var respMap map[string]interface{}
	err = json.Unmarshal(respByte, &respMap)
	if err != nil {
		log.Printf("_cluster/health?pretty, err:%v", err.Error())
		return nil, "", err
	}
	// log.Print("_cluster/health:", respMap)

	return respMap, string(respByte), nil
} // }}}

// Check cluster name
func (compositeOp *CompositeOp) CheckClusterName(expectClusterName string) (bool, error) { // {{{
	respMap, _, err := compositeOp.GetClusterHealth()
	if err != nil {
		log.Printf("_cluster/health?pretty, err:%v", err.Error())
		return false, err
	}

	clusterName := respMap["cluster_name"]
	if clusterName == nil {
		log.Printf("no cluster_name in resp of _cluster/health?pretty, err:%v", err.Error())
		return false, Error{Code: ErrNotFound, Message: "No found cluster name"}
	}

	if clusterName != expectClusterName {
		// log.Printf("[Not equal] clusterName:%v not equal to expectClusterName:%v", clusterName, expectClusterName)
		return false, nil
	}

	// log.Printf("[Equal] clusterName:%v equal to expectClusterName:%v", clusterName, expectClusterName)
	return true, nil
} // }}}

// Get indices information
func (compositeOp *CompositeOp) getIndicesInternal(uri string) ([]IndiceInfo, error) { // {{{
	respByte, err := compositeOp.EsOp.Get(uri)
	if err != nil {
		log.Printf("Failed to _cat/indices?pretty, err:%v\n", err.Error())
		return nil, err
	}
	// log.Printf("_cat/indices:%v", string(respByte))

	fullInidces := strings.Split(string(respByte), "\n")
	// log.Printf("indices:%d", len(fullInidces))

	// NOTE: 在执行 _cat/indices相关命令情况下应该获取到不是 {} 这种格式，json格式意味出错
	if json.Valid(respByte) {
		return nil, Error{Code: ErrRespErr, Message: string(respByte)}
	}

	indicesInfo := make([]IndiceInfo, 0)
	for _, indices := range fullInidces {
		fields := strings.Fields(indices)
		// log.Printf("len:%d", len(fields))
		if len(fields) == 10 || len(fields) == 6 {
			indicesInfo = append(indicesInfo, IndiceInfo{Health: fields[0], Status: fields[1], Name: fields[2], Uuid: fields[3]})
		} else if len(fields) == 3 {
			indicesInfo = append(indicesInfo, IndiceInfo{Health: "", Status: fields[0], Name: fields[1], Uuid: fields[2]})
		}
	}

	return indicesInfo, nil
} // }}}

// Get information of indices with prefix
func (compositeOp *CompositeOp) GetIndicesStartWith(prefix string) ([]IndiceInfo, error) { // {{{
	return compositeOp.getIndicesInternal("_cat/indices/" + prefix + "*?pretty")
} // }}}

// Get special indice
func (compositeOp *CompositeOp) GetIndice(indexName string) ([]IndiceInfo, error) { // {{{
	indicesInfo, err := compositeOp.getIndicesInternal("_cat/indices/" + indexName + "?pretty")
	if err != nil {
		//log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
		return indicesInfo, err
	}

	if len(indicesInfo) != 1 {
		// log.Printf("Invalid number of indices %v?pretty, err:%v\n", len(indicesInfo), err.Error())
		return indicesInfo, Error{Code: ErrInvalidNumber, Message: "invalid number of indice:" +
			indexName + " is " + strconv.Itoa(len(indicesInfo))}
	}

	if indicesInfo[0].Name != indexName {
		return indicesInfo, Error{Code: ErrNotEqual, Message: "[Not equal]src indice:" +
			indexName + "; dest indice:" + indicesInfo[0].Name}
	}

	return indicesInfo, err
} // }}}

// Get all indices information
func (compositeOp *CompositeOp) GetIndices() ([]IndiceInfo, error) { // {{{
	return compositeOp.getIndicesInternal("_cat/indices?pretty")
} // }}}

// Get indiecs of special health
func (compositeOp *CompositeOp) GetSpecialHealthIndices(health string) ([]IndiceInfo, error) { // {{{
	indicesInfo, err := compositeOp.GetIndices()
	if err != nil {
		log.Printf("Failed to _cat/indices?pretty, err:%v\n", err.Error())
		return nil, err
	}

	specialIndicesInfo := make([]IndiceInfo, 0)
	for _, indiceInfo := range indicesInfo {
		if health == indiceInfo.Health {
			specialIndicesInfo = append(specialIndicesInfo, indiceInfo)
		}
	}

	// log.Println(prefixIndicesName)
	return specialIndicesInfo, nil
} // }}}

// Get indices of special status
func (compositeOp *CompositeOp) GetSpecialStatusIndices(status string) ([]IndiceInfo, error) { // {{{
	indicesInfo, err := compositeOp.GetIndices()
	if err != nil {
		log.Printf("Failed to _cat/indices?pretty, err:%v\n", err.Error())
		return nil, err
	}

	specialIndicesInfo := make([]IndiceInfo, 0)
	for _, indiceInfo := range indicesInfo {
		if status == indiceInfo.Status {
			specialIndicesInfo = append(specialIndicesInfo, indiceInfo)
		}
	}

	// log.Println(prefixIndicesName)
	return specialIndicesInfo, nil
} // }}}

// Get cluster settings
func (compositeOp *CompositeOp) GetClusterSettings() (map[string]interface{}, error) { // {{{
	respByte, err := compositeOp.EsOp.Get("_cluster/settings?pretty")
	if err != nil {
		log.Printf("Failed to get _cluster/settings?pretty, err:%v\n", err.Error())
		return nil, err
	}
	log.Printf("_cluster/settings:%v", string(respByte))

	var respMap map[string]interface{}
	err = json.Unmarshal(respByte, &respMap)
	if err != nil {
		log.Printf("_cluster/health?pretty, err:%v", err.Error())
		return nil, Error{Code: ErrJsonUnmarshalFailed, Message: err.Error()}
	}
	log.Print("_cluster/settings:", respMap)

	return respMap, nil
} // }}}

func getValueOfKeyPath(key string, keyTerms []string, respMap map[string]interface{}) (interface{}, error) { // {{{
	var subRespMap interface{} = respMap
	for _, keyTerm := range keyTerms {
		switch subRespMap.(type) {
		case map[string]interface{}:
			subRespMap = subRespMap.(map[string]interface{})[keyTerm]
		case []interface{}:
			index, err := strconv.Atoi(keyTerm)
			if err != nil {
				return nil, Error{Code: ErrAtoiFailed, Message: "keyTerm not int: " + keyTerm + ", while settings is array"}
			}
			if index >= len(subRespMap.([]interface{})) {
				return nil, Error{Code: ErrInvalidIndex, Message: "index too large: " + keyTerm + ", while size of array:" +
					strconv.Itoa(len(subRespMap.([]interface{})))}
			}

			subRespMap = subRespMap.([]interface{})[index]
		default:
			if subRespMap == nil {
				return nil, Error{Code: ErrNotFound, Message: "Not found! key:" + key + ", termKey:" + keyTerm}
			} else {
				return nil, Error{Code: ErrNotFound, Message: "Not found! key:" + key + ", termKey:" + keyTerm + ", resp type:" +
					(reflect.TypeOf(subRespMap)).Name()}
			}
		}
	}

	return subRespMap, nil
} // }}}

// Get special setting of cluster
func (compositeOp *CompositeOp) GetClusterSettingsOfKey(key string) (interface{}, error) { // {{{
	if key == "" {
		return nil, Error{Code: ErrInvalidParam, Message: "key is nil"}
	}

	respMap, err := compositeOp.GetClusterSettings()
	if err != nil {
		log.Printf("Failed to get _cluster/settings?pretty, err:%v\n", err.Error())
		return nil, err
	}

	keyTerms := strings.Split(strings.Trim(string(key), " "), ".")
	return getValueOfKeyPath(key, keyTerms, respMap)
} // }}}

func (compositeOp *CompositeOp) getInfoInternal(url string) (map[string]interface{}, string, error) { // {{{
	respByte, err := compositeOp.EsOp.Get(url)
	if err != nil {
		log.Printf("Failed to get %v?pretty, err:%v\n", url, err.Error())
		return nil, "", err
	}
	// log.Printf("%v/_settings:%v", indexName, string(respByte))

	var respMap map[string]interface{}
	err = json.Unmarshal(respByte, &respMap)
	if err != nil {
		log.Printf("Failed to parse json?pretty, err:%v", err.Error())
		return nil, string(respByte), Error{Code: ErrJsonUnmarshalFailed, Message: err.Error()}
	}
	// log.Printf("%v/_settings: %v", indexName, respMap)

	if respMap["error"] != nil || respMap["status"] != nil {
		return respMap, string(respByte), Error{Code: ErrRespErr, Message: "Resp error:" + string(respByte)}
	}

	return respMap, string(respByte), nil
} // }}}

func (compositeOp *CompositeOp) getIndexInfoInternal(indexName string,
	uri string) (map[string]interface{}, string, error) { // {{{

	if indexName == "" {
		return nil, "", Error{Code: ErrInvalidParam, Message: "key is nil"}
	}

	return compositeOp.getInfoInternal(indexName + uri)
} // }}}

// Get indice settings
func (compositeOp *CompositeOp) GetIndexSettings(indexName string) (map[string]interface{}, string, error) { // {{{
	return compositeOp.getIndexInfoInternal(indexName, "/_settings?pretty")
} // }}}

// Get specail setting of indice
func (compositeOp *CompositeOp) GetIndexSettingsOfKey(indexName string, key string) (interface{}, error) { // {{{
	if indexName == "" || key == "" {
		return nil, Error{Code: ErrInvalidParam, Message: "index name or key is nil"}
	}

	respMap, _, err := compositeOp.GetIndexSettings(indexName)
	if err != nil {
		log.Printf("Failed to get %v/_settings?pretty, err:%v\n", indexName, err.Error())
		return nil, err
	}

	keyTerms := make([]string, 0)
	keyTerms = append(keyTerms, indexName)
	keyTerms = append(keyTerms, "settings")
	tmpTerms := strings.Split(strings.Trim(string(key), " "), ".")
	for _, tmpTerm := range tmpTerms {
		keyTerms = append(keyTerms, tmpTerm)
	}

	return getValueOfKeyPath(key, keyTerms, respMap)
} // }}}

// Get indice mapping
func (compositeOp *CompositeOp) GetIndexMapping(indexName string) (map[string]interface{}, string, error) { // {{{
	return compositeOp.getIndexInfoInternal(indexName, "/_mapping/_doc?pretty")
} // }}}

func (compositeOp *CompositeOp) setIndexInternal(indexName string, uri string, params string) error { // {{{
	if indexName == "" || uri == "" || params == "" {
		return Error{Code: ErrInvalidParam, Message: "index name or uri or param is nil"}
	}

	respByte, err := compositeOp.EsOp.Put(indexName+uri, params)
	if err != nil {
		log.Printf("Failed to put %v?pretty params:%v, err:%v\n", indexName, params, err.Error())
		return err
	}
	log.Printf("Resp of put %v%v, params:%v, %v", indexName, uri, params, string(respByte))

	var respMap map[string]interface{}
	err = json.Unmarshal(respByte, &respMap)
	if err != nil {
		log.Printf("Failed to put %v%v params:%v, err:%v\n", indexName, uri, params, err.Error())
		return Error{Code: ErrJsonUnmarshalFailed, Message: err.Error()}
	}

	if respMap["error"] != nil || respMap["status"] != nil {
		return Error{Code: ErrRespErr, Message: "Resp of create error:" + string(respByte)}
	}

	return nil
} // }}}

func (compositeOp *CompositeOp) setIndexSetingsInternal(indexName string, params string) error { // {{{
	if indexName == "" || params == "" {
		return Error{Code: ErrInvalidParam, Message: "index name or param is nil"}
	}

	_, respByteBefore, err := compositeOp.GetIndexSettings(indexName)
	if err != nil {
		return err
	}

	setErr := compositeOp.setIndexInternal(indexName, "/_settings?pretty", params)

	_, respByteAfter, err := compositeOp.GetIndexSettings(indexName)
	if err != nil {
		log.Printf("Failed to get settings of %v, err:%v; before index settings:%v", indexName, err, respByteBefore)
		return err
	}

	Diff(indexName+".settings", respByteBefore, respByteAfter)

	return setErr
} // }}}

func (compositeOp *CompositeOp) setIndexMappingsInternal(indexName string, params string) error { // {{{
	if indexName == "" || params == "" {
		return Error{Code: ErrInvalidParam, Message: "index name or param is nil"}
	}
	_, respByteBefore, err := compositeOp.GetIndexMapping(indexName)
	if err != nil {
		return err
	}

	setErr := compositeOp.setIndexInternal(indexName, "/_mapping/_doc?pretty", params)

	_, respByteAfter, err := compositeOp.GetIndexMapping(indexName)
	if err != nil {
		log.Printf("Failed to get mapping of %v, err:%v; before index mapping:%v", indexName, err, respByteBefore)
		return err
	}

	Diff(indexName+".mapping", respByteBefore, respByteAfter)

	return setErr
} // }}}

func (compositeOp *CompositeOp) createIndexInternal(indexName string) error { // {{{
	return compositeOp.setIndexInternal(indexName, "?pretty", "{}")
} // }}}

// Get recovery infomation of cluster
func (compositeOp *CompositeOp) GetRecoveryInfo() (map[string]interface{}, string, error) { // {{{
	return compositeOp.getInfoInternal("_recovery?active_only=true&pretty")
} // }}}

// First set allocation on so indice could be recovery
// Second set allocation off
func (compositeOp *CompositeOp) SetIndiceAllocationOnAndOff(clusterName, indexName string,
	waitSeconds int) error { // {{{
	if clusterName == "" || indexName == "" || waitSeconds <= 0 {
		return Error{Code: ErrInvalidParam, Message: "cluster name or index name or waitSeconds is nil"}
	}
	exist, err := compositeOp.CheckClusterName(clusterName)
	if err != nil {
		log.Printf("Failed to checkClusterName:%v\n", err.Error())
		return err
	}

	if exist == false {
		log.Printf("Not exist of cluste_name:%v\n", clusterName)
		return Error{Code: ErrNotFound, Message: "Not found cluster_name:" + clusterName}
	}

	indicesInfo, err := compositeOp.GetIndice(indexName)
	if err != nil {
		log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
		return err
	}

	enableValue, err := compositeOp.GetIndexSettingsOfKey(indexName, "index.routing.allocation.enable")
	if err != nil {
		code, msg := DecodeErr(err)
		if code != ErrNotFound {
			log.Printf("Failed to get index:%v of index.routing.allocation.enable, err:%v, msg:%v", indexName, err, msg)
			return err
		} // NOTE: 未找到的 index.routing.allocation.enable 即没有设置，为正常状态
	}

	if enableValue != "all" {
		params := "{\"index.routing.allocation.enable\":\"all\"}"
		err = compositeOp.setIndexSetingsInternal(indexName, params)
		if err != nil {
			log.Printf("Failed to set index.routing.allocation.enable %v?pretty, err:%v\n", indexName, err.Error())
			return err
		}
	}

	for {
		log.Printf("wait %v seconds to get indces info\n", waitSeconds)
		time.Sleep(time.Duration(waitSeconds) * time.Second) // NOTE: 循环等一段时间，判断当前索引是否搬迁完毕

		indicesInfo, err = compositeOp.GetIndice(indexName)
		if err != nil {
			log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
			return err
		}

		if indicesInfo[0].Health != Green {
			log.Printf("Indices %v health:%v\n", indexName, indicesInfo[0].Health)
			continue
		}

		recoveryMap, recoveryStr, err := compositeOp.GetRecoveryInfo()
		if err != nil {
			log.Printf("Failed to get recovery, err:%v\n", err)
			return err
		}

		_, ok := recoveryMap[indexName]
		if ok {
			log.Printf("Found recovery of index:%v, recoveryString:%v, then wait\n", indexName, recoveryStr)
			continue
		}

		break
	}

	params := "{\"index.routing.allocation.enable\":\"none\"}"
	err = compositeOp.setIndexSetingsInternal(indexName, params)
	if err != nil {
		log.Printf("Failed to create %v?pretty, err:%v\n", indexName, err.Error())
		return err
	}

	return nil
} // }}}

// Create indice and set allcation off
func (compositeOp *CompositeOp) CreateIndice(clusterName, indexName string, waitSeconds int) error { // {{{
	if clusterName == "" || indexName == "" || waitSeconds <= 0 {
		return Error{Code: ErrInvalidParam, Message: "cluster name or index name or waitSeconds is nil"}
	}
	exist, err := compositeOp.CheckClusterName(clusterName)
	if err != nil {
		log.Printf("Failed to checkClusterName:%v\n", err.Error())
		return err
	}

	if exist == false {
		log.Printf("Not exist of cluste_name:%v\n", clusterName)
		return Error{Code: ErrNotFound, Message: "Not found cluster_name:" + clusterName}
	}

	//    log.Printf("[Begin] to create index:%v of cluster:%v\n", indexName, clusterName)
	err = compositeOp.createIndexInternal(indexName)
	if err != nil {
		return err
	}

	err = compositeOp.SetIndiceAllocationOnAndOff(clusterName, indexName, waitSeconds)
	if err != nil {
		return err
	}

	//    log.Printf("[End] to create index:%v of cluster:%v\n", indexName, clusterName)
	return nil
} // }}}

// Set indice setttings
func (compositeOp *CompositeOp) SetIndiceSettings(clusterName, indexName, settings string) error { // {{{
	if clusterName == "" || indexName == "" || settings == "" {
		return Error{Code: ErrInvalidParam, Message: "cluster name or index name or settings is nil"}
	}
	exist, err := compositeOp.CheckClusterName(clusterName)
	if err != nil {
		log.Printf("Failed to checkClusterName:%v\n", err.Error())
		return err
	}

	if exist == false {
		log.Printf("Not exist of cluste_name:%v\n", clusterName)
		return Error{Code: ErrNotFound, Message: "Not found cluster_name:" + clusterName}
	}

	_, err = compositeOp.GetIndice(indexName)
	if err != nil {
		log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
		return err
	}

	err = compositeOp.setIndexSetingsInternal(indexName, settings)
	if err != nil {
		log.Printf("Failed to set index:%v for setting:%v ?pretty, err:%v\n", indexName, settings, err.Error())
		return err
	}

	return nil
} // }}}

// Set indice mapping
func (compositeOp *CompositeOp) SetIndiceMapping(clusterName, indexName, mappings string) error { // {{{
	if clusterName == "" || indexName == "" || mappings == "" {
		return Error{Code: ErrInvalidParam, Message: "cluster name or index name or mappings is nil"}
	}
	exist, err := compositeOp.CheckClusterName(clusterName)
	if err != nil {
		log.Printf("Failed to checkClusterName:%v\n", err.Error())
		return err
	}

	if exist == false {
		log.Printf("Not exist of cluste_name:%v\n", clusterName)
		return Error{Code: ErrNotFound, Message: "Not found cluster_name:" + clusterName}
	}

	_, err = compositeOp.GetIndice(indexName)
	if err != nil {
		log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
		return err
	}

	err = compositeOp.setIndexMappingsInternal(indexName, mappings)
	if err != nil {
		log.Printf("Failed to set index:%v for mappings:%v ?pretty, err:%v\n", indexName, mappings, err.Error())
		return err
	}

	return nil
} // }}}

func (compositeOp *CompositeOp) postIndexInternal(indexName string, uri string, params string) error { // {{{
	if indexName == "" || uri == "" || params == "" {
		return Error{Code: ErrInvalidParam, Message: "index name or uri or param is nil"}
	}

	respByte, err := compositeOp.EsOp.Post(indexName+uri, params)
	if err != nil {
		log.Printf("Failed to post %v?pretty params:%v, err:%v\n", indexName, params, err.Error())
		return err
	}
	log.Printf("Resp of post %v%v, params:%v, %v", indexName, uri, params, string(respByte))

	var respMap map[string]interface{}
	err = json.Unmarshal(respByte, &respMap)
	if err != nil {
		log.Printf("Failed to post %v%v params:%v, err:%v\n", indexName, uri, params, err.Error())
		return Error{Code: ErrJsonUnmarshalFailed, Message: err.Error()}
	}

	if respMap["error"] != nil || respMap["status"] != nil {
		return Error{Code: ErrRespErr, Message: "Resp of error:" + string(respByte)}
	}

	return nil
} // }}}

// Close indice
func (compositeOp *CompositeOp) CloseIndice(clusterName, indexName string) error { // {{{
	if clusterName == "" || indexName == "" {
		return Error{Code: ErrInvalidParam, Message: "cluster name or index name is nil"}
	}
	exist, err := compositeOp.CheckClusterName(clusterName)
	if err != nil {
		log.Printf("Failed to checkClusterName:%v\n", err.Error())
		return err
	}

	if exist == false {
		log.Printf("Not exist of cluste_name:%v\n", clusterName)
		return Error{Code: ErrNotFound, Message: "Not found cluster_name:" + clusterName}
	}

	err = compositeOp.postIndexInternal(indexName, "/_close?pretty", "{}")
	if err != nil {
		log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
		return err
	}

	return nil
} // }}}

// Open indice
func (compositeOp *CompositeOp) OpenIndice(clusterName, indexName string) error { // {{{
	if clusterName == "" || indexName == "" {
		return Error{Code: ErrInvalidParam, Message: "cluster name or index name is nil"}
	}
	exist, err := compositeOp.CheckClusterName(clusterName)
	if err != nil {
		log.Printf("Failed to checkClusterName:%v\n", err.Error())
		return err
	}

	if exist == false {
		log.Printf("Not exist of cluste_name:%v\n", clusterName)
		return Error{Code: ErrNotFound, Message: "Not found cluster_name:" + clusterName}
	}

	err = compositeOp.postIndexInternal(indexName, "/_open?pretty", "{}")
	if err != nil {
		log.Printf("Failed to get Indices %v?pretty, err:%v\n", indexName, err.Error())
		return err
	}

	return nil
} // }}}

// Get the difference of two string
func Diff(prefixName string, before, after string) error { // {{{
	logDir := "./log/" + time.Now().Format("20060102")
	err := os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		log.Printf("%v", err)
		return Error{Code: ErrMakeDirFailed, Message: err.Error()}
	}

	prefixPath := logDir + "/" + prefixName + "." + time.Now().Format("20060102030405")
	beforePath := prefixPath + ".before"
	afterPath := prefixPath + ".after"

	beforeFile, _ := os.OpenFile(beforePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	afterFile, _ := os.OpenFile(afterPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)

	defer beforeFile.Close()
	defer afterFile.Close()

	_, err = io.WriteString(beforeFile, before)
	if err != nil {
		log.Printf("%v", err)
		return Error{Code: ErrWriteFileFailed, Message: err.Error()}
	}

	_, err = io.WriteString(afterFile, after)
	if err != nil {
		log.Printf("%v", err)
		return Error{Code: ErrWriteFileFailed, Message: err.Error()}
	}

	cmd := "diff"
	if runtime.GOOS == "plan9" {
		cmd = "/bin/ape/diff"
	}

	data, err := exec.Command(cmd, "-u", beforePath, afterPath).CombinedOutput()
	if len(data) > 0 {
		// diff exits with a non-zero status when the files don't match.
		// Ignore that failure as long as we get output.
		err = nil
	}
	log.Printf("%v %v %v is:%v", cmd, beforePath, afterPath, string(data))
	return err
} // }}}

// Get real operation of es
func Create(esOp BaseEsOp) *CompositeOp { // {{{
	return &(CompositeOp{EsOp: esOp})
} // }}}
