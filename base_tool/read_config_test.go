// Package basetool implements a tool of es
package basetool

import (
	//	"bufio"
	//	"fmt"
	//	"io/ioutil"
	//	"strings"
	"github.com/google/go-cmp/cmp"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

func writeTmpFile(tmpDir, tmpFilePath, content string) error { // {{{
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		log.Printf("%v", err)
		return Error{Code: ErrMakeDirFailed, Message: err.Error()}
	}

	tmpFp, _ := os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)

	defer tmpFp.Close()

	_, err = io.WriteString(tmpFp, content)
	if err != nil {
		log.Printf("%v", err)
		return Error{Code: ErrWriteFileFailed, Message: err.Error()}
	}

	return nil
} // }}}

func Test_ReadCfgFile_Normal_CreateIndices(t *testing.T) { // {{{
	srcContent := `
## 创建新的索引，然后会打开索引的allocation直到shard创建完毕，然后会关闭索引的allocation
Cmd   CreateIndices

## 记录当前业务使用共用的文件配置，包含IPPort地址, 证书路径等
CommonFile      ./common_file.cfg

## 校验的集群名称，写操作为了避免出错，会校验集群名称
ClusterName         HaveTryTwo_First_One

## 待获取索引路由路径，在该文件中，每行一个索引名
IndicesPath  ./CreateIndices.indicesNames.cfg

## 等待时间，默认为10s
WaitSeconds  2`

	srcCnt := map[string]string{
		"Cmd":         "CreateIndices",
		"CommonFile":  "./common_file.cfg",
		"ClusterName": "HaveTryTwo_First_One",
		"IndicesPath": "./CreateIndices.indicesNames.cfg",
		"WaitSeconds": "2",
	}

	logDir := "../log/" + time.Now().Format("20060102")
	tmpFileName := "CreateIndices.cfg"
	tmpFilePath := logDir + "/" + tmpFileName + "." + time.Now().Format("20060102030405")

	err := writeTmpFile(logDir, tmpFilePath, srcContent)
	if err != nil {
		t.Fatalf("Failed to write tmp file:%v, err:%v", tmpFilePath, err)
	}

	readCnt, err := ReadCfgFile(tmpFilePath)
	if err != nil {
		t.Fatalf("Failed to read tmp file:%v, err:%v", tmpFilePath, err)
	}

	if len(srcCnt) != len(readCnt) {
		t.Fatalf("get num %v of config from %v not equal to %v", len(readCnt), tmpFilePath, len(srcCnt))
	}

	for key, value := range readCnt {
		srcValue, ok := srcCnt[key]
		if !ok {
			t.Fatalf("Failed to find:%v in src content:%v", key, srcCnt)
		}

		if srcValue != value {
			t.Fatalf("value:%v not equal to src value:%v of key:%v", value, srcValue, key)
		}
	}
	time.Sleep(1 * time.Second)
} // }}}

func Test_ReadCfgFile_Exception_NoFile(t *testing.T) { // {{{
	tmpFilePath := "aaxxl332kk"
	_, err := ReadCfgFile(tmpFilePath)
	if err == nil {
		t.Fatalf("Read tmp file:%v should failed but err is nil", tmpFilePath)
	}
	code, _ := DecodeErr(err)
	if code != ErrOpenFileFailed {
		t.Fatalf("err code:%v is not ErrOpenFileFailed:%v", code, ErrOpenFileFailed)
	}
	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_ReadCfgFile_Exception_InvalidFile(t *testing.T) { // {{{
	srcContent := `
## 记录当前业务使用共用的文件配置，包含IPPort地址, 证书路径等
CommonFile      ./common_file.cfg       dd.cfg
WaitSeconds  2`

	logDir := "../log/" + time.Now().Format("20060102")
	tmpFileName := "CreateIndices.cfg"
	tmpFilePath := logDir + "/" + tmpFileName + "." + time.Now().Format("20060102030405")

	err := writeTmpFile(logDir, tmpFilePath, srcContent)
	if err != nil {
		t.Fatalf("Failed to write tmp file:%v, err:%v", tmpFilePath, err)
	}

	_, err = ReadCfgFile(tmpFilePath)
	if err == nil {
		t.Fatalf("Read tmp file:%v should failed but err is nil", tmpFilePath)
	}

	code, _ := DecodeErr(err)
	if code != ErrInvalidContent {
		t.Fatalf("err code:%v is not ErrInvalidContent:%v", code, ErrInvalidContent)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_ReadAllLinesInFile_Normal_IndicesInfo(t *testing.T) { // {{{
	srcContent := `
just_tests_15
just_tests_16

##just_tests_17
 

     just_tests_18
`
	srcCnt := []string{"just_tests_15", "just_tests_16", "just_tests_18"}

	logDir := "../log/" + time.Now().Format("20060102")
	tmpFileName := "IndicesInfo.cfg"
	tmpFilePath := logDir + "/" + tmpFileName + "." + time.Now().Format("20060102030405")

	err := writeTmpFile(logDir, tmpFilePath, srcContent)
	if err != nil {
		t.Fatalf("Failed to write tmp file:%v, err:%v", tmpFilePath, err)
	}

	readCnt, err := ReadAllLinesInFile(tmpFilePath)
	if err != nil {
		t.Fatalf("Failed to read tmp file:%v, err:%v", tmpFilePath, err)
	}

	if len(srcCnt) != len(readCnt) {
		t.Fatalf("get num %v of config from %v not equal to %v", len(readCnt), tmpFilePath, len(srcCnt))
	}

	for _, value := range readCnt {
		exist := CheckExist(value, srcCnt)
		if !exist {
			t.Fatalf("Failed to find:%v in src content:%v", value, srcCnt)
		}
	}
	time.Sleep(1 * time.Second)
} // }}}

func Test_ReadAllLinesInFile_Exception_NoFile(t *testing.T) { // {{{
	tmpFilePath := "xx8989###!!!!^#$%"
	_, err := ReadAllLinesInFile(tmpFilePath)
	if err == nil {
		t.Fatalf("Read tmp file:%v should failed but err is nil", tmpFilePath)
	}
	code, _ := DecodeErr(err)
	if code != ErrOpenFileFailed {
		t.Fatalf("err code:%v is not ErrOpenFileFailed:%v", code, ErrOpenFileFailed)
	}
	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_ReadAllLinesInFile_Exception_InvalidFile(t *testing.T) { // {{{
	srcContent := `
just_tests_15   aaa
`
	logDir := "../log/" + time.Now().Format("20060102")
	tmpFileName := "IndicesInfo.cfg"
	tmpFilePath := logDir + "/" + tmpFileName + "." + time.Now().Format("20060102030405")

	err := writeTmpFile(logDir, tmpFilePath, srcContent)
	if err != nil {
		t.Fatalf("Failed to write tmp file:%v, err:%v", tmpFilePath, err)
	}

	_, err = ReadAllLinesInFile(tmpFilePath)
	if err == nil {
		t.Fatalf("Read tmp file:%v should failed but err is nil", tmpFilePath)
	}
	code, _ := DecodeErr(err)
	if code != ErrInvalidContent {
		t.Fatalf("err code:%v is not ErrInvalidContent:%v", code, ErrInvalidContent)
	}
	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_IsDir_Normal_RealDir(t *testing.T) { // {{{
	dirPath := "/usr"
	dirFlag, err := IsDir(dirPath)
	if err != nil {
		t.Fatalf("Failed to check dir path:%v, err:%v", dirPath, err)
	}

	if dirFlag != true {
		t.Fatalf("path:%v not dir", dirPath)
	}
} // }}}

func Test_IsDir_Normal_File(t *testing.T) { // {{{
	dirPath := "./read_config_test.go"
	dirFlag, err := IsDir(dirPath)
	if err != nil {
		t.Fatalf("Failed to check dir path:%v, err:%v", dirPath, err)
	}

	if dirFlag == true {
		t.Fatalf("path:%v is file, but current is dir", dirPath)
	}
} // }}}

func Test_IsDir_Exception_NotExist(t *testing.T) { // {{{
	dirPath := "asd23452891jkl"
	_, err := IsDir(dirPath)
	if err == nil {
		t.Fatalf("Invalid dir path:%v, but err is nil", dirPath)
	}
	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_ReadWholeFile_Normal_ReadAllFile(t *testing.T) { // {{{
	srcContent := `
## 创建新的索引，然后会打开索引的allocation直到shard创建完毕，然后会关闭索引的allocation
Cmd   CreateIndices

## 记录当前业务使用共用的文件配置，包含IPPort地址, 证书路径等
CommonFile      ./common_file.cfg

## 校验的集群名称，写操作为了避免出错，会校验集群名称
ClusterName         HaveTryTwo_First_One

## 等待时间，默认为10s
WaitSeconds  2`

	logDir := "../log/" + time.Now().Format("20060102")
	tmpFileName := "WholeFile.cfg"
	tmpFilePath := logDir + "/" + tmpFileName + "." + time.Now().Format("20060102030405")

	err := writeTmpFile(logDir, tmpFilePath, srcContent)
	if err != nil {
		t.Fatalf("Failed to write tmp file:%v, err:%v", tmpFilePath, err)
	}

	readCnt, err := ReadWholeFile(tmpFilePath)
	if err != nil {
		t.Fatalf("Failed to read tmp file:%v, err:%v", tmpFilePath, err)
	}

	if len(readCnt) != len(srcContent) {
		t.Fatalf("get num %v of config from %v not equal to %v", len(readCnt), tmpFilePath, len(srcContent))
	}
	if string(readCnt) != srcContent {
		t.Fatalf("value:%v not equal to src content:%v", readCnt, srcContent)
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_ReadWholeFile_Exception_NoFile(t *testing.T) { // {{{
	tmpFilePath := "nmvasd89234k"
	_, err := ReadWholeFile(tmpFilePath)
	if err == nil {
		t.Fatalf("Read tmp file:%v should failed but err is nil", tmpFilePath)
	}
	code, _ := DecodeErr(err)
	if code != ErrOpenFileFailed {
		t.Fatalf("err code:%v is not ErrOpenFileFailed:%v", code, ErrOpenFileFailed)
	}
	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_GetLines_Normal_CheckContent(t *testing.T) { // {{{

	srcContents := []string{`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0
red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb
yellow open  test_index_1                cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb
green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
       close just_tests_12               AInkpffbQU-oYIOJLUI89M`,
		`
just_tests_00

just_tests_01
just_tests_10

just_tests_11
        `,
		``,
	}

	checkContents := [][]string{
		{
			"red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0",
			"red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0",
			"green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b",
			"yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb",
			"yellow open  test_index_1                cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb",
			"green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb",
			"close just_tests_11               VInnpfgbQU-oYVMItaliaw",
			"close just_tests_12               AInkpffbQU-oYIOJLUI89M",
		}, {
			"just_tests_00",
			"just_tests_01",
			"just_tests_10",
			"just_tests_11",
		}, {},
	}

	for i, srcContent := range srcContents {
		retLines, err := GetLines([]byte(srcContent))
		if err != nil {
			t.Fatalf("Failed to GetLines of %v, err:%v", srcContent, err)
		}

		if len(retLines) != len(checkContents[i]) {
			t.Fatalf("len:%v of ret lines not equal to len:%v", len(retLines), len(checkContents[i]))
		}

		tmpDiff := cmp.Diff(retLines, checkContents[i])
		if tmpDiff != "" {
			t.Fatalf("Diff of retLines:%v not equal to %v, which is %v", retLines, checkContents[i], tmpDiff)
		}
	}
} // }}}

func Test_GetLines_Exception_NilContent(t *testing.T) { // {{{
	_, err := GetLines(nil)
	if err == nil {
		t.Fatalf("GetLines should be failed, but err is nil")
	}

	code, _ := DecodeErr(err)
	if code != ErrInvalidParam {
		t.Fatalf("err code:%v is not ErrInvalidParam:%v", code, ErrInvalidParam)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_CheckExist_Normal_RealExist(t *testing.T) { // {{{
	arr := []string{PUT, GET, POST, DELETE, HEAD, PATCH, TRACE, "XXX", "888asdf", "$@%"}
	testArr := []string{PUT, POST, DELETE, GET, PATCH, "$@%"}

	for _, testOne := range testArr {
		exist := CheckExist(testOne, arr)
		if exist != true {
			t.Fatalf("Failed to check %v in arr:%v", testOne, arr)
		}
	}
} // }}}

func Test_CheckExist_Normal_NotExist(t *testing.T) { // {{{
	arr := []string{PUT, GET, POST, DELETE, HEAD, PATCH, TRACE}
	testArr := []string{"aa", "DD", "123345", "##"}

	for _, testOne := range testArr {
		exist := CheckExist(testOne, arr)
		if exist == true {
			t.Fatalf("Failed to check %v in arr:%v", testOne, arr)
		}
	}
} // }}}

func Test_CheckExist_Normal_Empty(t *testing.T) { // {{{
	arr := []string{}
	testArr := []string{}

	for _, testOne := range testArr {
		exist := CheckExist(testOne, arr)
		if exist == true {
			t.Fatalf("Failed to check %v in arr:%v", testOne, arr)
		}
	}
} // }}}
