// Package basetool implements a tool of es
package basetool

import (
	//	"io"
	//	"log"
	//	"os"
	//	"os/exec"
	//	"reflect"
	//	"runtime"
	//	"strconv"
	//	"strings"
	//	"time"
	gomock "github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	json "github.com/json-iterator/go"
	"reflect"
	"testing"
)

func Test_GetClusterHealth_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResps := []string{`{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "timed_out" : false,
  "number_of_nodes" : 6,
  "number_of_data_nodes" : 3,
  "active_primary_shards" : 290,
  "active_shards" : 556,
  "relocating_shards" : 0,
  "initializing_shards" : 0,
  "unassigned_shards" : 0,
  "delayed_unassigned_shards" : 0,
  "number_of_pending_tasks" : 0,
  "number_of_in_flight_fetch" : 0,
  "task_max_waiting_in_queue_millis" : 0,
  "active_shards_percent_as_number" : 100.0
}`,
		`{"cluster_name" : "HaveTryTwo_First_One"}`,
		"{}",
	}

	for _, srcClusterResp := range srcClusterResps {
		var srcClusterRespMap map[string]interface{}
		err := json.Unmarshal([]byte(srcClusterResp), &srcClusterRespMap)
		if err != nil {
			t.Fatalf("Failed to json unmarshall:%v err:%v", srcClusterResp, err)
		}
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

		compositeOp := Create(mockEsOp)
		respMap, respByte, err := compositeOp.GetClusterHealth()
		if err != nil {
			t.Fatalf("Failed to check %v, err:%v", srcClusterReq, err)
		}

		if respByte != srcClusterResp {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcClusterResp)
		}

		if len(respMap) != len(srcClusterRespMap) {
			t.Fatalf("Num:%v of RespMap not equal to mock resp:%v", len(respMap), len(srcClusterRespMap))
		}

		for key, value := range respMap {
			srcValue, ok := srcClusterRespMap[key]
			if !ok {
				t.Fatalf("Key:%v not in srcClusterRespMap:%v", key, srcClusterRespMap)
			}

			cmpDiff := cmp.Diff(value, srcValue)
			if cmpDiff != "" {
				t.Fatalf("Value:%v not equal to src:%v, diff:%v", value, srcValue, cmpDiff)
			}
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetClusterHealth_Normal_Empty_Response(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResp := "{}"

	mockEsOp := NewMockBaseEsOp(ctrl)
	mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

	compositeOp := Create(mockEsOp)
	respMap, respByte, err := compositeOp.GetClusterHealth()
	if err != nil {
		t.Fatalf("Failed to check %v, err:%v", srcClusterReq, err)
	}

	if respByte != srcClusterResp {
		t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcClusterResp)
	}

	if len(respMap) != 0 {
		t.Fatalf("Num:%v of RespMap not equal to mock resp:0", len(respMap))
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetClusterHealth_Exception_GetErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"

	var err error = Error{Code: ErrInvalidParam, Message: "Invalid op: XXPMG"}
	mockEsOp := NewMockBaseEsOp(ctrl)
	mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, err)

	compositeOp := Create(mockEsOp)
	_, _, err = compositeOp.GetClusterHealth()
	if err == nil {
		t.Fatalf("Mock resp failed but err is nil of GetClusterHealth")
	}
	code, _ := DecodeErr(err)
	if code != ErrInvalidParam {
		t.Fatalf("err code:%v is not ErrInvalidParam:%v", code, ErrInvalidParam)
	}
	t.Logf("Exception Test! err:%v", err)
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetClusterHealth_Exception_InvalidResponse(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResp := "xxaabb{ad:}ddd"

	mockEsOp := NewMockBaseEsOp(ctrl)
	mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

	compositeOp := Create(mockEsOp)
	_, _, err := compositeOp.GetClusterHealth()
	if err == nil {
		t.Fatalf("Mock resp failed but err is nil of GetClusterHealth")
	}
	code, _ := DecodeErr(err)
	if code != ErrJsonUnmarshalFailed {
		t.Fatalf("err code:%v is not ErrJsonUnmarshalFailed:%v", code, ErrJsonUnmarshalFailed)
	}
	t.Logf("Exception Test! err:%v", err)
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_CheckClusterName_Normal_Equal(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResps := []string{`{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "timed_out" : false,
  "number_of_nodes" : 6,
  "number_of_data_nodes" : 3,
  "active_primary_shards" : 290,
  "active_shards" : 556,
  "relocating_shards" : 0,
  "initializing_shards" : 0,
  "unassigned_shards" : 0,
  "delayed_unassigned_shards" : 0,
  "number_of_pending_tasks" : 0,
  "number_of_in_flight_fetch" : 0,
  "task_max_waiting_in_queue_millis" : 0,
  "active_shards_percent_as_number" : 100.0
}`,
		`{"cluster_name" : "HaveTryTwo_First_One"}`,
		`{"cluster_name" : "HaveTryTwo_First_two"}`,
		`{"cluster_name" : "aabb"}`,
	}

	srcCheckClusterNames := []string{
		"HaveTryTwo_First_One",
		"HaveTryTwo_First_One",
		"HaveTryTwo_First_two",
		"aabb",
	}

	for i, srcClusterResp := range srcClusterResps {
		var srcClusterRespMap map[string]interface{}
		err := json.Unmarshal([]byte(srcClusterResp), &srcClusterRespMap)
		if err != nil {
			t.Fatalf("Failed to json unmarshall:%v err:%v", srcClusterResp, err)
		}
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

		compositeOp := Create(mockEsOp)
		isEqual, err := compositeOp.CheckClusterName(srcCheckClusterNames[i])
		if err != nil {
			t.Fatalf("Failed to check %v, err:%v", srcClusterReq, err)
		}

		if !isEqual {
			t.Fatalf("check cluster name %v Not Equal to mock resp:%v", srcCheckClusterNames[i], srcClusterResp)
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_CheckClusterName_Normal_NotEqual(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResps := []string{`{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "timed_out" : false,
  "number_of_nodes" : 6,
  "number_of_data_nodes" : 3,
  "active_primary_shards" : 290,
  "active_shards" : 556,
  "relocating_shards" : 0,
  "initializing_shards" : 0,
  "unassigned_shards" : 0,
  "delayed_unassigned_shards" : 0,
  "number_of_pending_tasks" : 0,
  "number_of_in_flight_fetch" : 0,
  "task_max_waiting_in_queue_millis" : 0,
  "active_shards_percent_as_number" : 100.0
}`,
		`{"cluster_name" : "HaveTryTwo_First_One"}`,
		`{"cluster_name" : "HaveTryTwo_First_two"}`,
		`{"cluster_name" : "aabb"}`,
	}

	srcCheckClusterNames := []string{
		"HaveTryTwo_First_Oneaaa",
		"HaveTryTwo_First_Onexxx",
		"HaveTryTwo_First_two1111",
		"aabbbbblll",
	}

	for i, srcClusterResp := range srcClusterResps {
		var srcClusterRespMap map[string]interface{}
		err := json.Unmarshal([]byte(srcClusterResp), &srcClusterRespMap)
		if err != nil {
			t.Fatalf("Failed to json unmarshall:%v err:%v", srcClusterResp, err)
		}
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

		compositeOp := Create(mockEsOp)
		isEqual, err := compositeOp.CheckClusterName(srcCheckClusterNames[i])
		if err != nil {
			t.Fatalf("Failed to check %v, err:%v", srcClusterReq, err)
		}

		if isEqual {
			t.Fatalf("check cluster name %v Equal to mock resp:%v", srcCheckClusterNames[i], srcClusterResp)
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_CheckClusterName_Exception_GetErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"

	var err error = Error{Code: ErrInvalidParam, Message: "Invalid op: XXPMG"}
	mockEsOp := NewMockBaseEsOp(ctrl)
	mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, err)

	compositeOp := Create(mockEsOp)
	_, err = compositeOp.CheckClusterName("aabb")
	if err == nil {
		t.Fatalf("Mock resp failed but err is nil of CheckClusterName")
	}
	code, _ := DecodeErr(err)
	if code != ErrInvalidParam {
		t.Fatalf("err code:%v is not ErrInvalidParam:%v", code, ErrInvalidParam)
	}
	t.Logf("Exception Test! err:%v", err)
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_CheckClusterName_Exception_InvalidResponse(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResp := "xxaabb{ad:}mmnneeddd"

	mockEsOp := NewMockBaseEsOp(ctrl)
	mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

	compositeOp := Create(mockEsOp)
	_, err := compositeOp.CheckClusterName("aabb")
	if err == nil {
		t.Fatalf("Mock resp failed but err is nil of CheckClusterName")
	}
	code, _ := DecodeErr(err)
	if code != ErrJsonUnmarshalFailed {
		t.Fatalf("err code:%v is not ErrJsonUnmarshalFailed:%v", code, ErrJsonUnmarshalFailed)
	}
	t.Logf("Exception Test! err:%v", err)
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_CheckClusterName_Exception_ClusterNameKey_NotFound(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResp := `{"status":"green"}`

	mockEsOp := NewMockBaseEsOp(ctrl)
	mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResp), nil)

	compositeOp := Create(mockEsOp)
	_, err := compositeOp.CheckClusterName("aabb")
	if err == nil {
		t.Fatalf("Mock resp failed but err is nil of CheckClusterName")
	}
	code, _ := DecodeErr(err)
	if code != ErrNotFound {
		t.Fatalf("err code:%v is not ErrNotFound:%v", code, ErrNotFound)
	}
	t.Logf("Exception Test! err:%v", err)
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetIndicesInternal_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indces?pretty"
	srcIndicesResps := []string{`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0
red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb
yellow open  test_index_1                cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb
green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
       close just_tests_12               AInkpffbQU-oYIOJLUI89M`,
		`green  open  just_tests_31               OSDIODUI89234MAXUSQ7QA  1 1      3    0  20.8kb  10.4kb`,
		``,
	}

	srcIndicesInfo := [][]IndiceInfo{
		{{"red", "open", "just_tests_03", "S6GoZ56uSoaHGjXn0nNVRg"},
			{"red", "open", "just_tests_07", "869LZ56uSoaHGjXn0nOJIM"},
			{"green", "open", "just_tests_01", "rVogrm3IR42MBLsPKRl_JQ"},
			{"yellow", "open", "just_tests_00", "1yYOkM4rRcGKZwKVE-PD1Q"},
			{"yellow", "open", "test_index_1", "cUQGSdQvT6GxSunhJEvtXQ"},
			{"green", "open", "just_tests_04", "G7S28w0dS7qJ8yLTYsI7QA"},
			{"", "close", "just_tests_11", "VInnpfgbQU-oYVMItaliaw"},
			{"", "close", "just_tests_12", "AInkpffbQU-oYIOJLUI89M"},
		}, {
			{"green", "open", "just_tests_31", "OSDIODUI89234MAXUSQ7QA"},
		}, {},
	}

	for i, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		respIndicesInfo, err := compositeOp.getIndicesInternal(srcIndicesReq)
		if err != nil {
			t.Fatalf("Failed to getIndicesInternal %v, err:%v", srcIndicesReq, err)
		}

		if len(respIndicesInfo) != len(srcIndicesInfo[i]) {
			t.Fatalf("Num %v of %v not equal to mock resp:%v", len(respIndicesInfo),
				respIndicesInfo, len(srcIndicesInfo[i]))
		}

		if !reflect.DeepEqual(respIndicesInfo, srcIndicesInfo[i]) {
			t.Fatalf("resp:%v not equal to mock resp:%v", respIndicesInfo, srcIndicesInfo[i])
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetIndicesInternal_Exception_InvalidLenOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indces?pretty"
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0 xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1  
`,
		`
close just_tests_11               VInnpfgbQU-oYVMItaliaw xxxx
`,
		`
close just_tests_11              
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.getIndicesInternal(srcIndicesReq)
		if err == nil {
			t.Fatalf("Expect getIndicesInternal excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndicesInternal_Exception_JsonResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indces?pretty"
	srcIndicesResps := []string{`
{
    "error" : "Incorrect HTTP method for uri [/_cat/indicess?pretty] and method [GET], allowed: [POST]",
    "status" : 405
}
`,
		`
{
    "error" : {
        "root_cause" : [
        {
            "type" : "index_not_found_exception",
            "reason" : "no such index",
            "index_uuid" : "_na_",
            "resource.type" : "index_or_alias",
            "resource.id" : "aa",
            "index" : "aa"
        }
        ],
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "resource.type" : "index_or_alias",
        "resource.id" : "aa",
        "index" : "aa"
    },
    "status" : 404
}
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.getIndicesInternal(srcIndicesReq)
		if err == nil {
			t.Fatalf("Expect getIndicesInternal excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndicesStartWith_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	prefixs := []string{"just_tests", "xxx", ""}
	srcIndicesReqs := make([]string, 0)
	for _, prefix := range prefixs {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+prefix+"*?pretty")
	}

	srcIndicesResps := []string{`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0
red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb
yellow open  just_tests_02               cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb
green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
       close just_tests_12               AInkpffbQU-oYIOJLUI89M`,
		`green  open  xxx_aa               OSDIODUI89234MAXUSQ7QA  1 1      3    0  20.8kb  10.4kb`,
		``,
	}

	srcIndicesInfo := [][]IndiceInfo{
		{{"red", "open", "just_tests_03", "S6GoZ56uSoaHGjXn0nNVRg"},
			{"red", "open", "just_tests_07", "869LZ56uSoaHGjXn0nOJIM"},
			{"green", "open", "just_tests_01", "rVogrm3IR42MBLsPKRl_JQ"},
			{"yellow", "open", "just_tests_00", "1yYOkM4rRcGKZwKVE-PD1Q"},
			{"yellow", "open", "just_tests_02", "cUQGSdQvT6GxSunhJEvtXQ"},
			{"green", "open", "just_tests_04", "G7S28w0dS7qJ8yLTYsI7QA"},
			{"", "close", "just_tests_11", "VInnpfgbQU-oYVMItaliaw"},
			{"", "close", "just_tests_12", "AInkpffbQU-oYIOJLUI89M"},
		}, {
			{"green", "open", "xxx_aa", "OSDIODUI89234MAXUSQ7QA"},
		}, {},
	}

	for i, prefix := range prefixs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respIndicesInfo, err := compositeOp.GetIndicesStartWith(prefix)
		if err != nil {
			t.Fatalf("Failed to GetIndicesStartWith prefix %v, err:%v", prefix, err)
		}

		if len(respIndicesInfo) != len(srcIndicesInfo[i]) {
			t.Fatalf("Num %v of %v not equal to mock resp:%v", len(respIndicesInfo),
				respIndicesInfo, len(srcIndicesInfo[i]))
		}

		if !reflect.DeepEqual(respIndicesInfo, srcIndicesInfo[i]) {
			t.Fatalf("resp:%v not equal to mock resp:%v", respIndicesInfo, srcIndicesInfo[i])
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetIndicesStartWith_Exception_InvalidLenOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	prefixs := []string{"just_tests", "xxx", "yy", "zz", "mm"}
	srcIndicesReqs := make([]string, 0)
	for _, prefix := range prefixs {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+prefix+"*?pretty")
	}
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0 xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1  
`,
		`
close just_tests_11               VInnpfgbQU-oYVMItaliaw xxxx
`,
		`
close just_tests_11              
`,
	}

	for i, prefix := range prefixs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndicesStartWith(prefix)
		if err == nil {
			t.Fatalf("Expect GetIndicesStartWith %v excute failed, but err is nil", prefix)
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndicesStartWith_Exception_JsonResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	prefixs := []string{"aa/bb/", "cc//."}
	srcIndicesReqs := make([]string, 0)
	for _, prefix := range prefixs {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+prefix+"*?pretty")
	}
	srcIndicesResps := []string{`
{
    "error" : "Incorrect HTTP method for uri [/_cat/indicess?pretty] and method [GET], allowed: [POST]",
    "status" : 405
}
`,
		`
{
    "error" : {
        "root_cause" : [
        {
            "type" : "index_not_found_exception",
            "reason" : "no such index",
            "index_uuid" : "_na_",
            "resource.type" : "index_or_alias",
            "resource.id" : "aa",
            "index" : "aa"
        }
        ],
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "resource.type" : "index_or_alias",
        "resource.id" : "aa",
        "index" : "aa"
    },
    "status" : 404
}
`,
	}

	for i, prefix := range prefixs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndicesStartWith(prefix)
		if err == nil {
			t.Fatalf("Expect GetIndicesStartWith %v excute failed, but err is nil", prefix)
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndice_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	indiceNames := []string{"just_tests_01", "xxx_aa", "just_tests_11"}
	srcIndicesReqs := make([]string, 0)
	for _, indiceName := range indiceNames {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+indiceName+"?pretty")
	}

	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b`,
		`green  open  xxx_aa               OSDIODUI89234MAXUSQ7QA  1 1      3    0  20.8kb  10.4kb`,
		`close just_tests_11               VInnpfgbQU-oYVMItaliaw`,
	}

	srcIndicesInfo := [][]IndiceInfo{
		{{"green", "open", "just_tests_01", "rVogrm3IR42MBLsPKRl_JQ"}},
		{{"green", "open", "xxx_aa", "OSDIODUI89234MAXUSQ7QA"}},
		{{"", "close", "just_tests_11", "VInnpfgbQU-oYVMItaliaw"}},
	}

	for i, indiceName := range indiceNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respIndicesInfo, err := compositeOp.GetIndice(indiceName)
		if err != nil {
			t.Fatalf("Failed to GetIndice %v, err:%v", indiceName, err)
		}

		if len(respIndicesInfo) != len(srcIndicesInfo[i]) {
			t.Fatalf("Num %v of %v not equal to mock resp:%v", len(respIndicesInfo),
				respIndicesInfo, len(srcIndicesInfo[i]))
		}

		if !reflect.DeepEqual(respIndicesInfo, srcIndicesInfo[i]) {
			t.Fatalf("resp:%v not equal to mock resp:%v", respIndicesInfo, srcIndicesInfo[i])
		}
	}
} // }}}

func Test_GetIndice_Exception_EmptyIndexName(t *testing.T) { // {{{
	compositeOp := Create(nil)
	_, err := compositeOp.GetIndice("")
	if err == nil {
		t.Fatalf("GetIndice should be failed, but err is nil")
	}

	code, _ := DecodeErr(err)
	if code != ErrInvalidParam {
		t.Fatalf("err code:%v is not ErrInvalidParam:%v", code, ErrInvalidParam)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_GetIndice_Exception_InvalidLenOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	indiceNames := []string{"just_tests", "xxx", "yyy", "kk", "jj"}
	srcIndicesReqs := make([]string, 0)
	for _, indiceName := range indiceNames {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+indiceName+"?pretty")
	}
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0 xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1  
`,
		`
close just_tests_11               VInnpfgbQU-oYVMItaliaw xxxx
`,
		`
close just_tests_11              
`,
	}

	for i, indiceName := range indiceNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndice(indiceName)
		if err == nil {
			t.Fatalf("Expect GetIndice %v excute failed, but err is nil", indiceName)
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndice_Exception_JsonResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	indiceNames := []string{"aa/bb/", "cc//."}
	srcIndicesReqs := make([]string, 0)
	for _, indiceName := range indiceNames {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+indiceName+"?pretty")
	}
	srcIndicesResps := []string{`
{
    "error" : "Incorrect HTTP method for uri [/_cat/indicess?pretty] and method [GET], allowed: [POST]",
    "status" : 405
}
`,
		`
{
    "error" : {
        "root_cause" : [
        {
            "type" : "index_not_found_exception",
            "reason" : "no such index",
            "index_uuid" : "_na_",
            "resource.type" : "index_or_alias",
            "resource.id" : "aa",
            "index" : "aa"
        }
        ],
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "resource.type" : "index_or_alias",
        "resource.id" : "aa",
        "index" : "aa"
    },
    "status" : 404
}
`,
	}

	for i, indiceName := range indiceNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndice(indiceName)
		if err == nil {
			t.Fatalf("Expect GetIndice %v excute failed, but err is nil", indiceName)
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndice_Exception_InvalidNumberOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	indiceNames := []string{"just_tests_01", "just_tests_05", "yyy"}
	srcIndicesReqs := make([]string, 0)
	for _, indiceName := range indiceNames {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+indiceName+"?pretty")
	}
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0
`,
		`
red    open  just_tests_05               S6GoZ56uSoaHGjXn0nNVRg 1 0
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
red    open  just_tests_06               S6GoZ56uSoaHGjXn0nNVRg 1 0
`,
		``,
	}

	for i, indiceName := range indiceNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndice(indiceName)
		if err == nil {
			t.Fatalf("Expect GetIndice %v excute failed, but err is nil", indiceName)
		}

		code, _ := DecodeErr(err)
		if code != ErrInvalidNumber {
			t.Fatalf("err code:%v is not ErrInvalidNumber:%v", code, ErrInvalidNumber)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndice_Exception_InvalidNameOfRespIndice(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	indiceNames := []string{"just_tests_01", "just_tests_05", "yyy"}
	srcIndicesReqs := make([]string, 0)
	for _, indiceName := range indiceNames {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+indiceName+"?pretty")
	}
	srcIndicesResps := []string{`
green  open  just_tests_02               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
`,
		`
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
`,
		`
red    open  just_tests_06               S6GoZ56uSoaHGjXn0nNVRg 1 0
`,
	}

	for i, indiceName := range indiceNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return([]byte(srcIndicesResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndice(indiceName)
		if err == nil {
			t.Fatalf("Expect GetIndice %v excute failed, but err is nil", indiceName)
		}

		code, _ := DecodeErr(err)
		if code != ErrNotEqual {
			t.Fatalf("err code:%v is not ErrNotEqual:%v", code, ErrNotEqual)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndices_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0
red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb
yellow open  test_index_1                cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb
green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
       close just_tests_12               AInkpffbQU-oYIOJLUI89M`,
		`green  open  just_tests_31               OSDIODUI89234MAXUSQ7QA  1 1      3    0  20.8kb  10.4kb`,
		``,
	}

	srcIndicesInfo := [][]IndiceInfo{
		{{"red", "open", "just_tests_03", "S6GoZ56uSoaHGjXn0nNVRg"},
			{"red", "open", "just_tests_07", "869LZ56uSoaHGjXn0nOJIM"},
			{"green", "open", "just_tests_01", "rVogrm3IR42MBLsPKRl_JQ"},
			{"yellow", "open", "just_tests_00", "1yYOkM4rRcGKZwKVE-PD1Q"},
			{"yellow", "open", "test_index_1", "cUQGSdQvT6GxSunhJEvtXQ"},
			{"green", "open", "just_tests_04", "G7S28w0dS7qJ8yLTYsI7QA"},
			{"", "close", "just_tests_11", "VInnpfgbQU-oYVMItaliaw"},
			{"", "close", "just_tests_12", "AInkpffbQU-oYIOJLUI89M"},
		}, {
			{"green", "open", "just_tests_31", "OSDIODUI89234MAXUSQ7QA"},
		}, {},
	}

	for i, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		respIndicesInfo, err := compositeOp.GetIndices()
		if err != nil {
			t.Fatalf("Failed to GetIndices %v, err:%v", srcIndicesReq, err)
		}

		if len(respIndicesInfo) != len(srcIndicesInfo[i]) {
			t.Fatalf("Num %v of %v not equal to mock resp:%v", len(respIndicesInfo),
				respIndicesInfo, len(srcIndicesInfo[i]))
		}

		if !reflect.DeepEqual(respIndicesInfo, srcIndicesInfo[i]) {
			t.Fatalf("resp:%v not equal to mock resp:%v", respIndicesInfo, srcIndicesInfo[i])
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetIndices_Exception_InvalidLenOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0 xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1  
`,
		`
close just_tests_11               VInnpfgbQU-oYVMItaliaw xxxx
`,
		`
close just_tests_11              
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndices()
		if err == nil {
			t.Fatalf("Expect GetIndices to excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndices_Exception_JsonResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
{
    "error" : "Incorrect HTTP method for uri [/_cat/indicess?pretty] and method [GET], allowed: [POST]",
    "status" : 405
}
`,
		`
{
    "error" : {
        "root_cause" : [
        {
            "type" : "index_not_found_exception",
            "reason" : "no such index",
            "index_uuid" : "_na_",
            "resource.type" : "index_or_alias",
            "resource.id" : "aa",
            "index" : "aa"
        }
        ],
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "resource.type" : "index_or_alias",
        "resource.id" : "aa",
        "index" : "aa"
    },
    "status" : 404
}
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndices()
		if err == nil {
			t.Fatalf("Expect GetIndices excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetSpecialHealthIndices_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResp := `
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0
red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb
yellow open  test_index_1                cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb
green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
       close just_tests_12               AInkpffbQU-oYIOJLUI89M`

	srcIndicesInfo := make(map[string][]IndiceInfo)
	srcIndicesInfo["red"] = []IndiceInfo{
		{"red", "open", "just_tests_03", "S6GoZ56uSoaHGjXn0nNVRg"},
		{"red", "open", "just_tests_07", "869LZ56uSoaHGjXn0nOJIM"},
	}
	srcIndicesInfo["green"] = []IndiceInfo{
		{"green", "open", "just_tests_01", "rVogrm3IR42MBLsPKRl_JQ"},
		{"green", "open", "just_tests_04", "G7S28w0dS7qJ8yLTYsI7QA"},
	}
	srcIndicesInfo["yellow"] = []IndiceInfo{
		{"yellow", "open", "just_tests_00", "1yYOkM4rRcGKZwKVE-PD1Q"},
		{"yellow", "open", "test_index_1", "cUQGSdQvT6GxSunhJEvtXQ"},
	}
	srcIndicesInfo["other"] = []IndiceInfo{}

	healthReqs := []string{Red, Green, Yellow, "other"}
	for _, health := range healthReqs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		respIndicesInfo, err := compositeOp.GetSpecialHealthIndices(health)
		if err != nil {
			t.Fatalf("Failed to GetSpecialHealthIndices %v, err:%v", health, err)
		}

		if len(respIndicesInfo) != len(srcIndicesInfo[health]) {
			t.Fatalf("Num %v of %v not equal to mock resp:%v", len(respIndicesInfo),
				respIndicesInfo, len(srcIndicesInfo[health]))
		}

		if !reflect.DeepEqual(respIndicesInfo, srcIndicesInfo[health]) {
			t.Fatalf("resp:%v not equal to mock resp:%v", respIndicesInfo, srcIndicesInfo[health])
		}
	}
	// t.Logf("respByte:%v, respMap:%v", respByte, respMap)
} // }}}

func Test_GetSpecialHealthIndices_Exception_InvalidLenOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0 xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1  
`,
		`
close just_tests_11               VInnpfgbQU-oYVMItaliaw xxxx
`,
		`
close just_tests_11              
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetSpecialHealthIndices(Red)
		if err == nil {
			t.Fatalf("Expect GetSpecialHealthIndices to excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetSpecialHealthIndices_Exception_JsonResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
{
    "error" : "Incorrect HTTP method for uri [/_cat/indicess?pretty] and method [GET], allowed: [POST]",
    "status" : 405
}
`,
		`
{
    "error" : {
        "root_cause" : [
        {
            "type" : "index_not_found_exception",
            "reason" : "no such index",
            "index_uuid" : "_na_",
            "resource.type" : "index_or_alias",
            "resource.id" : "aa",
            "index" : "aa"
        }
        ],
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "resource.type" : "index_or_alias",
        "resource.id" : "aa",
        "index" : "aa"
    },
    "status" : 404
}
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetSpecialHealthIndices(Green)
		if err == nil {
			t.Fatalf("Expect GetSpecailHealthIndices excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetSpecialStatusIndices_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResp := `
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 30 0
red    open  just_tests_07               869LZ56uSoaHGjXn0nOJIM 30 0
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b
yellow open  just_tests_00               1yYOkM4rRcGKZwKVE-PD1Q  5 1      0    0   1.2kb   1.2kb
yellow open  test_index_1                cUQGSdQvT6GxSunhJEvtXQ  5 1      0    0   1.2kb   1.2kb
green  open  just_tests_04               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb
       close just_tests_11               VInnpfgbQU-oYVMItaliaw
       close just_tests_12               AInkpffbQU-oYIOJLUI89M`

	srcIndicesInfo := make(map[string][]IndiceInfo)
	srcIndicesInfo["open"] = []IndiceInfo{
		{"red", "open", "just_tests_03", "S6GoZ56uSoaHGjXn0nNVRg"},
		{"red", "open", "just_tests_07", "869LZ56uSoaHGjXn0nOJIM"},
		{"green", "open", "just_tests_01", "rVogrm3IR42MBLsPKRl_JQ"},
		{"yellow", "open", "just_tests_00", "1yYOkM4rRcGKZwKVE-PD1Q"},
		{"yellow", "open", "test_index_1", "cUQGSdQvT6GxSunhJEvtXQ"},
		{"green", "open", "just_tests_04", "G7S28w0dS7qJ8yLTYsI7QA"},
	}
	srcIndicesInfo["close"] = []IndiceInfo{
		{"", "close", "just_tests_11", "VInnpfgbQU-oYVMItaliaw"},
		{"", "close", "just_tests_12", "AInkpffbQU-oYIOJLUI89M"},
	}
	srcIndicesInfo["other"] = []IndiceInfo{}

	statusReqs := []string{Open, Close, "other"}
	for _, status := range statusReqs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		respIndicesInfo, err := compositeOp.GetSpecialStatusIndices(status)
		if err != nil {
			t.Fatalf("Failed to GetSpecialStatusIndices %v, err:%v", status, err)
		}

		if len(respIndicesInfo) != len(srcIndicesInfo[status]) {
			t.Fatalf("Num %v of %v not equal to mock resp:%v", len(respIndicesInfo),
				respIndicesInfo, len(srcIndicesInfo[status]))
		}

		if !reflect.DeepEqual(respIndicesInfo, srcIndicesInfo[status]) {
			t.Fatalf("resp:%v not equal to mock resp:%v", respIndicesInfo, srcIndicesInfo[status])
		}
	}
} // }}}

func Test_GetSpecialStatusIndices_Exception_InvalidLenOfResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
green  open  just_tests_01               rVogrm3IR42MBLsPKRl_JQ  1 1      0    0    522b    261b xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1 0 xxxx
`,
		`
red    open  just_tests_03               S6GoZ56uSoaHGjXn0nNVRg 1  
`,
		`
close just_tests_11               VInnpfgbQU-oYVMItaliaw xxxx
`,
		`
close just_tests_11              
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetSpecialStatusIndices(Open)
		if err == nil {
			t.Fatalf("Expect GetSpecialStatusIndices to excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetSpecialStatusIndices_Exception_JsonResp(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	srcIndicesResps := []string{`
{
    "error" : "Incorrect HTTP method for uri [/_cat/indicess?pretty] and method [GET], allowed: [POST]",
    "status" : 405
}
`,
		`
{
    "error" : {
        "root_cause" : [
        {
            "type" : "index_not_found_exception",
            "reason" : "no such index",
            "index_uuid" : "_na_",
            "resource.type" : "index_or_alias",
            "resource.id" : "aa",
            "index" : "aa"
        }
        ],
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "resource.type" : "index_or_alias",
        "resource.id" : "aa",
        "index" : "aa"
    },
    "status" : 404
}
`,
	}

	for _, srcIndicesResp := range srcIndicesResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return([]byte(srcIndicesResp), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetSpecialStatusIndices(Close)
		if err == nil {
			t.Fatalf("Expect GetSpecailStatusIndices excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != ErrRespErr {
			t.Fatalf("err code:%v is not ErrRespErr:%v", code, ErrRespErr)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}
