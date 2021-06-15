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
	//	"time"
	gomock "github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	json "github.com/json-iterator/go"
	"reflect"
	"strings"
	"testing"
)

func Test_GetInfoInternal_Normal_Get(t *testing.T) { // {{{
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
		`{"cluster_name" : "HaveTryTwo_First_Two"}`,
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
		respMap, respByte, err := compositeOp.getInfoInternal(srcClusterReq)
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
} // }}}

func Test_GetInfoInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcClusterReq := "_cluster/settings?pretty"
	srcClusterResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/_cluster/settingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "aaa"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "aaa"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.getInfoInternal(srcClusterReq)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		//			if err != errors[i] {
		//				t.Fatalf("err %v is not expect: %v", err, errors[i])
		//			}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetInfoInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.getInfoInternal(srcClusterReq)
		if err == nil {
			t.Fatalf("Expect getInfoInternal %v excute failed, but err is nil", srcClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetInfoInternal_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "uri is nil"}
	compositeOp := Create(nil)
	_, _, err := compositeOp.getInfoInternal("")
	if err == nil {
		t.Fatalf("getInfoInternal expect to be failed:%v, but err is nil", expectErr)
	}

	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}
	if err != expectErr {
		t.Fatalf("err %v is not expect: %v", err, expectErr)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

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

func Test_GetClusterHealth_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcClusterReq := "_cluster/health?pretty"
	srcClusterResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/_cluster/settingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "aaa"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "aaa"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetClusterHealth()
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		//			if err != errors[i] {
		//				t.Fatalf("err %v is not expect: %v", err, errors[i])
		//			}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetClusterHealth_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetClusterHealth()
		if err == nil {
			t.Fatalf("Expect GetClusterHealth %v excute failed, but err is nil", srcClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
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

func Test_CheckClusterName_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.CheckClusterName("aabb")
		if err == nil {
			t.Fatalf("Expect CheckClusterName %v excute failed, but err is nil", srcClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_CheckClusterName_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcClusterReq := "_cluster/health?pretty"
	srcClusterResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/_cluster/settingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "aaa"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "aaa"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.CheckClusterName("aabb")
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		//			if err != errors[i] {
		//				t.Fatalf("err %v is not expect: %v", err, errors[i])
		//			}

		t.Logf("Exception Test! err:%v", err)
	}
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

func Test_GetIndicesInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	prefixs := []string{"aa/bb", "cc", "ee", "ff", "gg"}
	srcIndicesReqs := make([]string, 0)
	for _, prefix := range prefixs {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+prefix+"*?pretty")
	}
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, srcIndicesReq := range srcIndicesReqs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.getIndicesInternal(srcIndicesReq)
		if err == nil {
			t.Fatalf("Expect getIndicesInternal %v excute failed, but err is nil", srcIndicesReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
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

func Test_GetIndicesStartWith_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	prefixs := []string{"aa/bb", "cc", "ee", "ff", "gg"}
	srcIndicesReqs := make([]string, 0)
	for _, prefix := range prefixs {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+prefix+"*?pretty")
	}
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, prefix := range prefixs {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndicesStartWith(prefix)
		if err == nil {
			t.Fatalf("Expect GetIndicesStartWith %v excute failed, but err is nil", prefix)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
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

func Test_GetIndice_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	indiceNames := []string{"aa/bb", "cc", "ee", "ff", "gg"}
	srcIndicesReqs := make([]string, 0)
	for _, indiceName := range indiceNames {
		srcIndicesReqs = append(srcIndicesReqs, "_cat/indices/"+indiceName+"?pretty")
	}
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, indiceName := range indiceNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReqs[i])).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndice(indiceName)
		if err == nil {
			t.Fatalf("Expect GetIndice %v excute failed, but err is nil", indiceName)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
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

func Test_GetIndices_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndices()
		if err == nil {
			t.Fatalf("Expect GetIndices excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
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

func Test_GetSpecialHealthIndices_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetSpecialHealthIndices(Yellow)
		if err == nil {
			t.Fatalf("Expect GetSpecailHealthIndices excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
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

func Test_GetSpecialStatusIndices_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndicesReq := "_cat/indices?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndicesReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetSpecialStatusIndices(Open)
		if err == nil {
			t.Fatalf("Expect GetSpecailStatusIndices excute failed, but err is nil")
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetClusterSettings_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/settings?pretty"
	srcClusterResps := []string{`{
  "persistent" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "none",
          "node_initial_primaries_recoveries" : "4"
        }
      }
    },
    "search" : {
      "remote" : {
        "HaveTryTwo_First_One" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9300"
          ]
        },
        "HaveTryTwo_First_two" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9910"
          ]
        }
      }
    }
  },
  "transient" : { }
}`,
		`{"persistent" : {}, "transient" : {} }`,
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
		respMap, respByte, err := compositeOp.GetClusterSettings()
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
} // }}}

func Test_GetClusterSettings_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcClusterReq := "_cluster/settings?pretty"
	srcClusterResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/_cluster/settingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "aaa"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "aaa"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetClusterSettings()
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		//			if err != errors[i] {
		//				t.Fatalf("err %v is not expect: %v", err, errors[i])
		//			}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetClusterSettings_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/settings?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetClusterSettings()
		if err == nil {
			t.Fatalf("Expect GetClusterSettings %v excute failed, but err is nil", srcClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetValueOfKeyPath_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKeysArr := [][]string{{"persistent.cluster.routing.allocation.enable",
		"persistent.cluster.routing.allocation.node_initial_primaries_recoveries",
	}, {
		"transient.cluster.routing.allocation.enable",
		"transient.cluster.routing.allocation.node_initial_primaries_recoveries",
		"transient.search.remote.HaveTryTwo_First_two.seeds.1",
	}, {
		"just_tests_8.settings.index.routing.allocation.enable",
		"just_tests_8.settings.index.number_of_shards",
		"just_tests_8.settings.index.provided_name",
		"just_tests_8.settings.index.creation_date",
		"just_tests_8.settings.index.number_of_replicas",
		"just_tests_8.settings.index.uuid",
		"just_tests_8.settings.index.version.created",
	},
	}

	srcKeyResultArr := [][]string{{
		"none", "4",
	}, {
		"all", "10", "localhost:9920",
	}, {
		"none", "50", "just_tests_8", "1574793584746", "3", "TwjJ_FUFRke79HMCuhp4MQ", "6030499",
	},
	}

	srcRespMapStrs := []string{`{
  "persistent" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "none",
          "node_initial_primaries_recoveries" : "4"
        }
      }
    }
  },
  "transient" : { }
}`, `{
  "transient" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "all",
          "node_initial_primaries_recoveries" : "10"
        }
      }
    },
    "search" : {
      "remote" : {
        "HaveTryTwo_First_One" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9700",
            "localhost:9710"
          ]
        },
        "HaveTryTwo_First_two" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9910",
            "localhost:9920"
          ]
        }
      }
    }
  },
  "persistent" : { }
}`, `{
  "just_tests_8" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "50",
        "provided_name" : "just_tests_8",
        "creation_date" : "1574793584746",
        "number_of_replicas" : "3",
        "uuid" : "TwjJ_FUFRke79HMCuhp4MQ",
        "version" : {
          "created" : "6030499"
        }
      }
    }
  }
}`,
	}

	for i, srcKeys := range srcKeysArr {
		for j, srcKey := range srcKeys {
			srcKeyTerms := strings.Split(strings.Trim(string(srcKey), " "), ".")
			var srcRespMap map[string]interface{}
			err := json.Unmarshal([]byte(srcRespMapStrs[i]), &srcRespMap)
			if err != nil {
				t.Fatalf("Failed to json unmarshall:%v err:%v", srcRespMapStrs[i], err)
			}

			dstResult, err := getValueOfKeyPath(srcKey, srcKeyTerms, srcRespMap)
			if err != nil {
				t.Fatalf("Failed to getValueOfKeyPath:%v, err:%v", srcKey, err)
			}

			typeOfDstResult := reflect.TypeOf(dstResult)
			if typeOfDstResult.Kind() != reflect.String {
				t.Fatalf("Type of dstResult:%v not String which is %v", dstResult, typeOfDstResult)
			}

			cmpDiff := cmp.Diff(dstResult, srcKeyResultArr[i][j])
			if cmpDiff != "" {
				t.Fatalf("Value:%v not equal to src:%v, diff:%v, key:%v, srcRespMap:%v", dstResult,
					srcKeyResultArr[i][j], cmpDiff, srcKey, srcRespMap)
			}
		}
	}
} // }}}

func Test_GetValueOfKeyPath_Exception_Err(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKeysArr := [][]string{{"persistent.cluster.routing.allocation.enabled",
		"persistent.cluster.routing.allocation.node_initial_primaries_recoveries.xx",
		"persistent.cluster.routing.allocation.enabled.xx",
	}, {
		"transient.cluster.routing.allocation.enable.xx",
		"transient.cluster.routing.allocation.node_initial_primaries_recoveriesdd",
		"transient.search.remote.HaveTryTwo_First_two.seeds.100",
		"transient.search.remote.HaveTryTwo_First_two.seeds.aaa",
	}, {
		"just_tests_8.settings.index.routing.allocation.enabledd",
		"just_tests_8.settings.index.number_of_shards.xxx",
	}, {
		"",
	},
	}
	errors := [][]Error{{
		{ErrNotFound, "Not found! key:persistent.cluster.routing.allocation.enabled, termKey:enabled"},
		{ErrNotFound, "Not found! key:persistent.cluster.routing.allocation.node_initial_primaries_recoveries.xx," +
			" termKey:xx, resp type:string"},
		{ErrNotFound, "Not found! key:persistent.cluster.routing.allocation.enabled.xx, termKey:enabled"},
	}, {
		{ErrNotFound, "Not found! key:transient.cluster.routing.allocation.enable.xx, termKey:xx, resp type:string"},
		{ErrNotFound, "Not found! key:transient.cluster.routing.allocation.node_initial_primaries_recoveriesdd," +
			" termKey:node_initial_primaries_recoveriesdd"},
		{ErrInvalidIndex, "index too large: 100, while size of array:2"},
		{ErrAtoiFailed, "keyTerm not int: aaa, while settings is array"},
	}, {
		{ErrNotFound, "Not found! key:just_tests_8.settings.index.routing.allocation.enabledd, termKey:enabledd"},
		{ErrNotFound, "Not found! key:just_tests_8.settings.index.number_of_shards.xxx, termKey:xxx, resp type:string"},
	}, {
		{ErrInvalidParam, "key or keyTerms or respMap is empty"},
	},
	}

	srcRespMapStrs := []string{`{
  "persistent" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "none",
          "node_initial_primaries_recoveries" : "4"
        }
      }
    }
  },
  "transient" : { }
}`, `{
  "transient" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "all",
          "node_initial_primaries_recoveries" : "10"
        }
      }
    },
    "search" : {
      "remote" : {
        "HaveTryTwo_First_two" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9910",
            "localhost:9920"
          ]
        }
      }
    }
  },
  "persistent" : { }
}`, `{
  "just_tests_8" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "50"
      }
    }
  }
}`, `{
    "just_tests_10" : {}
}`,
	}

	for i, srcKeys := range srcKeysArr {
		for j, srcKey := range srcKeys {
			srcKeyTerms := strings.Split(strings.Trim(string(srcKey), " "), ".")
			var srcRespMap map[string]interface{}
			err := json.Unmarshal([]byte(srcRespMapStrs[i]), &srcRespMap)
			if err != nil {
				t.Fatalf("Failed to json unmarshall:%v err:%v", srcRespMapStrs[i], err)
			}

			_, err = getValueOfKeyPath(srcKey, srcKeyTerms, srcRespMap)
			if err == nil {
				t.Fatalf("Expect getValueOfKeyPath to be error:%v, but err is nil", errors[i][j])
			}

			code, _ := DecodeErr(err)
			if code != errors[i][j].Code {
				t.Fatalf("err code:%v is not expect: %v", code, errors[i][j].Code)
			}

			if err != errors[i][j] {
				t.Fatalf("err %v is not expect: %v", err, errors[i][j])
			}

			t.Logf("Exception Test! err:%v", err)
		}
	}
} // }}}

func Test_GetClusterSettingsOfKey_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKeysArr := [][]string{{"persistent.cluster.routing.allocation.enable",
		"persistent.cluster.routing.allocation.node_initial_primaries_recoveries",
	}, {
		"transient.cluster.routing.allocation.enable",
		"transient.cluster.routing.allocation.node_initial_primaries_recoveries",
		"transient.search.remote.HaveTryTwo_First_two.seeds.1",
	},
	}

	srcKeyResultArr := [][]string{{
		"none", "4",
	}, {
		"all", "10", "localhost:9920",
	},
	}

	srcClusterReq := "_cluster/settings?pretty"
	srcClusterResps := []string{`{
  "persistent" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "none",
          "node_initial_primaries_recoveries" : "4"
        }
      }
    }
  },
  "transient" : { }
}`, `{
  "transient" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "all",
          "node_initial_primaries_recoveries" : "10"
        }
      }
    },
    "search" : {
      "remote" : {
        "HaveTryTwo_First_One" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9700",
            "localhost:9710"
          ]
        },
        "HaveTryTwo_First_two" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9910",
            "localhost:9920"
          ]
        }
      }
    }
  },
  "persistent" : { }
}`,
	}

	for i, srcKeys := range srcKeysArr {
		for j, srcKey := range srcKeys {
			mockEsOp := NewMockBaseEsOp(ctrl)
			mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

			compositeOp := Create(mockEsOp)
			dstResult, err := compositeOp.GetClusterSettingsOfKey(srcKey)
			if err != nil {
				t.Fatalf("Failed to check %v, err:%v", srcClusterReq, err)
			}

			typeOfDstResult := reflect.TypeOf(dstResult)
			if typeOfDstResult.Kind() != reflect.String {
				t.Fatalf("Type of dstResult:%v not String which is %v", dstResult, typeOfDstResult)
			}

			cmpDiff := cmp.Diff(dstResult, srcKeyResultArr[i][j])
			if cmpDiff != "" {
				t.Fatalf("Value:%v not equal to expect:%v, diff:%v, key:%v", dstResult,
					srcKeyResultArr[i][j], cmpDiff, srcKey)
			}
		}
	}
} // }}}

func Test_GetClusterSettingsOfKey_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKey := "transient.cluster.routing.allocation.enable"
	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcClusterReq := "_cluster/settings?pretty"
	srcClusterResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/_cluster/settingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "aaa"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "aaa"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetClusterSettingsOfKey(srcKey)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		//			if err != errors[i] {
		//				t.Fatalf("err %v is not expect: %v", err, errors[i])
		//			}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetClusterSettingsOfKey_Exception_ErrFound(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKeysArr := [][]string{{"persistent.cluster.routing.allocation.enabled",
		"persistent.cluster.routing.allocation.node_initial_primaries_recoveries.xx",
		"persistent.cluster.routing.allocation.enabled.xx",
	}, {
		"transient.cluster.routing.allocation.enable.xx",
		"transient.cluster.routing.allocation.node_initial_primaries_recoveriesdd",
		"transient.search.remote.HaveTryTwo_First_two.seeds.100",
		"transient.search.remote.HaveTryTwo_First_two.seeds.aaa",
	}, {
		"just_tests_8.settings.index.routing.allocation.enabledd",
		"just_tests_8.settings.index.number_of_shards.xxx",
	},
	}
	errors := [][]Error{{
		{ErrNotFound, "Not found! key:persistent.cluster.routing.allocation.enabled, termKey:enabled"},
		{ErrNotFound, "Not found! key:persistent.cluster.routing.allocation.node_initial_primaries_recoveries.xx," +
			" termKey:xx, resp type:string"},
		{ErrNotFound, "Not found! key:persistent.cluster.routing.allocation.enabled.xx, termKey:enabled"},
	}, {
		{ErrNotFound, "Not found! key:transient.cluster.routing.allocation.enable.xx, termKey:xx, resp type:string"},
		{ErrNotFound, "Not found! key:transient.cluster.routing.allocation.node_initial_primaries_recoveriesdd," +
			" termKey:node_initial_primaries_recoveriesdd"},
		{ErrInvalidIndex, "index too large: 100, while size of array:2"},
		{ErrAtoiFailed, "keyTerm not int: aaa, while settings is array"},
	}, {
		{ErrNotFound, "Not found! key:just_tests_8.settings.index.routing.allocation.enabledd, termKey:enabledd"},
		{ErrNotFound, "Not found! key:just_tests_8.settings.index.number_of_shards.xxx, termKey:xxx, resp type:string"},
	},
	}

	srcClusterReq := "_cluster/settings?pretty"
	srcClusterResps := []string{`{
  "persistent" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "none",
          "node_initial_primaries_recoveries" : "4"
        }
      }
    }
  },
  "transient" : { }
}`, `{
  "transient" : {
    "cluster" : {
      "routing" : {
        "allocation" : {
          "enable" : "all",
          "node_initial_primaries_recoveries" : "10"
        }
      }
    },
    "search" : {
      "remote" : {
        "HaveTryTwo_First_two" : {
          "skip_unavailable" : "true",
          "seeds" : [
            "localhost:9910",
            "localhost:9920"
          ]
        }
      }
    }
  },
  "persistent" : { }
}`, `{
  "just_tests_8" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "50"
      }
    }
  }
}`,
	}

	for i, srcKeys := range srcKeysArr {
		for j, srcKey := range srcKeys {
			mockEsOp := NewMockBaseEsOp(ctrl)
			mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

			compositeOp := Create(mockEsOp)
			_, err := compositeOp.GetClusterSettingsOfKey(srcKey)
			if err == nil {
				t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i][j])
			}

			code, _ := DecodeErr(err)
			if code != errors[i][j].Code {
				t.Fatalf("err code:%v is not expect: %v", code, errors[i][j].Code)
			}

			if err != errors[i][j] {
				t.Fatalf("err %v is not expect: %v", err, errors[i][j])
			}

			t.Logf("Exception Test! err:%v", err)
		}
	}
} // }}}

func Test_GetClusterSettingsOfKey_Exception_EmptyKey(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "key is nil"}
	compositeOp := Create(nil)
	_, err := compositeOp.GetClusterSettingsOfKey("")
	if err == nil {
		t.Fatalf("GetClusterSettingsOfKey should be failed:%v, but err is nil", expectErr)
	}

	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}
	if err != expectErr {
		t.Fatalf("err %v is not expect: %v", err, expectErr)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_GetClusterSettingsOfKey_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKey := "transient.cluster.routing.allocation.enable"
	srcClusterReq := "_cluster/settings?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetClusterSettingsOfKey(srcKey)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not equal to %v", code, errors[i].Code)
		}

		if err != errors[i] {
			t.Fatalf("err %v is not equal to %v", err, errors[i])
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexSettings_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "just_tests_19"}
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+"/_settings?pretty")
	}
	srcIndexResps := []string{`{
  "just_tests_10" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88",
        "provided_name" : "just_tests_10",
        "creation_date" : "1609029284313",
        "number_of_replicas" : "5",
        "uuid" : "iX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
  "just_tests_15" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "18",
        "provided_name" : "just_tests_15",
        "creation_date" : "1609129284318",
        "number_of_replicas" : "7",
        "uuid" : "OX5jxJsdMQvmdhOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
  "just_tests_18" : {
  }
}`, `{
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		var srcIndexRespMap map[string]interface{}
		err := json.Unmarshal([]byte(srcIndexResps[i]), &srcIndexRespMap)
		if err != nil {
			t.Fatalf("Failed to json unmarshall:%v err:%v", srcIndexResps[i], err)
		}
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respMap, respByte, err := compositeOp.GetIndexSettings(srcIndexName)
		if err != nil {
			t.Fatalf("Failed to check %v, err:%v", srcIndexReqs[i], err)
		}

		if respByte != srcIndexResps[i] {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcIndexResps[i])
		}

		if len(respMap) != len(srcIndexRespMap) {
			t.Fatalf("Num:%v of RespMap not equal to mock resp:%v", len(respMap), len(srcIndexRespMap))
		}

		cmpDiff := cmp.Diff(respMap, srcIndexRespMap)
		if cmpDiff != "" {
			t.Fatalf("Value:%v not equal to expect:%v, diff:%v", respMap, srcIndexRespMap, cmpDiff)
		}
	}
} // }}}

func Test_GetIndexSettings_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	srcIndexReq := "just_tests_18/_settings?pretty"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "just_test_18"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "just_test_18"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReq)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetIndexSettings(srcIndexName)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexSettings_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexName := "just_tests_18"
	srcIndexReq := "just_tests_18/_settings?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetIndexSettings(srcIndexName)
		if err == nil {
			t.Fatalf("Expect to be failed:%v, but err is nil", srcIndexReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexSettings_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name is nil"}
	compositeOp := Create(nil)
	_, _, err := compositeOp.GetIndexSettings("")
	if err == nil {
		t.Fatalf("GetIndexSettings expect to be failed:%v, but err is nil", expectErr)
	}

	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}
	if err != expectErr {
		t.Fatalf("err %v is not expect: %v", err, expectErr)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_GetIndexSettingsOfKey_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKeysArr := [][]string{{
		"index.routing.allocation.enable",
		"index.number_of_shards",
		"index.provided_name",
		"index.creation_date",
		"index.number_of_replicas",
		"index.uuid",
		"index.version.created",
	}, {
		"index.number_of_shards",
		"index.provided_name",
		"index.creation_date",
		"index.number_of_replicas",
		"index.uuid",
		"index.version.created",
	},
	}

	srcKeyResultArr := [][]string{{
		"none", "50", "just_tests_8", "1574793584746", "3", "TwjJ_FUFRke79HMCuhp4MQ", "6030499",
	}, {
		"1000", "just_tests_15", "1534793584746", "23", "HMjJ_FUFRke79HMCuhp4MQ", "6030499",
	},
	}

	srcIndexNames := []string{"just_tests_8", "just_tests_15"}
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+"/_settings?pretty")
	}
	srcIndexResps := []string{`{
  "just_tests_8" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "50",
        "provided_name" : "just_tests_8",
        "creation_date" : "1574793584746",
        "number_of_replicas" : "3",
        "uuid" : "TwjJ_FUFRke79HMCuhp4MQ",
        "version" : {
          "created" : "6030499"
        }
      }
    }
  }
}`, `{
  "just_tests_15" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "1000",
        "provided_name" : "just_tests_15",
        "creation_date" : "1534793584746",
        "number_of_replicas" : "23",
        "uuid" : "HMjJ_FUFRke79HMCuhp4MQ",
        "version" : {
          "created" : "6030499"
        }
      }
    }
  }
}`,
	}

	for i, srcKeys := range srcKeysArr {
		for j, srcKey := range srcKeys {
			mockEsOp := NewMockBaseEsOp(ctrl)
			mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcIndexResps[i]), nil)

			compositeOp := Create(mockEsOp)

			dstResult, err := compositeOp.GetIndexSettingsOfKey(srcIndexNames[i], srcKey)
			if err != nil {
				t.Fatalf("Failed to GetIndexSettingsOfKey:%v, err:%v", srcKey, err)
			}

			typeOfDstResult := reflect.TypeOf(dstResult)
			if typeOfDstResult.Kind() != reflect.String {
				t.Fatalf("Type of dstResult:%v not String which is %v", dstResult, typeOfDstResult)
			}

			cmpDiff := cmp.Diff(dstResult, srcKeyResultArr[i][j])
			if cmpDiff != "" {
				t.Fatalf("Value:%v not equal to src:%v, diff:%v, key:%v", dstResult,
					srcKeyResultArr[i][j], cmpDiff, srcKey)
			}
		}
	}
} // }}}

func Test_GetIndexSettingsOfKey_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKey := "index.routing.allocation.enable"
	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	srcClusterReq := "just_tests_18/_settings?pretty"
	srcClusterResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_tests_18/_settingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "aaa"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "aaa"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcClusterReq)).Return([]byte(srcClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndexSettingsOfKey(srcIndexName, srcKey)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexSettingsOfKey_Exception_ErrFound(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKeysArr := [][]string{{
		"index.routing.allocation.enabledd",
		"index.number_of_shards.xxx",
	},
	}
	errors := [][]Error{{
		{ErrNotFound, "Not found! key:index.routing.allocation.enabledd, termKey:enabledd"},
		{ErrNotFound, "Not found! key:index.number_of_shards.xxx, termKey:xxx, resp type:string"},
	},
	}

	srcIndexNames := []string{"just_tests_8", "just_tests_15"}
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+"/_settings?pretty")
	}
	srcIndexResps := []string{`{
  "just_tests_8" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "50"
      }
    }
  }
}`,
	}

	for i, srcKeys := range srcKeysArr {
		for j, srcKey := range srcKeys {
			mockEsOp := NewMockBaseEsOp(ctrl)
			mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcIndexResps[i]), nil)

			compositeOp := Create(mockEsOp)
			_, err := compositeOp.GetIndexSettingsOfKey(srcIndexNames[i], srcKey)
			if err == nil {
				t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i][j])
			}

			code, _ := DecodeErr(err)
			if code != errors[i][j].Code {
				t.Fatalf("err code:%v is not expect: %v", code, errors[i][j].Code)
			}

			if err != errors[i][j] {
				t.Fatalf("err %v is not expect: %v", err, errors[i][j])
			}

			t.Logf("Exception Test! err:%v", err)
		}
	}
} // }}}

func Test_GetIndexSettingsOfKey_Exception_EmptyKey(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name or key is nil"}
	compositeOp := Create(nil)
	_, err := compositeOp.GetIndexSettingsOfKey("", "")
	if err == nil {
		t.Fatalf("GetIndexSettingsOfKey should be failed:%v, but err is nil", expectErr)
	}

	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}
	if err != expectErr {
		t.Fatalf("err %v is not expect: %v", err, expectErr)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_GetIndexSettingsOfKey_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcKey := "index.routing.allocation.enable"

	srcIndexName := "just_tests_8"
	srcIndexReq := "just_tests_8/_settings?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.GetIndexSettingsOfKey(srcIndexName, srcKey)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not equal to %v", code, errors[i].Code)
		}

		if err != errors[i] {
			t.Fatalf("err %v is not equal to %v", err, errors[i])
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexMapping_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "just_tests_19"}
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+"/_mapping?pretty")
	}
	srcIndexResps := []string{`{
  "tests_10" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "copy_to_name" : {
            "type" : "keyword",
            "index" : false
          },
          "name" : {
            "type" : "keyword",
            "doc_values" : false,
            "copy_to" : [
              "copy_to_name"
            ]
          },
          "tweet" : {
            "type" : "keyword"
          }
        }
      }
    }
  }
}`, `{
  "tests_15" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : false,
        "properties" : {
          "name" : {
            "type" : "keyword",
            "doc_values" : false
          },
          "tweet" : {
            "type" : "keyword"
          }
        }
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "mappings" : { }
  }
}`, `{
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		var srcIndexRespMap map[string]interface{}
		err := json.Unmarshal([]byte(srcIndexResps[i]), &srcIndexRespMap)
		if err != nil {
			t.Fatalf("Failed to json unmarshall:%v err:%v", srcIndexResps[i], err)
		}
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respMap, respByte, err := compositeOp.GetIndexMapping(srcIndexName)
		if err != nil {
			t.Fatalf("Failed to check %v, err:%v", srcIndexReqs[i], err)
		}

		if respByte != srcIndexResps[i] {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcIndexResps[i])
		}

		if len(respMap) != len(srcIndexRespMap) {
			t.Fatalf("Num:%v of RespMap not equal to mock resp:%v", len(respMap), len(srcIndexRespMap))
		}

		cmpDiff := cmp.Diff(respMap, srcIndexRespMap)
		if cmpDiff != "" {
			t.Fatalf("Value:%v not equal to expect:%v, diff:%v", respMap, srcIndexRespMap, cmpDiff)
		}
	}
} // }}}

func Test_GetIndexMapping_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	srcIndexReq := "just_tests_18/_mapping?pretty"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/smapping?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
  "error" : {
    "root_cause" : [
      {
        "type" : "index_not_found_exception",
        "reason" : "no such index",
        "index_uuid" : "_na_",
        "index" : "just_test_18"
      }
    ],
    "type" : "index_not_found_exception",
    "reason" : "no such index",
    "index_uuid" : "_na_",
    "index" : "just_test_18"
  },
  "status" : 404
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReq)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetIndexMapping(srcIndexName)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexMapping_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexName := "just_tests_18"
	srcIndexReq := "just_tests_18/_mapping?pretty"
	errors := []Error{
		{ErrInvalidParam, "Invalid op: xxxx"},
		{ErrNewRequestFailed, "Failed to create new request!!"},
		{ErrHttpDoFailed, "Get \"http://localhost:39908/_cat/indices?pretty\": " +
			"dial tcp localhost:39908: connect: connection refused"},
		{ErrIoUtilReadAllFailed, "Read content from resp.body failed"},
		{ErrTlsLoadX509Failed, "tls load failed"},
	}

	for i, _ := range errors {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetIndexMapping(srcIndexName)
		if err == nil {
			t.Fatalf("Expect to be failed:%v, but err is nil", srcIndexReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_GetIndexMapping_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name is nil"}
	compositeOp := Create(nil)
	_, _, err := compositeOp.GetIndexMapping("")
	if err == nil {
		t.Fatalf("GetIndexMapping expect to be failed:%v, but err is nil", expectErr)
	}

	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}
	if err != expectErr {
		t.Fatalf("err %v is not expect: %v", err, expectErr)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}
