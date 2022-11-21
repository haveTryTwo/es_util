// Package basetool implements a tool of es
package basetool

import (
	//	"io"
	//	"log"
	//	"os/exec"
	//	"reflect"
	//	"runtime"
	//	"strconv"
	gomock "github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	json "github.com/json-iterator/go"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
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

func Test_SetIndexInternal_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "just_tests_19"}
	uris := []string{"/_settings?pretty", "/_settings?pretty", "/_mapping/_doc?pretty", "/_mapping/_doc?pretty"}
	srcIndexReqs := make([]string, 0)
	for i, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uris[i])
	}

	params := []string{`{
        "index.routing.allocation.enable": "none"
    }`, `{
        "index.number_of_replicas":"5"
    }`, `{
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word"
            },
            "name" : {
                "type" : "keyword"
            }
        }
    }`, `{
        "properties": {
            "age" : {
                "type" : "long"
            }
        }
    }`,
	}

	srcIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respByte, err := compositeOp.setIndexInternal(srcIndexName, uris[i], params[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}

		if respByte != srcIndexResps[i] {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcIndexResps[i])
		}
	}
} // }}}

func Test_SetIndexInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	uri := "/_settings?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{\"index.routing.allocation.enable\": \"none\"}"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.setIndexInternal(srcIndexName, uri, param)
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

func Test_SetIndexInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexName := "just_tests_18"
	uri := "/_settings?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{\"index.routing.allocation.enable\": \"none\"}"
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
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.setIndexInternal(srcIndexName, uri, param)
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

func Test_SetIndexInternal_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name or uri or param is nil"}
	compositeOp := Create(nil)
	_, err := compositeOp.setIndexInternal("", "xx", "yy")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_Diff_Normal_Check(t *testing.T) { // {{{
	befores := []string{` {
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
          }
        }
      }
    }
  }
}`, `{
}`, `{
  "tests_10" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : false,
        "properties" : {
          "copy_to_name" : {
            "type" : "keyword",
            "index" : false
          }
      }
    }
  }
}`, `{
}`,
	}

	afters := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88",
        "provided_name" : "just_tests_10",
        "creation_date" : "1609029284313",
        "number_of_replicas" : "50",
        "uuid" : "iX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
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
    "just_tests_10.settings.index.routing.allocation":"all"
}`, `{
}`, `{
}`,
	}

	prefixNames := []string{"tests_1", "tests_2", "tests_3", "tests_4", "tests_5"}

	for i, prefixName := range prefixNames {
		logDir := "./log/" + time.Now().Format("20060102")
		defer os.RemoveAll(logDir)
		prefixPath := logDir + "/" + prefixName + "." + time.Now().Format("20060102030405")
		beforePath := prefixPath + ".before"
		afterPath := prefixPath + ".after"

		err := Diff(prefixName, befores[i], afters[i])
		if err != nil {
			t.Fatalf("Diff %v failed , err:%v", prefixName, err)
		}

		readCnt, err := ReadWholeFile(beforePath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", beforePath, err)
		}

		if len(readCnt) != len(befores[i]) {
			t.Fatalf("get num %v of config from %v not equal to %v", len(readCnt), beforePath, len(befores[i]))
		}
		if string(readCnt) != befores[i] {
			t.Fatalf("value:%v not equal to src content:%v", readCnt, befores[i])
		}

		readCnt, err = ReadWholeFile(afterPath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", afterPath, err)
		}

		if len(readCnt) != len(afters[i]) {
			t.Fatalf("get num %v of config from %v not equal to %v", len(readCnt), afterPath, len(afters[i]))
		}
		if string(readCnt) != afters[i] {
			t.Fatalf("value:%v not equal to src content:%v", readCnt, afters[i])
		}
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexSettingsInternal_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcBeforeGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
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
        "number_of_shards" : "88",
        "provided_name" : "just_tests_15",
        "creation_date" : "1609029784313",
        "number_of_replicas" : "2",
        "uuid" : "MX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
}`,
	}

	srcAfterGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
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
        "number_of_shards" : "88",
        "provided_name" : "just_tests_15",
        "creation_date" : "1609029784313",
        "number_of_replicas" : "5",
        "uuid" : "MX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88",
        "provided_name" : "just_tests_18",
        "creation_date" : "1679029784313",
        "number_of_replicas" : "50",
        "uuid" : "JX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`,
	}

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18"}
	uris := []string{"/_settings?pretty", "/_settings?pretty", "/_settings?pretty"}
	srcIndexReqs := make([]string, 0)
	for i, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uris[i])
	}

	params := []string{`{
        "index.routing.allocation.enable": "none"
    }`, `{
        "index.number_of_replicas":"5"
    }`, `{
        "index.number_of_replicas":"50"
    }`, `{
    }`,
	}

	srcIndexPutResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexPutResps[i]), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.setIndexSettingsInternal(srcIndexName, params[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}

		// Check file
		logDir := "./log/" + time.Now().Format("20060102")
		defer os.RemoveAll(logDir)
		prefixPath := logDir + "/" + srcIndexName + ".settings." + time.Now().Format("20060102030405")
		beforePath := prefixPath + ".before"
		afterPath := prefixPath + ".after"

		readCnt, err := ReadWholeFile(beforePath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", beforePath, err)
		}

		if len(readCnt) != len(srcBeforeGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), beforePath, len(srcBeforeGetResps[i]))
		}
		if string(readCnt) != srcBeforeGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcBeforeGetResps[i])
		}

		readCnt, err = ReadWholeFile(afterPath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", afterPath, err)
		}

		if len(readCnt) != len(srcAfterGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), afterPath, len(srcAfterGetResps[i]))
		}
		if string(readCnt) != srcAfterGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcAfterGetResps[i])
		}
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexSettingsInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}

	srcBeforeGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
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
  "just_tests_15" : { "settings.index.number_of_shards" : "88" }
}`, `{
  "just_tests_18" : { "settings.index.number_of_shards" : "88" }
}`, `{
  "tests_21" : { "settings.index.number_of_shards" : "88" }
}`, `{
  "tests_22" : { "settings.index.number_of_shards" : "88" }
}`,
	}

	srcAfterGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88",
        "provided_name" : "just_tests_10",
        "creation_date" : "1609029285313",
        "number_of_replicas" : "5",
        "uuid" : "iX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
  "just_tests_15" : { "settings.index.number_of_shards" : "98" }
}`, `{
  "just_tests_18" : { "settings.index.number_of_shards" : "128" }
}`, `{
  "tests_21" : { "settings.index.number_of_shards" : "228" }
}`, `{
  "tests_22" : { "settings.index.number_of_shards" : "328" }
}`,
	}
	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "tests_21", "tests_22"}
	uri := "/_settings?pretty"
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uri)
	}

	param := "{\"index.routing.allocation.enable\": \"none\"}"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReqs[i]), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.setIndexSettingsInternal(srcIndexNames[i], param)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexSettingsInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcBeforeGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
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
  "just_tests_15" : { "settings.index.number_of_shards" : "88" }
}`, `{
  "just_tests_18" : { "settings.index.number_of_shards" : "88" }
}`, `{
  "tests_21" : { "settings.index.number_of_shards" : "88" }
}`, `{
  "tests_22" : { "settings.index.number_of_shards" : "88" }
}`,
	}

	srcAfterGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88",
        "provided_name" : "just_tests_10",
        "creation_date" : "1609029285313",
        "number_of_replicas" : "5",
        "uuid" : "iX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
  "just_tests_15" : { "settings.index.number_of_shards" : "98" }
}`, `{
  "just_tests_18" : { "settings.index.number_of_shards" : "128" }
}`, `{
  "tests_21" : { "settings.index.number_of_shards" : "228" }
}`, `{
  "tests_22" : { "settings.index.number_of_shards" : "328" }
}`,
	}
	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "tests_21", "tests_22"}
	uri := "/_settings?pretty"
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uri)
	}

	param := "{\"index.routing.allocation.enable\": \"none\"}"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReqs[i]), gomock.Eq(param)).Return(nil, errors[i])
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.setIndexSettingsInternal(srcIndexNames[i], param)
		if err == nil {
			t.Fatalf("Expect to be failed:%v, but err is nil", srcIndexReqs[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexSettingsInternal_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name or param is nil"}
	compositeOp := Create(nil)
	err := compositeOp.setIndexSettingsInternal("", "yy")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_SetIndexMappingInternal_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcBeforeGetResps := []string{` {
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
          }
        }
      }
    }
  }
}`, `{
  "tests_15" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "country" : {
            "type" : "keyword",
            "index" : false
          },
          "name" : {
            "type" : "keyword",
            "doc_values" : false
          }
        }
      }
    }
  }
}`, `{
}`,
	}

	srcAfterGetResps := []string{` {
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
          "age" : {
            "type" : "integer"
          }
        }
      }
    }
  }
}`, `{
  "tests_15" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "country" : {
            "type" : "keyword",
            "index" : false
          },
          "name" : {
            "type" : "keyword",
            "doc_values" : false
          },
          "hehe" : {
            "type" : "keyword",
            "index" : false,
            "doc_values" : false
          }
        }
      }
    }
  }
}`, `{
  "tests_18" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "country" : {
            "type" : "keyword",
            "index" : false
          }
        }
      }
    }
  }
}`,
	}

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18"}
	getUris := []string{"/_mapping?pretty", "/_mapping?pretty", "/_mapping?pretty"}
	putUris := []string{"/_mapping/_doc?pretty", "/_mapping/_doc?pretty", "/_mapping/_doc?pretty"}
	srcGetIndexReqs := make([]string, 0)
	srcPutIndexReqs := make([]string, 0)
	for i, srcIndexName := range srcIndexNames {
		srcGetIndexReqs = append(srcGetIndexReqs, srcIndexName+getUris[i])
		srcPutIndexReqs = append(srcPutIndexReqs, srcIndexName+putUris[i])
	}

	params := []string{`{
        "properties": {
          "age" : {
            "type" : "integer"
          }
        }
    }`, `{
        "properties": {
          "hehe" : {
            "type" : "keyword",
            "index" : false,
            "doc_values" : false,
          }
      }
    }`, `{
        "properties": {
          "country" : {
            "type" : "keyword",
            "index" : false
          }
      }
    }`,
	}

	srcIndexPutResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcPutIndexReqs[i]),
			gomock.Eq(params[i])).Return([]byte(srcIndexPutResps[i]), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.setIndexMappingsInternal(srcIndexName, params[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcPutIndexReqs[i], err)
		}

		// Check file
		logDir := "./log/" + time.Now().Format("20060102")
		defer os.RemoveAll(logDir)
		prefixPath := logDir + "/" + srcIndexName + ".mapping." + time.Now().Format("20060102030405")
		beforePath := prefixPath + ".before"
		afterPath := prefixPath + ".after"

		readCnt, err := ReadWholeFile(beforePath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", beforePath, err)
		}

		if len(readCnt) != len(srcBeforeGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), beforePath, len(srcBeforeGetResps[i]))
		}
		if string(readCnt) != srcBeforeGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcBeforeGetResps[i])
		}

		readCnt, err = ReadWholeFile(afterPath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", afterPath, err)
		}

		if len(readCnt) != len(srcAfterGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), afterPath, len(srcAfterGetResps[i]))
		}
		if string(readCnt) != srcAfterGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcAfterGetResps[i])
		}
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexMappingInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}

	srcBeforeGetResps := []string{` {
  "just_tests_10" : {
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
          }
        }
      }
    }
  }
}`, `{
  "just_tests_15" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`, `{
  "tests_21" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`, `{
  "tests_22" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`,
	}

	srcAfterGetResps := []string{` {
  "just_tests_10" : {
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
          "age" : {"type": "integer"}
        }
      }
    }
  }
}`, `{
  "just_tests_15" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"keyword"}
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"text"}
      }
    }
  }
}`, `{
  "tests_21" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"integer"}
      }
    }
  }
}`, `{
  "tests_22" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"long"}
      }
    }
  }
}`,
	}
	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "tests_21", "tests_22"}
	getUri := "/_mapping?pretty"
	putUri := "/_mapping/_doc?pretty"
	srcGetIndexReqs := make([]string, 0)
	srcPutIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcGetIndexReqs = append(srcGetIndexReqs, srcIndexName+getUri)
		srcPutIndexReqs = append(srcPutIndexReqs, srcIndexName+putUri)
	}

	param := `{
        "properties": {
          "age" : {
            "type" : "integer"
          }
        }}`

	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcPutIndexReqs[i]), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.setIndexMappingsInternal(srcIndexNames[i], param)
		if err == nil {
			t.Fatalf("Mock resp expect to be failed:%v, but err nil", errors[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not expect: %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexMappingsInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcBeforeGetResps := []string{` {
  "just_tests_10" : {
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
          }
        }
      }
    }
  }
}`, `{
  "just_tests_15" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`, `{
  "tests_21" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`, `{
  "tests_22" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"}
      }
    }
  }
}`,
	}

	srcAfterGetResps := []string{` {
  "just_tests_10" : {
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
          "age" : {"type": "integer"}
        }
      }
    }
  }
}`, `{
  "just_tests_15" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"keyword"}
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"text"}
      }
    }
  }
}`, `{
  "tests_21" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"integer"}
      }
    }
  }
}`, `{
  "tests_22" : {
    "mappings" : {
      "_doc" : {
          "name": {"type":"keyword"},
          "info": {"type":"long"}
      }
    }
  }
}`,
	}
	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "tests_21", "tests_22"}
	getUri := "/_mapping?pretty"
	putUri := "/_mapping/_doc?pretty"
	srcGetIndexReqs := make([]string, 0)
	srcPutIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcGetIndexReqs = append(srcGetIndexReqs, srcIndexName+getUri)
		srcPutIndexReqs = append(srcPutIndexReqs, srcIndexName+putUri)
	}

	param := `{
        "properties": {
          "age" : {
            "type" : "integer"
          }
        }}`

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
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcPutIndexReqs[i]), gomock.Eq(param)).Return(nil, errors[i])
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.setIndexMappingsInternal(srcIndexNames[i], param)
		if err == nil {
			t.Fatalf("Expect to be failed:%v, but err is nil", srcPutIndexReqs[i])
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndexMappingsInternal_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name or param is nil"}
	compositeOp := Create(nil)
	err := compositeOp.setIndexMappingsInternal("", "yy")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_CreateIndexInternal_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexNames := []string{"just_tests_10", "just_tests_15"}
	uri := "?pretty"
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uri)
	}
	params := []string{`{}`, `{}`}

	srcIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.createIndexInternal(srcIndexName)
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}
	}
} // }}}

func Test_CreateIndexInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	uri := "?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.createIndexInternal(srcIndexName)
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

func Test_CreateIndexInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexName := "just_tests_18"
	uri := "?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
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
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		err := compositeOp.createIndexInternal(srcIndexName)
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

func Test_GetRecoveryInfo_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryResps := []string{`{
  "just_tests_11": {
    "shards": [
      {
        "id": 2,
        "type": "PEER",
        "stage": "INDEX",
        "primary": true,
        "start_time_in_millis": 1623849678021,
        "total_time_in_millis": 790259,
        "source": {
          "id": "GHbHRBgjTomKe0MDdFeoIQ",
          "host": "xxx.xxx.xxx.xx",
          "transport_address": "xxx.xxx.xxx.xx:31111",
          "ip": "xxx.xxx.xxx.xx",
          "name": "Data_xxx.xxx.xxx.xx"
        },
        "target": {
          "id": "Hos25BbiQLq6-wXnV0MoAw",
          "host": "yyy.yyy.yyy.yy",
          "transport_address": "yyy.yyy.yyy.yy:40160",
          "ip": "yyy.yyy.yyy.yy",
          "name": "Data_yyy.yyy.yyy.yy"
        },
        "index": {
          "size": {
            "total_in_bytes": 34359750872,
            "reused_in_bytes": 0,
            "recovered_in_bytes": 16673608345,
            "percent": "48.5%"
          },
          "files": {
            "total": 458,
            "reused": 0,
            "recovered": 447,
            "percent": "97.6%"
          },
          "total_time_in_millis": 790122,
          "source_throttle_time_in_millis": 135702,
          "target_throttle_time_in_millis": 449688
        },
        "translog": {
          "recovered": 0,
          "total": 360,
          "percent": "0.0%",
          "total_on_start": 360,
          "total_time_in_millis": 0
        },
        "verify_index": {
          "check_index_time_in_millis": 0,
          "total_time_in_millis": 0
        }
      }
    ]
  }
}`, `{
  "just_tests_11": { }
}`, `{
}`,
	}

	for _, srcGetRecoveryResp := range srcGetRecoveryResps {
		var srcGetRecoveryRespMap map[string]interface{}
		err := json.Unmarshal([]byte(srcGetRecoveryResp), &srcGetRecoveryRespMap)
		if err != nil {
			t.Fatalf("Failed to json unmarshall:%v err:%v", srcGetRecoveryResp, err)
		}
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryResp), nil)

		compositeOp := Create(mockEsOp)
		respMap, respByte, err := compositeOp.GetRecoveryInfo()
		if err != nil {
			t.Fatalf("Failed to check %v, err:%v", srcGetRecoveryReq, err)
		}

		if respByte != srcGetRecoveryResp {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcGetRecoveryResp)
		}

		if len(respMap) != len(srcGetRecoveryRespMap) {
			t.Fatalf("Num:%v of RespMap not equal to mock resp:%v", len(respMap), len(srcGetRecoveryRespMap))
		}
		cmpDiff := cmp.Diff(respMap, srcGetRecoveryRespMap)
		if cmpDiff != "" {
			t.Fatalf("Value:%v not equal to src:%v, diff:%v", respMap, srcGetRecoveryRespMap, cmpDiff)
		}
	}
} // }}}

func Test_GetRecoveryInfo_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryResps := []string{`{
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

	for i, _ := range srcGetRecoveryResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetRecoveryInfo()
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

func Test_GetRecoveryInfo_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, _, err := compositeOp.GetRecoveryInfo()
		if err == nil {
			t.Fatalf("Expect getInfoInternal %v excute failed, but err is nil", srcGetRecoveryReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_SetIndiceAllocationOnAndOff_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndiceName := "just_tests_01"
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcBeforeSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	allParam := "{\"index.routing.allocation.enable\":\"all\"}"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetIndiceAllocationOnAndOff(srcCheckClusterName, srcIndiceName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndiceName, err)
	}
} // }}}

func Test_SetIndiceAllocationOnAndOff_Normal_AllocationExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndiceName := "just_tests_01"
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcBeforeSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	allParam := "{\"index.routing.allocation.enable\":\"all\"}"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetIndiceAllocationOnAndOff(srcCheckClusterName, srcIndiceName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndiceName, err)
	}
} // }}}

func Test_SetIndiceAllocationOnAndOff_Normal_AllocationAll(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndiceName := "just_tests_01"
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	// It's not used because allocaiton is all
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetIndiceAllocationOnAndOff(srcCheckClusterName, srcIndiceName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndiceName, err)
	}
} // }}}

func Test_SetIndiceAllocationOnAndOff_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name or waitSeconds is nil"}
	compositeOp := Create(nil)
	err := compositeOp.SetIndiceAllocationOnAndOff("aa", "", 1)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_SetIndiceAllocationOnAndOff_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	compositeOp := Create(mockEsOp)
	err := compositeOp.SetIndiceAllocationOnAndOff(srcCheckClusterName, srcIndiceName, 1)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_SetIndiceAllocationOnAndOff_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResps := []string{`{
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

	for i, _ := range srcCheckClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		err := compositeOp.SetIndiceAllocationOnAndOff(srcCheckClusterName, srcIndiceName, 1)
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

func Test_SetIndiceAllocationOnAndOff_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		err := compositeOp.SetIndiceAllocationOnAndOff(srcCheckClusterName, srcIndiceName, 1)
		if err == nil {
			t.Fatalf("Expect %v excute failed, but err is nil", srcCheckClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Normal_One_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndiceName := "just_tests_01"
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcBeforeSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	allParam := "{\"index.routing.allocation.enable\":\"all\"}"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	srcIndicesName := []string{srcIndiceName}
	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndicesName, err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Normal_Two_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndicesName := []string{"just_tests_01", "just_tests_02"}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceResp := `green  open ` + srcIndiceName + `  rVogrm3IR42MBLsPKRl_JQ  1 1  0    0  522b    261b`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		// GetIndexSetttingsOfKey
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcGetIndexSettingsResp := `{
            "` + srcIndiceName + `" : {
                "settings" : { "index" : { "number_of_shards" : "88" } } }  }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

		// setIndexSettingsInternal
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : { "index" : { "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`
		allParam := "{\"index.routing.allocation.enable\":\"all\"}"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)
	}

	// GetIndice
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		break // Just one request
	}

	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_02" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},  "just_tests_01": {},
  "just_tests_02": {} }`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryOneNotInResp := `{ "just_tests_11": {}, "just_tests_01": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryOneNotInResp), nil)

	srcGetRecoveryTwoNotInResp := `{ "just_tests_11": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryTwoNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : {
                    "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "none" } },
                        "number_of_shards" : "88" } } } }`
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)
	}

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndicesName, err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Normal_Three_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndicesName := []string{"just_tests_01", "just_tests_02", "just_tests_03"}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceResp := `green  open ` + srcIndiceName + `  rVogrm3IR42MBLsPKRl_JQ  1 1  0    0  522b    261b`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		// GetIndexSetttingsOfKey
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcGetIndexSettingsResp := `{
            "` + srcIndiceName + `" : {
                "settings" : { "index" : {  "number_of_shards" : "88"  } } } }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

		// setIndexSettingsInternal
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : { "index" : {  "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : {
                    "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88"
                    } } } }`
		allParam := "{\"index.routing.allocation.enable\":\"all\"}"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)
	}

	// GetIndice
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		break // Just one request
	}

	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_02" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_03" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},  "just_tests_01": {},
  "just_tests_02": {},  "just_tests_03": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryOneNotInResp := `{
  "just_tests_11": {},  "just_tests_01": {},
  "just_tests_02": {}
}`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryOneNotInResp), nil)

	srcGetRecoveryTwoNotInResp := `{
  "just_tests_11": {}, "just_tests_02": {}
}`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryTwoNotInResp), nil)

	srcGetRecoveryThreeNotInResp := `{
  "just_tests_11": {}
}`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryThreeNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : {
                    "index" : {
                        "routing" : { "allocation" : {  "enable" : "all" } },
                        "number_of_shards" : "88"
                    } } } }`
		srcAfterSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : {
                    "index" : {
                        "routing" : { "allocation" : {  "enable" : "none" } },
                        "number_of_shards" : "88"
                    } } } }`
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)
	}

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndicesName, err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Normal_Four_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green", "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndicesName := []string{"just_tests_01", "just_tests_02", "just_tests_03", "just_tests_04"}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceResp := `green  open ` + srcIndiceName + `  rVogrm3IR42MBLsPKRl_JQ  1 1  0    0  522b    261b`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		// GetIndexSetttingsOfKey
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcGetIndexSettingsResp := `{
            "` + srcIndiceName + `" : {  "settings" : { "index" : { "number_of_shards" : "88" } } } }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

		// setIndexSettingsInternal
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : { "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all"  } },
                        "number_of_shards" : "88" } } } }`
		allParam := "{\"index.routing.allocation.enable\":\"all\"}"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)
	}

	// GetIndice
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		break // Just one request
	}

	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_02" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_03" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_04" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},  "just_tests_01": {},  "just_tests_02": {},  "just_tests_03": {},  "just_tests_04": {} }`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryOneNotInResp := `{
  "just_tests_11": {},  "just_tests_01": {},  "just_tests_02": {},  "just_tests_03": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryOneNotInResp), nil)

	srcGetRecoveryTwoNotInResp := `{ "just_tests_11": {},  "just_tests_01": {},  "just_tests_02": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryTwoNotInResp), nil)

	srcGetRecoveryThreeNotInResp := `{ "just_tests_11": {},  "just_tests_01": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryThreeNotInResp), nil)

	srcGetRecoveryFourNotInResp := `{ "just_tests_11": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryFourNotInResp), nil)
	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : {
                "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "none" } },
                        "number_of_shards" : "88" } } } }`
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)
	}

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndicesName, err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Normal_Four_AllocationExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{ "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",  "number_of_nodes" : 6 }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndicesName := []string{"just_tests_01", "just_tests_02", "just_tests_03", "just_tests_04"}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceResp := `green  open ` + srcIndiceName + `  rVogrm3IR42MBLsPKRl_JQ  1 1  0    0  522b    261b`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		// GetIndexSetttingsOfKey
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcGetIndexSettingsResp := `{
            "` + srcIndiceName + `" : { "settings" : { "index" : { 
                        "routing" : { "allocation" : { "enable" : "none" } },
                        "number_of_shards" : "88" } } } }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

		// setIndexSettingsInternal
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "none" } },
                        "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`
		allParam := "{\"index.routing.allocation.enable\":\"all\"}"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)
	}

	// GetIndice
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		break // Just one request
	}

	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_02" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_03" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_04" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {}, "just_tests_01": {}, "just_tests_02": {}, "just_tests_03": {}, "just_tests_04": {} }`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryOneNotInResp := `{
  "just_tests_11": {}, "just_tests_01": {}, "just_tests_02": {}, "just_tests_03": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryOneNotInResp), nil)

	srcGetRecoveryTwoNotInResp := `{ "just_tests_11": {}, "just_tests_01": {}, "just_tests_02": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryTwoNotInResp), nil)

	srcGetRecoveryThreeNotInResp := `{ "just_tests_11": {}, "just_tests_01": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryThreeNotInResp), nil)

	srcGetRecoveryFourNotInResp := `{ "just_tests_11": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryFourNotInResp), nil)
	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "none" } },
                        "number_of_shards" : "88" } } } }`
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)
	}

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndicesName, err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Normal_Four_AllocationAll(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{ "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green", "number_of_nodes" : 6 }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcIndicesName := []string{"just_tests_01", "just_tests_02", "just_tests_03", "just_tests_04"}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceResp := `green  open ` + srcIndiceName + `  rVogrm3IR42MBLsPKRl_JQ  1 1  0    0  522b    261b`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		// GetIndexSetttingsOfKey
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcGetIndexSettingsResp := `{
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)
	}

	// GetIndice
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		break // Just one request
	}

	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_02" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_03" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		if srcIndiceName == "just_tests_04" {
			srcGetIndiceYellowResp := `yellow  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
			break
		} else {
			srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
			mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
		}
	}
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{ "just_tests_11": {}, "just_tests_01": {}, "just_tests_02": {},
  "just_tests_03": {},  "just_tests_04": {} }`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryOneNotInResp := `{ "just_tests_11": {}, "just_tests_01": {}, "just_tests_02": {},
  "just_tests_03": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryOneNotInResp), nil)

	srcGetRecoveryTwoNotInResp := `{ "just_tests_11": {}, "just_tests_01": {}, "just_tests_02": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryTwoNotInResp), nil)

	srcGetRecoveryThreeNotInResp := `{ "just_tests_11": {}, "just_tests_01": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryThreeNotInResp), nil)

	srcGetRecoveryFourNotInResp := `{ "just_tests_11": {} }`
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
		srcGetIndiceYellowResp := `green  open ` + srcIndiceName + ` rVogrm3IR42MBLsPKRl_JQ  1 1  0 0 522b  261b`
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)
	}
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryFourNotInResp), nil)
	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	for _, srcIndiceName := range srcIndicesName {
		srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcBeforeSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : { "index" : {
                        "routing" : { "allocation" : { "enable" : "all" } },
                        "number_of_shards" : "88" } } } }`
		srcAfterSetSettingsToNoneGetResp := ` {
            "` + srcIndiceName + `" : { "settings" : {  "index" : {
                        "routing" : { "allocation" : { "enable" : "none" } },
                        "number_of_shards" : "88" } } } }`
		srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
		srcSetSettingsPutResp := `{ "acknowledged" : true }`

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)
	}

	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndicesName, err)
	}
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Exception_EmptyIndicesName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or indices name or waitSeconds is nil"}
	compositeOp := Create(nil)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff("aa", []string{}, 1)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func testClusterNameNotFound(resp string, t *testing.T) {// {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(resp), nil)

	srcIndicesName := []string{"just_tests_01"}
	compositeOp := Create(mockEsOp)
	err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
}// }}}

func Test_SetBatchIndiceAllocationOnAndOff_Exception_ClusetrNotExist(t *testing.T) { // {{{
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green",
  "number_of_nodes" : 6
}`

    testClusterNameNotFound(srcCheckClusterResp, t)
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Exception_NoClusterName(t *testing.T) { // {{{
	srcCheckClusterResp := `{
  "status" : "green",
  "number_of_nodes" : 6
}`
    testClusterNameNotFound(srcCheckClusterResp, t)
} // }}}

func Test_SetBatchIndiceAllocationOnAndOff_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResps := []string{`{
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

	for i, _ := range srcCheckClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		srcIndicesName := []string{"just_tests_01"}
		err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
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

func Test_SetBatchIndiceAllocationOnAndOff_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		srcIndicesName := []string{"just_tests_01"}
		err := compositeOp.SetBatchIndiceAllocationOnAndOff(srcCheckClusterName, srcIndicesName, 1)
		if err == nil {
			t.Fatalf("Expect %v excute failed, but err is nil", srcCheckClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_CreateIndice_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// createIndexInternal
	srcIndiceName := "just_tests_01"
	srcCreateIndexReq := srcIndiceName + "?pretty"
	srcCreateIndexResp := `{ "acknowledged" : true }`
	createParam := "{}"

	mockEsOp.EXPECT().Put(gomock.Eq(srcCreateIndexReq), gomock.Eq(createParam)).Return([]byte(srcCreateIndexResp), nil)

	// Cluster check
	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcBeforeSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	allParam := "{\"index.routing.allocation.enable\":\"all\"}"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	compositeOp := Create(mockEsOp)
	err := compositeOp.CreateIndice(srcCheckClusterName, srcIndiceName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndiceName, err)
	}
} // }}}

func Test_CreateIndice_Normal_AllocationExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// createIndexInternal
	srcIndiceName := "just_tests_01"
	srcCreateIndexReq := srcIndiceName + "?pretty"
	srcCreateIndexResp := `{ "acknowledged" : true }`
	createParam := "{}"

	mockEsOp.EXPECT().Put(gomock.Eq(srcCreateIndexReq), gomock.Eq(createParam)).Return([]byte(srcCreateIndexResp), nil)

	// Cluster check
	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcBeforeSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	allParam := "{\"index.routing.allocation.enable\":\"all\"}"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(allParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsGetResp), nil)

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	compositeOp := Create(mockEsOp)
	err := compositeOp.CreateIndice(srcCheckClusterName, srcIndiceName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndiceName, err)
	}
} // }}}

func Test_CreateIndice_Normal_AllocationAll(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// createIndexInternal
	srcIndiceName := "just_tests_01"
	srcCreateIndexReq := srcIndiceName + "?pretty"
	srcCreateIndexResp := `{ "acknowledged" : true }`
	createParam := "{}"

	mockEsOp.EXPECT().Put(gomock.Eq(srcCreateIndexReq), gomock.Eq(createParam)).Return([]byte(srcCreateIndexResp), nil)

	// Cluster check
	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	// GetIndice
	srcGetIndiceReq := "_cat/indices/" + srcIndiceName + "?pretty"
	srcGetIndiceResp := `green  open  just_tests_01       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

	// GetIndexSetttingsOfKey
	srcGetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcGetIndexSettingsResp := `{
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcGetIndexSettingsResp), nil)

	// setIndexSettingsInternal
	// It's not used because allocaiton is all
	srcSetIndexSettingsReq := srcIndiceName + "/_settings?pretty"
	srcSetSettingsPutResp := `{ "acknowledged" : true }`

	// GetIndice
	srcGetIndiceYellowResp := `yellow  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceYellowResp), nil)

	srcGetIndiceGreenResp := `green  open  just_tests_01   rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)

	// GetRecoveryInfo
	srcGetRecoveryReq := "_recovery?active_only=true&pretty"
	srcGetRecoveryInResp := `{
  "just_tests_11": {},
  "just_tests_01": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryInResp), nil)

	srcGetRecoveryNotInResp := `{
  "just_tests_11": {}
}`
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceGreenResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetRecoveryReq)).Return([]byte(srcGetRecoveryNotInResp), nil)

	// setIndexSettingsInternal
	noneParam := "{\"index.routing.allocation.enable\":\"none\"}"
	srcBeforeSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`
	srcAfterSetSettingsToNoneGetResp := ` {
  "just_tests_01" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "none"
          }
        },
        "number_of_shards" : "88"
      }
    }
  }
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcBeforeSetSettingsToNoneGetResp), nil)
	mockEsOp.EXPECT().Put(gomock.Eq(srcSetIndexSettingsReq), gomock.Eq(noneParam)).Return([]byte(srcSetSettingsPutResp), nil)
	mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexSettingsReq)).Return([]byte(srcAfterSetSettingsToNoneGetResp), nil)

	compositeOp := Create(mockEsOp)
	err := compositeOp.CreateIndice(srcCheckClusterName, srcIndiceName, 1)
	if err != nil {
		t.Fatalf("Failed to set allocation on and off of %v, %v, err:%v", srcCheckClusterName, srcIndiceName, err)
	}
} // }}}

func Test_CreateIndice_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name or waitSeconds is nil"}
	compositeOp := Create(nil)
	err := compositeOp.CreateIndice("", "yy", 1)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_CreateIndice_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	compositeOp := Create(mockEsOp)
	err := compositeOp.CreateIndice(srcCheckClusterName, srcIndiceName, 1)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_CreateIndice_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResps := []string{`{
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

	for i, _ := range srcCheckClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		err := compositeOp.CreateIndice(srcCheckClusterName, srcIndiceName, 1)
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

func Test_CreateIndice_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		err := compositeOp.CreateIndice(srcCheckClusterName, srcIndiceName, 1)
		if err == nil {
			t.Fatalf("Expect %v excute failed, but err is nil", srcCheckClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_SetIndiceSettings_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// GetIndice
	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18"}
	uri := "/_settings?pretty"
	srcGetIndiceReqs := make([]string, 0)
	srcIndexSettingsReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexSettingsReqs = append(srcIndexSettingsReqs, srcIndexName+uri)
		srcGetIndiceReqs = append(srcGetIndiceReqs, "_cat/indices/"+srcIndexName+"?pretty")
	}

	srcGetIndiceResps := []string{
		`green  open  just_tests_10       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`,
		`green  open  just_tests_15       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`,
		`green  open  just_tests_18       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`,
	}

	// setIndexSettingsInternal
	srcBeforeGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
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
        "number_of_shards" : "88",
        "provided_name" : "just_tests_15",
        "creation_date" : "1609029784313",
        "number_of_replicas" : "2",
        "uuid" : "MX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
}`,
	}

	srcAfterGetResps := []string{` {
  "just_tests_10" : {
    "settings" : {
      "index" : {
        "routing" : {
          "allocation" : {
            "enable" : "all"
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
        "number_of_shards" : "88",
        "provided_name" : "just_tests_15",
        "creation_date" : "1609029784313",
        "number_of_replicas" : "5",
        "uuid" : "MX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`, `{
  "just_tests_18" : {
    "settings" : {
      "index" : {
        "number_of_shards" : "88",
        "provided_name" : "just_tests_18",
        "creation_date" : "1679029784313",
        "number_of_replicas" : "50",
        "uuid" : "JX5jxasdYQvidcOasdDPdblQ",
        "version" : {
          "created" : "6030999"
        }
      }
    }
  }
}`,
	}

	params := []string{`{
        "index.routing.allocation.enable": "none"
    }`, `{
        "index.number_of_replicas":"5"
    }`, `{
        "index.number_of_replicas":"50"
    }`, `{
    }`,
	}

	srcIndexPutResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReqs[i])).Return([]byte(srcGetIndiceResps[i]), nil)

		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexSettingsReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcIndexSettingsReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexPutResps[i]), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcIndexSettingsReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.SetIndiceSettings(srcCheckClusterName, srcIndexName, params[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexSettingsReqs[i], err)
		}

		// Check file
		logDir := "./log/" + time.Now().Format("20060102")
		defer os.RemoveAll(logDir)
		prefixPath := logDir + "/" + srcIndexName + ".settings." + time.Now().Format("20060102030405")
		beforePath := prefixPath + ".before"
		afterPath := prefixPath + ".after"

		readCnt, err := ReadWholeFile(beforePath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", beforePath, err)
		}

		if len(readCnt) != len(srcBeforeGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), beforePath, len(srcBeforeGetResps[i]))
		}
		if string(readCnt) != srcBeforeGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcBeforeGetResps[i])
		}

		readCnt, err = ReadWholeFile(afterPath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", afterPath, err)
		}

		if len(readCnt) != len(srcAfterGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), afterPath, len(srcAfterGetResps[i]))
		}
		if string(readCnt) != srcAfterGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcAfterGetResps[i])
		}
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndiceSettings_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green",
  "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	settingParam := `{"index.routing.allocation.enable": "none"}`
	compositeOp := Create(mockEsOp)
	err := compositeOp.SetIndiceSettings(srcCheckClusterName, srcIndiceName, settingParam)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_SetIndiceSettings_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name or settings is nil"}
	compositeOp := Create(nil)
	err := compositeOp.SetIndiceSettings("aa", "", "bb")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_SetIndiceSettings_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResps := []string{`{
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

	for i, _ := range srcCheckClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		settingParam := `{"index.routing.allocation.enable": "none"}`
		err := compositeOp.SetIndiceSettings(srcCheckClusterName, srcIndiceName, settingParam)
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

func Test_SetIndiceSettings_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		settingParam := `{"index.routing.allocation.enable": "none"}`
		err := compositeOp.SetIndiceSettings(srcCheckClusterName, srcIndiceName, settingParam)
		if err == nil {
			t.Fatalf("Expect %v excute failed, but err is nil", srcCheckClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_SetIndiceMapping_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// GetIndice
	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18"}
	getUris := []string{"/_mapping?pretty", "/_mapping?pretty", "/_mapping?pretty"}
	putUris := []string{"/_mapping/_doc?pretty", "/_mapping/_doc?pretty", "/_mapping/_doc?pretty"}
	srcGetIndiceReqs := make([]string, 0)
	srcGetIndexMappingReqs := make([]string, 0)
	srcPutIndexMappingReqs := make([]string, 0)
	for i, srcIndexName := range srcIndexNames {
		srcGetIndexMappingReqs = append(srcGetIndexMappingReqs, srcIndexName+getUris[i])
		srcPutIndexMappingReqs = append(srcPutIndexMappingReqs, srcIndexName+putUris[i])

		srcGetIndiceReqs = append(srcGetIndiceReqs, "_cat/indices/"+srcIndexName+"?pretty")
	}

	srcGetIndiceResps := []string{
		`green  open  just_tests_10       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`,
		`green  open  just_tests_15       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`,
		`green  open  just_tests_18       rVogrm3IR42MBLsPKRl_JQ  1 1    0    0    522b    261b`,
	}

	// setIndexMappingsInternal
	srcBeforeGetResps := []string{` {
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
          }
        }
      }
    }
  }
}`, `{
  "tests_15" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "country" : {
            "type" : "keyword",
            "index" : false
          },
          "name" : {
            "type" : "keyword",
            "doc_values" : false
          }
        }
      }
    }
  }
}`, `{
}`,
	}

	srcAfterGetResps := []string{` {
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
          "age" : {
            "type" : "integer"
          }
        }
      }
    }
  }
}`, `{
  "tests_15" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "country" : {
            "type" : "keyword",
            "index" : false
          },
          "name" : {
            "type" : "keyword",
            "doc_values" : false
          },
          "hehe" : {
            "type" : "keyword",
            "index" : false,
            "doc_values" : false
          }
        }
      }
    }
  }
}`, `{
  "tests_18" : {
    "mappings" : {
      "_doc" : {
        "dynamic" : "strict",
        "properties" : {
          "country" : {
            "type" : "keyword",
            "index" : false
          }
        }
      }
    }
  }
}`,
	}

	params := []string{`{
        "properties": {
          "age" : {
            "type" : "integer"
          }
        }
    }`, `{
        "properties": {
          "hehe" : {
            "type" : "keyword",
            "index" : false,
            "doc_values" : false,
          }
      }
    }`, `{
        "properties": {
          "country" : {
            "type" : "keyword",
            "index" : false
          }
      }
    }`,
	}

	srcIndexPutResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReqs[i])).Return([]byte(srcGetIndiceResps[i]), nil)

		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexMappingReqs[i])).Return([]byte(srcBeforeGetResps[i]), nil)
		mockEsOp.EXPECT().Put(gomock.Eq(srcPutIndexMappingReqs[i]),
			gomock.Eq(params[i])).Return([]byte(srcIndexPutResps[i]), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndexMappingReqs[i])).Return([]byte(srcAfterGetResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.SetIndiceMapping(srcCheckClusterName, srcIndexName, params[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcPutIndexMappingReqs[i], err)
		}

		// Check file
		logDir := "./log/" + time.Now().Format("20060102")
		defer os.RemoveAll(logDir)
		prefixPath := logDir + "/" + srcIndexName + ".mapping." + time.Now().Format("20060102030405")
		beforePath := prefixPath + ".before"
		afterPath := prefixPath + ".after"

		readCnt, err := ReadWholeFile(beforePath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", beforePath, err)
		}

		if len(readCnt) != len(srcBeforeGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), beforePath, len(srcBeforeGetResps[i]))
		}
		if string(readCnt) != srcBeforeGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcBeforeGetResps[i])
		}

		readCnt, err = ReadWholeFile(afterPath)
		if err != nil {
			t.Fatalf("Failed to read tmp file:%v, err:%v", afterPath, err)
		}

		if len(readCnt) != len(srcAfterGetResps[i]) {
			t.Fatalf("get num %v from %v not equal to %v", len(readCnt), afterPath, len(srcAfterGetResps[i]))
		}
		if string(readCnt) != srcAfterGetResps[i] {
			t.Fatalf("value:%v not equal to src content:%v", string(readCnt), srcAfterGetResps[i])
		}
	}

	time.Sleep(1 * time.Second)
} // }}}

func Test_SetIndiceMapping_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green", "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	mappingParam := `{"properties":{"age":{"type":"integer" } } }`
	compositeOp := Create(mockEsOp)
	err := compositeOp.SetIndiceMapping(srcCheckClusterName, srcIndiceName, mappingParam)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_SetIndiceMapping_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name or mappings is nil"}
	compositeOp := Create(nil)
	err := compositeOp.SetIndiceMapping("aa", "cc", "")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_SetIndiceMapping_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResps := []string{`{
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

	for i, _ := range srcCheckClusterResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResps[i]), nil)

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		mappingParam := `{"properties":{"age":{"type":"integer"} } }`
		err := compositeOp.SetIndiceMapping(srcCheckClusterName, srcIndiceName, mappingParam)
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

func Test_SetIndiceMapping_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		srcIndiceName := "just_tests_01"
		mappingParam := `{"properties":{"age":{"type":"integer"} } }`
		err := compositeOp.SetIndiceMapping(srcCheckClusterName, srcIndiceName, mappingParam)
		if err == nil {
			t.Fatalf("Expect %v excute failed, but err is nil", srcCheckClusterReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_PostIndexInternal_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "just_tests_19"}
	uris := []string{"/_close?pretty", "/_close?pretty", "/_open/_doc?pretty", "/_open/_doc?pretty"}
	srcIndexReqs := make([]string, 0)
	for i, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uris[i])
	}

	params := []string{`{}`, `{}`, `{}`, `{}`}

	srcIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true,
      "shards_acknowledged" : true
}`, `{
      "acknowledged" : true,
      "shards_acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respByte, err := compositeOp.postIndexInternal(srcIndexName, uris[i], params[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}

		if respByte != srcIndexResps[i] {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcIndexResps[i])
		}
	}
} // }}}

func Test_PostIndexInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	uri := "/_close?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.postIndexInternal(srcIndexName, uri, param)
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

func Test_PostIndexInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexName := "just_tests_18"
	uri := "/_open?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
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
		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.postIndexInternal(srcIndexName, uri, param)
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

func Test_PostIndexInternal_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name or uri or param is nil"}
	compositeOp := Create(nil)
	_, err := compositeOp.postIndexInternal("", "xx", "yy")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_CloseIndice_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "just_tests_19"}
	uri := "/_close?pretty"
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uri)
	}

	params := []string{`{}`, `{}`, `{}`, `{}`}

	srcIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true,
      "shards_acknowledged" : true
}`, `{
      "acknowledged" : true,
      "shards_acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.CloseIndice(srcCheckClusterName, srcIndexName)
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}
	}
} // }}}

func Test_CloseIndice_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green", "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	compositeOp := Create(mockEsOp)
	err := compositeOp.CloseIndice(srcCheckClusterName, srcIndiceName)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_CloseIndice_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	uri := "/_close?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.CloseIndice(srcCheckClusterName, srcIndexName)
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

func Test_CloseIndice_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// postIndexInternal
	srcIndexName := "just_tests_18"
	uri := "/_close?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		err := compositeOp.CloseIndice(srcCheckClusterName, srcIndexName)
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

func Test_CloseIndice_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name is nil"}
	compositeOp := Create(nil)
	err := compositeOp.CloseIndice("", "xx")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_OpenIndice_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	srcIndexNames := []string{"just_tests_10", "just_tests_15", "just_tests_18", "just_tests_19"}
	uri := "/_open?pretty"
	srcIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uri)
	}

	params := []string{`{}`, `{}`, `{}`, `{}`}

	srcIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`, `{
      "acknowledged" : true,
      "shards_acknowledged" : true
}`, `{
      "acknowledged" : true,
      "shards_acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReqs[i]), gomock.Eq(params[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.OpenIndice(srcCheckClusterName, srcIndexName)
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}
	}
} // }}}

func Test_OpenIndice_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green", "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	compositeOp := Create(mockEsOp)
	err := compositeOp.OpenIndice(srcCheckClusterName, srcIndiceName)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_OpenIndice_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	uri := "/_open?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.OpenIndice(srcCheckClusterName, srcIndexName)
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

func Test_OpenIndice_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// postIndexInternal
	srcIndexName := "just_tests_18"
	uri := "/_open?pretty"
	srcIndexReq := srcIndexName + uri
	param := "{}"
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Post(gomock.Eq(srcIndexReq), gomock.Eq(param)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		err := compositeOp.OpenIndice(srcCheckClusterName, srcIndexName)
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

func Test_OpenIndice_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name is nil"}
	compositeOp := Create(nil)
	err := compositeOp.OpenIndice("mm", "")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_DeleteIndexInternal_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexNames := []string{"just_tests_10", "just_tests_15"}
	uris := []string{"?expand_wildcards=closed&pretty", "?pretty"}
	srcIndexReqs := make([]string, 0)
	for i, srcIndexName := range srcIndexNames {
		srcIndexReqs = append(srcIndexReqs, srcIndexName+uris[i])
	}

	srcIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Delete(gomock.Eq(srcIndexReqs[i])).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		respByte, err := compositeOp.deleteIndexInternal(srcIndexName, uris[i])
		if err != nil {
			t.Fatalf("Failed to set %v, err:%v", srcIndexReqs[i], err)
		}

		if respByte != srcIndexResps[i] {
			t.Fatalf("RespByte:%v not equal to mock resp:%v", respByte, srcIndexResps[i])
		}
	}
} // }}}

func Test_DeleteIndexInternal_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	srcIndexName := "just_tests_18"
	uri := "?expand_wildcards=closed&pretty"
	srcIndexReq := srcIndexName + uri
	srcIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Delete(gomock.Eq(srcIndexReq)).Return([]byte(srcIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.deleteIndexInternal(srcIndexName, uri)
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

func Test_DeleteIndexInternal_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcIndexName := "just_tests_18"
	uri := "?expand_wildcards=closed&pretty"
	srcIndexReq := srcIndexName + uri
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
		mockEsOp.EXPECT().Delete(gomock.Eq(srcIndexReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		_, err := compositeOp.deleteIndexInternal(srcIndexName, uri)
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

func Test_DeleteIndexInternal_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "index name or uri is nil"}
	compositeOp := Create(nil)
	_, err := compositeOp.deleteIndexInternal("", "xx")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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

func Test_DeleteCloseIndice_Normal_Set(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// GetIndice
	srcIndexNames := []string{"just_tests_10", "just_tests_15"}
	srcGetIndiceReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcGetIndiceReqs = append(srcGetIndiceReqs, "_cat/indices/"+srcIndexName+"?pretty")
	}

	srcGetIndiceResps := []string{
		`close just_tests_10               VInnpfgbQU-oYVMItaliaw`,
		`close just_tests_15               VInnpfgbQU-oYVMItaliaw`,
	}

	// deleteIndexInternal
	uri := "?expand_wildcards=closed&pretty"
	srcDeleteIndexReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcDeleteIndexReqs = append(srcDeleteIndexReqs, srcIndexName+uri)
	}

	srcDeleteIndexResps := []string{`{
      "acknowledged" : true
}`, `{
      "acknowledged" : true
}`,
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReqs[i])).Return([]byte(srcGetIndiceResps[i]), nil)

		mockEsOp.EXPECT().Delete(gomock.Eq(srcDeleteIndexReqs[i])).Return([]byte(srcDeleteIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.DeleteClosedIndice(srcCheckClusterName, srcIndexName)
		if err != nil {
			t.Fatalf("Failed to delete %v, err:%v", srcDeleteIndexReqs[i], err)
		}
	}
} // }}}

func Test_DeleteCloseIndice_Exception_ClusetrNotExist(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEsOp := NewMockBaseEsOp(ctrl)
	expectErr := Error{ErrNotFound, "Not found cluster"}

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_Two",
  "status" : "green", "number_of_nodes" : 6
}`

	mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)

	srcIndiceName := "just_tests_01"
	compositeOp := Create(mockEsOp)
	err := compositeOp.DeleteClosedIndice(srcCheckClusterName, srcIndiceName)
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
	}
	code, _ := DecodeErr(err)
	if code != expectErr.Code {
		t.Fatalf("err code:%v is not %v", code, expectErr.Code)
	}

	t.Logf("Exception Test! err:%v", err)
} // }}}

func Test_DeleteCloseIndice_Exception_IndiceNotClose(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// GetIndice
	srcIndexNames := []string{"just_tests_10", "just_tests_15"}
	srcGetIndiceReqs := make([]string, 0)
	for _, srcIndexName := range srcIndexNames {
		srcGetIndiceReqs = append(srcGetIndiceReqs, "_cat/indices/"+srcIndexName+"?pretty")
	}

	srcGetIndiceResps := []string{
		`green  open  just_tests_10               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb`,
		`yellow  open  just_tests_15               G7S28w0dS7qJ8yLTYsI7QA  1 1      3    0  20.8kb  10.4kb`,
	}

	errors := []Error{
		{ErrNotClosed, "Not closed of"},
		{ErrNotClosed, "Not closed of"},
	}

	for i, srcIndexName := range srcIndexNames {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReqs[i])).Return([]byte(srcGetIndiceResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.DeleteClosedIndice(srcCheckClusterName, srcIndexName)
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

func Test_DeleteCloseIndice_Exception_ResponseErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// GetIndice
	srcIndexName := "just_tests_10"
	srcGetIndiceReq := "_cat/indices/" + srcIndexName + "?pretty"
	srcGetIndiceResp := "close just_tests_10               VInnpfgbQU-oYVMItaliaw"

	errors := []Error{
		{ErrJsonUnmarshalFailed, "ReadMapCB: expect :"},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
		{ErrRespErr, "Resp error: "},
	}
	uri := "?expand_wildcards=closed&pretty"
	srcDeleteIndexReq := srcIndexName + uri
	srcDeleteIndexResps := []string{`{
        "xxxyyyyjjmm"
}`, `{
      "error" : "Incorrect HTTP method for uri [/just_test_18/ssettingss?pretty] and method [GET], allowed: [POST]",
      "status" : 405
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/settings/update]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "Can't update non dynamic settings [[index.number_of_shards]] for open indices [[tests_3/o7t]]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "remote_transport_exception",
            "reason" : "[Master_1][localhost:9999][indices:admin/mapping/put]"
        }
        ],
        "type" : "illegal_argument_exception",
        "reason" : "mapper [status] of different type, current_type [long], merged_type [keyword]"
    },
    "status" : 400
}`, `{
    "error" : {
        "root_cause" : [
        {
            "type" : "mapper_parsing_exception",
            "reason" : "No handler for type [aaa] declared on field [status]"
        }
        ],
        "type" : "mapper_parsing_exception",
        "reason" : "No handler for type [aaa] declared on field [status]"
    },
    "status" : 400
}`,
	}

	for i, _ := range srcDeleteIndexResps {
		mockEsOp := NewMockBaseEsOp(ctrl)
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		mockEsOp.EXPECT().Delete(gomock.Eq(srcDeleteIndexReq)).Return([]byte(srcDeleteIndexResps[i]), nil)

		compositeOp := Create(mockEsOp)
		err := compositeOp.DeleteClosedIndice(srcCheckClusterName, srcIndexName)
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

func Test_DeleteCloseIndice_Exception_OtherErr(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Cluster check
	srcCheckClusterName := "HaveTryTwo_First_One"
	srcCheckClusterReq := "_cluster/health?pretty"
	srcCheckClusterResp := `{
  "cluster_name" : "HaveTryTwo_First_One",
  "status" : "green",
  "number_of_nodes" : 6
}`

	// GetIndice
	srcIndexName := "just_tests_10"
	srcGetIndiceReq := "_cat/indices/" + srcIndexName + "?pretty"
	srcGetIndiceResp := "close just_tests_10               VInnpfgbQU-oYVMItaliaw"

	uri := "?expand_wildcards=closed&pretty"
	srcDeleteIndexReq := srcIndexName + uri
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
		mockEsOp.EXPECT().Get(gomock.Eq(srcCheckClusterReq)).Return([]byte(srcCheckClusterResp), nil)
		mockEsOp.EXPECT().Get(gomock.Eq(srcGetIndiceReq)).Return([]byte(srcGetIndiceResp), nil)

		mockEsOp.EXPECT().Delete(gomock.Eq(srcDeleteIndexReq)).Return(nil, errors[i])

		compositeOp := Create(mockEsOp)
		err := compositeOp.DeleteClosedIndice(srcCheckClusterName, srcIndexName)
		if err == nil {
			t.Fatalf("Expect to be failed:%v, but err is nil", srcDeleteIndexReq)
		}

		code, _ := DecodeErr(err)
		if code != errors[i].Code {
			t.Fatalf("err code:%v is not %v", code, errors[i].Code)
		}

		t.Logf("Exception Test! err:%v", err)
	}
} // }}}

func Test_DeleteCloseIndice_Exception_EmptyIndexName(t *testing.T) { // {{{
	expectErr := Error{ErrInvalidParam, "cluster name or index name is nil"}
	compositeOp := Create(nil)
	err := compositeOp.DeleteClosedIndice("yy", "")
	if err == nil {
		t.Fatalf("Expect to be failed:%v, but err is nil", expectErr)
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
