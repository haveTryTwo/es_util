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
	json "github.com/json-iterator/go"
	gomock "github.com/golang/mock/gomock"
    "github.com/google/go-cmp/cmp"
	"testing"
)

func Test_GetClusterHealth_Normal_Get(t *testing.T) { // {{{
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	srcClusterReq := "_cluster/health?pretty"
	srcClusterResp := `{
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
}`

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
