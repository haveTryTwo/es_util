// Package basetool implements a tool of es
package basetool

import (
	//	"bufio"
	//	"fmt"
	//	"io/ioutil"
	//	"strings"
	"github.com/google/go-cmp/cmp"
	"testing"
)

func testSortStringArr(arr []string, checkArr []string, direction int, t *testing.T) { // {{{
	err := SortStringArr(arr, direction)
	if err != nil {
		t.Fatalf("Failed to sort string arr:%v, err:%v", arr, err)
	}
	if len(arr) != len(checkArr) {
		t.Fatalf("Not equal of len(arr):%v to len(dest):%v", len(arr), len(checkArr))
	}
	tmpDiff := cmp.Diff(arr, checkArr)
	if tmpDiff != "" {
		t.Fatalf("Not equal of arr:%v to dest:%v, diff:%v", arr, checkArr, tmpDiff)
	}
} // }}}

func Test_SortStringArr_Normal_Positive(t *testing.T) { // {{{
	arr := []string{"aa", "bb", "cc", "dd", "ee", "ff"}
	positiveDstArr := []string{"aa", "bb", "cc", "dd", "ee", "ff"}
    testSortStringArr(arr, positiveDstArr, Positive, t)

	arr = []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg"}
	positiveDstArr = []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg"}
    testSortStringArr(arr, positiveDstArr, Positive, t)
} // }}}

func Test_SortStringArr_Normal_Reverse(t *testing.T) { // {{{

	arr := []string{"aa", "bb", "cc", "dd", "ee", "ff"}
	positiveDstArr := []string{"ff", "ee", "dd", "cc", "bb", "aa"}
    testSortStringArr(arr, positiveDstArr, Reverse, t)

	arr = []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg"}
	positiveDstArr = []string{"gg", "ff", "ee", "dd", "cc", "bb", "aa"}
    testSortStringArr(arr, positiveDstArr, Reverse, t)
} // }}}

func Test_SortStringArr_Normal_BiDirectional(t *testing.T) { // {{{

	arr := []string{"aa", "bb", "cc", "dd", "ee", "ff"}
	positiveDstArr := []string{"aa", "ff", "bb", "ee", "cc", "dd"}
    testSortStringArr(arr, positiveDstArr, BiDirectional, t)

	arr = []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg"}
	positiveDstArr = []string{"aa", "gg", "bb", "ff", "cc", "ee", "dd"}
    testSortStringArr(arr, positiveDstArr, BiDirectional, t)
} // }}}

func Test_SortStringArr_Exceptional_InvalidType(t *testing.T) { // {{{

	arr := []string{"aa", "bb", "cc", "dd", "ee", "ff"}
	err := SortStringArr(arr, 3)
	if err == nil {
		t.Fatalf("Invalid type:%v, but err is nil", 3)
	}
	code, _ := DecodeErr(err)
	if code != ErrInvalidParam {
		t.Fatalf("err code:%v is not ErrInvalidParam:%v", code, ErrInvalidParam)
	}
	t.Logf("Exception Test! err:%v", err)
} // }}}
