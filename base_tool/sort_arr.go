// Package basetool implements a tool of es
package basetool

import (
	"sort"
	"strconv"
)

// SortStringArr sort array according direction
func SortStringArr(arr []string, direction int) error {
	switch direction {
	case Positive:
		sort.Strings(arr)
		return nil
	case Reverse:
		sort.Sort(sort.Reverse(sort.StringSlice(arr)))
		return nil
	case BiDirectional:
		sort.Strings(arr)
		tmpArr := make([]string, len(arr))
		for i := 0; i < len(arr)/2; i++ {
			tmpArr[2*i] = arr[i]
			tmpArr[2*i+1] = arr[len(arr)-1-i]
		}
		if len(arr)%2 != 0 {
			tmpArr[len(arr)-1] = arr[len(arr)/2]
		}

		for i := 0; i < len(arr); i++ {
			arr[i] = tmpArr[i]
		}
		return nil
	default:
		return Error{Code: ErrInvalidParam, Message: "Invalid type " + strconv.Itoa(direction)}
	}

	return nil
}
