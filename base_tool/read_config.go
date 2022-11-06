// Package basetool implements a tool of es
package basetool

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// ReadCfgFile Read config file and return configs as map.
// Each line in config file should be like "key value", such as "username Tom".
func ReadCfgFile(filePath string) (map[string]string, error) { // {{{
	var configs map[string]string
	configs = make(map[string]string)

	fi, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error when opening config file [%s]: %s\n", filePath, err)
		return nil, Error{Code: ErrOpenFileFailed, Message: err.Error()}
	}

	defer fi.Close()

	reader := bufio.NewReader(fi)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, Error{Code: ErrReadLineFailed, Message: err.Error()}
		}

		str := strings.TrimSpace(string(line))
		// fmt.Println(str)

		// ignore comments
		if str == "" || strings.HasPrefix(str, "#") {
			continue
		}
		fmt.Println(str)

		kv := strings.Fields(str)
		if len(kv) != 2 {
			fmt.Printf("Warning: invalid config: %s\n", str)
			return nil, Error{Code: ErrInvalidContent, Message: "Invalid content:\"" + str +
				"\" which not to be: key value"}
		}

		configs[kv[0]] = kv[1]
	}

	return configs, nil
} // }}}

// ReadAllLinesInFile Read all lines in a file and return them in a string array.
func ReadAllLinesInFile(filePath string) ([]string, error) { // {{{
	var lines []string

	fi, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error when opening file [%s]: %s\n", filePath, err)
		return nil, Error{Code: ErrOpenFileFailed, Message: err.Error()}
	}

	defer fi.Close()

	reader := bufio.NewReader(fi)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		str := strings.TrimSpace(string(line))
		// ignore comments
		if str == "" || strings.HasPrefix(str, "#") {
			continue
		}

		kv := strings.Fields(str)
		if len(kv) != 1 {
			return nil, Error{Code: ErrInvalidContent, Message: "Invalid content:\"" + str + "\" which not to be key"}
		}

		lines = append(lines, str)
	}

	return lines, nil
} // }}}

// IsDir 判断所给路径是否为文件夹
func IsDir(path string) (bool, error) { // {{{
	s, err := os.Stat(path)
	if err != nil {
		return false, Error{Code: ErrStatFileFailed, Message: err.Error()}
	}
	return s.IsDir(), nil
} // }}}

// ReadWholeFile Get content of whole file
func ReadWholeFile(path string) ([]byte, error) { // {{{
	file, err := os.Open(path)
	if err != nil {
		return nil, Error{Code: ErrOpenFileFailed, Message: err.Error()}
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	return content, nil
} // }}}

// GetLines Get lines of content
func GetLines(content []byte) ([]string, error) { // {{{
	if content == nil {
		return nil, Error{Code: ErrInvalidParam, Message: "content is nil which should not be"}
	}

	lines := strings.Split(string(content), "\n")
	returnLines := make([]string, 0)
	for _, line := range lines {
		trimLine := strings.Trim(string(line), " ")
		if trimLine != "" {
			returnLines = append(returnLines, trimLine)
		}
	}

	return returnLines, nil
} // }}}

// CheckExist Check whether key exist in arr
func CheckExist(key string, arr []string) bool { // {{{
	for _, v := range arr {
		if key == v {
			return true
		}
	}

	return false
} // }}}
