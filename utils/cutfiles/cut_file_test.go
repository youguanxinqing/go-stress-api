package cutfiles

import (
	"testing"
)

func TestCutFile(t *testing.T) {
	filename := "/Users/guan/Mine/Code/go-stress-api/resources/test/img/test1.png"

	chunks, err := CutFile(filename, 10*1024, true)
	if err != nil {
		t.Log(err)
		return
	}
	for chunk := range chunks {
		if chunk.Err != nil {
			t.Log(chunk.Err)
		} else {
			t.Log(chunk)
		}
	}
}

func TestTool_MergeByOneChunkName(t *testing.T) {
	err := CutFileTool.MergeByOneChunkPath(
		"/Users/guan/Mine/Code/go-stress-api/resources/test/img/test1.png.go-stress-chunk.2",
		"/Users/guan/Mine/Code/go-stress-api/resources/test/img/new-test.png",
	)
	if err != nil {
		t.Log(err)
	} else {
		t.Log("Success")
	}
}
