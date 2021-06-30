package cutfiles

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

const ChunkSpecialFlag = "go-stress-chunk"

type FileChunk struct {
	Name             string `json:"name"`
	Path             string `json:"path"`
	OriginalFileName string `json:"original_file_name"`
	Size             int64  `json:"total"`
	Content          []byte `json:"content"`
	IsFallDisk       bool   `json:"is_fall_disk"`
	Offset           int    `json:"offset"`
	Err              error  `json:"err"`
}

func NewFileChunk(
	originalFileName string,
	offset int,
	size int64,
	content []byte,
	isFallDisk bool,
) *FileChunk {
	chunkFilPath := fmt.Sprintf("%s.%s.%s", originalFileName, ChunkSpecialFlag, strconv.Itoa(offset))
	chunkFileName := path.Base(chunkFilPath)
	return &FileChunk{
		Name:             chunkFileName,
		Path:             chunkFilPath,
		OriginalFileName: originalFileName,
		Size:             size,
		Content:          content,
		IsFallDisk:       isFallDisk,
	}
}

func NewErrorFileChunk(err error) *FileChunk {
	fileChunk := new(FileChunk)
	fileChunk.Err = err
	return fileChunk
}

// FallDisk chunk 落盘
func (chunk *FileChunk) FallDisk() {
	if !chunk.IsFallDisk {
		return
	}

	file, err := os.Create(chunk.Path)
	if err != nil {
		chunk.Err = err
		return
	}
	defer file.Close()

	_, err = file.Write(chunk.Content)
	chunk.Err = err
}

func (chunk *FileChunk) String() string {
	return fmt.Sprintf("[chunkname]: %s, [size]: %d", chunk.Name, chunk.Size)
}

// CutFile 拆分文件
// - maxSize: 单位 byte
func CutFile(filename string, maxSize int64, isFallDisk bool) (<-chan *FileChunk, error) {
	ch := make(chan *FileChunk)
	fileInfo, err := os.Stat(filename)
	if err != nil {
		close(ch)
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		close(ch)
		return nil, err
	}

	go func() {
		defer file.Close()

		offset := 0
		totalSize := fileInfo.Size()
		for totalSize != 0 {
			buf := make([]byte, maxSize)
			n, err := file.Read(buf)
			if err != nil {
				ch <- NewErrorFileChunk(err)
				break
			} else {
				chunk := NewFileChunk(filename, offset, int64(n), buf[:n], isFallDisk)
				chunk.FallDisk()
				ch <- chunk
			}
			totalSize -= int64(n)
			offset += 1
		}
		close(ch)
	}()

	return ch, nil
}

// *********** tools ***************

type tool struct{}

// GetChunkPathsByFilePath 传入 filePath, 返回旗下所有的 chunk
func (t *tool) GetChunkPathsByFilePath(filePath string) (map[int]string, error) {
	chunkDir := path.Dir(filePath)
	fileInfos, err := ioutil.ReadDir(chunkDir)
	if err != nil {
		return nil, err
	}

	chunksMap := make(map[int]string)
	chunkPrefix := fmt.Sprintf("%s.%s.", filePath, ChunkSpecialFlag)
	for _, info := range fileInfos {
		wholeFilePath := path.Join(chunkDir, info.Name())
		if strings.HasPrefix(wholeFilePath, chunkPrefix) {
			noStr := strings.TrimPrefix(wholeFilePath, chunkPrefix)
			no, _ := strconv.Atoi(noStr)
			chunksMap[no] = wholeFilePath
		}
	}
	return chunksMap, nil
}

// MergeByOneFileChunk 传入任意一个 chunk, 自动合并所有 chunk
func (t *tool) MergeByOneFileChunk(chunk *FileChunk, targetFilePath string) error {
	return t.MergeByOneChunkPath(chunk.Path, targetFilePath)
}

func (t *tool) MergeByOneChunkPath(chunkPath, targetFilePath string) error {
	// filePath.[ChunkSpecialFlag].[no]
	clauses := strings.Split(chunkPath, ".")
	filePath := strings.Join(clauses[0:len(clauses)-2], ".")
	chunksMap, err := t.GetChunkPathsByFilePath(filePath)
	if err != nil {
		return err
	}

	file, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	i := 0
	for ; ; i++ {
		oneChunkPath, ok := chunksMap[i]
		if !ok {
			break
		}

		chunkFile, err := os.Open(oneChunkPath)
		if err != nil {
			return err
		}

		chunkContent, _ := ioutil.ReadAll(chunkFile)
		if _, err = file.Write(chunkContent); err != nil {
			return err
		}
		_ = chunkFile.Close()
	}
	return nil
}

var CutFileTool = &tool{}
