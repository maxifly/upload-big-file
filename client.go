package uploadbig

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const MB = 1048576

type Logger struct {
	ErrorLog *log.Logger
	InfoLog  *log.Logger
	DebugLog *log.Logger
}

// UploadData structure
type UploadData struct {
	client            *http.Client
	method            string
	url               string
	filePath          string
	reader            *io.Reader
	id                string
	chunkSize         int
	file              *os.File
	Status            UploadStatus
	logger            Logger
	additionalHeaders map[string]string
}

// UploadStatus holds the data about uploadFile
type UploadStatus struct {
	Size                 int64
	SizeTransferred      int64
	Parts                uint64
	PartsTransferred     uint64
	IsDone               bool
	TransferredException bool
}

func NewUploaderFromReader(method string, url string, reader *io.Reader, size int64, additionalHeaders map[string]string,
	client *http.Client, chunkSize int,
	logger *Logger) *UploadData {

	uploader := createUploader(method, url, additionalHeaders, client, chunkSize, logger)

	uploader.reader = reader
	uploader.Status.Size = size
	return uploader
}

// NewUploaderFromFile  creates new uploader instance
func NewUploaderFromFile(method string, url string, filePath string, additionalHeaders map[string]string,
	client *http.Client, chunkSize int,
	logger *Logger) *UploadData {

	uploader := createUploader(method, url, additionalHeaders, client, chunkSize, logger)

	uploader.filePath = filePath
	return uploader
}

func createUploader(method string, url string, additionalHeaders map[string]string, client *http.Client, chunkSize int,
	logger *Logger) *UploadData {

	if logger == nil {
		logger = &Logger{
			DebugLog: log.New(NewNullWriter(), "DEBUG\t", log.Ldate|log.Ltime|log.Lshortfile),
			InfoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
			ErrorLog: log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile),
		}
	}

	uploadData := &UploadData{
		client:            client,
		method:            method,
		url:               url,
		id:                generateSessionID(),
		chunkSize:         chunkSize,
		logger:            *logger,
		additionalHeaders: additionalHeaders,
		Status: UploadStatus{
			Size:                 0,
			SizeTransferred:      0,
			Parts:                0,
			PartsTransferred:     0,
			IsDone:               false,
			TransferredException: false,
		},
	}

	return uploadData
}

// Init method initializes uploadFile
func (c *UploadData) Init() error {

	if c.filePath != "" {
		fileStat, err := os.Stat(c.filePath)
		if c.checkError(err) {
			return err
		}

		c.Status.Size = fileStat.Size()
		file, err := os.Open(c.filePath)
		if c.checkError(err) {
			return err
		}

		var reader io.Reader = file

		c.reader = &reader
		c.file = file
	}

	defer c.Close()
	c.Status.Parts = uint64(math.Ceil(float64(c.Status.Size) / float64(c.chunkSize)))
	c.uploadFile()
	c.logger.InfoLog.Printf("Done\n")
	return nil
}

func (c *UploadData) Close() {
	c.logger.DebugLog.Printf("Close uploader %s\n", c.id)

	if c.filePath != "" {
		c.logger.DebugLog.Printf("Close file %s\n", c.filePath)
		err := c.file.Close()
		if err != nil {
			c.logger.ErrorLog.Println(err)
		}
	}
}

func (c *UploadData) checkError(err error) bool {
	if err != nil {
		c.logger.ErrorLog.Println(err)
		c.uploadDone(true)
	}
	return err != nil
}

func (c *UploadData) uploadFile() {
	i := uint64(0)

	for !c.Status.IsDone {
		c.uploadChunk(i)
		i = i + 1
	}
}

type CalculateTransferredSize func(body string, partSize int, status UploadStatus) (int64, error)

func calculateTransferredSize(body string, partSize int, status UploadStatus) (int64, error) {
	if body != "" {
		return parseBody(body)
	}

	result := int64(partSize)
	if (result + status.SizeTransferred) > status.Size {
		result = status.Size - status.SizeTransferred
	}

	return result, nil
}

func parseBody(body string) (int64, error) {
	fromTo := strings.Split(body, "/")[0]
	splitted := strings.Split(fromTo, "-")

	partTo, err := strconv.ParseInt(splitted[1], 10, 64)
	if err != nil {
		return 0, err
	}

	return partTo, nil
}

func (c *UploadData) uploadDone(isException bool) {
	if isException {
		c.logger.ErrorLog.Printf("Upload process done by exception\n")
	} else {
		c.logger.InfoLog.Printf("Upload process done\n")
	}
	c.Status.IsDone = true
	c.Status.TransferredException = isException
}

func (c *UploadData) uploadChunk(i uint64) {
	if i == c.Status.Parts {
		c.logger.InfoLog.Printf("Upload %s: done\n", c.id)
		c.uploadDone(false)
	} else if c.Status.TransferredException {
		c.logger.ErrorLog.Printf("ERROR. Transfered exception\n")
	} else {
		fileName := filepath.Base(c.filePath)
		partSize := int(math.Ceil(math.Min(float64(c.chunkSize), float64(c.Status.Size-int64(i*uint64(c.chunkSize))))))
		if partSize <= 0 {
			return
		}

		partBuffer := make([]byte, partSize)
		readBytes, err := io.ReadFull(*c.reader, partBuffer)
		if err != nil {
			c.logger.ErrorLog.Println(err)
			c.uploadDone(true)
			return
		}
		c.logger.DebugLog.Printf("Read %d bytes", readBytes)

		contentRange := generateContentRange(i, c.chunkSize, partSize, c.Status.Size)

		var isSuccess = false
		var responseBody = ""
		var errorCount = 0

		for !isSuccess && errorCount < 3 {
			isSuccess, responseBody, err = httpRequest(c.method, c.url, c.client, c.id, partBuffer, contentRange, fileName, c.logger.DebugLog)
			c.logger.DebugLog.Printf("isSuccess: %t \n", isSuccess)
			if err != nil {
				c.logger.ErrorLog.Println(err)
				isSuccess = false
			}
			if !isSuccess {
				errorCount++
			}
		}

		if isSuccess {
			transferredBytes, err1 := calculateTransferredSize(responseBody, partSize, c.Status)
			if !c.checkError(err1) {
				c.Status.SizeTransferred += transferredBytes
				c.Status.PartsTransferred = i + 1
			}
		} else {
			c.uploadDone(true)
		}

		c.logger.DebugLog.Printf("Part: %d of: %d", c.Status.PartsTransferred, c.Status.Parts)
	}
}

func httpRequest(method string,
	url string,
	additionalHeaders map[string]string,
	client *http.Client,
	sessionID string,
	part []byte,
	contentRange string,
	fileName string,
	debugLogger *log.Logger) (bool, string, error) {
	request, err := http.NewRequest(method, url, bytes.NewBuffer(part))
	if err != nil {
		return false, "", err
	}

	request.Header.Add("Content-Type", "application/octet-stream")
	request.Header.Add("Content-Disposition", "attachment; filename=\""+fileName+"\"")
	request.Header.Add("Content-Range", contentRange)
	request.Header.Add("Session-ID", sessionID)

	if additionalHeaders != nil {
		for key, value := range additionalHeaders {
			request.Header.Add(key, value)
		}
	}

	response, err := client.Do(request)
	if err != nil {
		return false, "", err
	}

	statusCode := response.StatusCode

	debugLogger.Printf("  %s HTTP code %d", contentRange, statusCode)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return false, "", err
	}
	debugLogger.Printf("  Body %v\n", body)
	return statusCode >= 200 && statusCode <= 299, string(body), nil
}
