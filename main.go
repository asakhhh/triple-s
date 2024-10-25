package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

var rootDir string

func writeHttpError(w http.ResponseWriter, code int, errorCode string, message string) {
	w.WriteHeader(code)
	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<Error>")
	fmt.Fprintln(w, "\t<Code>"+errorCode+"</Code>")
	fmt.Fprintln(w, "\t<Message>")
	fmt.Fprintln(w, "\t\t"+message)
	fmt.Fprintln(w, "\t</Message>")
	fmt.Fprintln(w, "</Error>")
}

// TODO: GOFUMPT
func getBuckets(w http.ResponseWriter, r *http.Request) {
	bucketMetadata, err := os.Open(filepath.Join(rootDir, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	csvReader := csv.NewReader(bucketMetadata)
	var bkts []map[string]string
	_, err = csvReader.Read() // header record
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}

	fields, err := csvReader.Read()
	for err == nil && len(fields) == 4 {
		bkt := make(map[string]string)
		bkt["Name"] = fields[0]
		bkt["CreationTime"] = fields[1]
		bkt["LastModifiedTime"] = fields[2]
		bkt["Status"] = fields[3]
		if bkt["Status"] == "Active" {
			bkts = append(bkts, bkt)
		}
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Invalid metadata content")
		return
	}

	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<ListAllMyBucketsResult>")
	fmt.Fprintln(w, "\t<Buckets>")
	for _, bkt := range bkts {
		fmt.Fprintln(w, "\t\t<Bucket>")
		fmt.Fprintln(w, "\t\t\t<Name>"+bkt["Name"]+"</Name>")
		fmt.Fprintln(w, "\t\t\t<CreationTime>"+bkt["CreationTime"]+"</CreationTime>")
		fmt.Fprintln(w, "\t\t\t<LastModifiedTime>"+bkt["LastModifiedTime"]+"</LastModifiedTime>")
		fmt.Fprintln(w, "\t\t\t<Status>"+bkt["Status"]+"</Status>")
		fmt.Fprintln(w, "\t\t</Bucket>")
	}
	fmt.Fprintln(w, "\t</Buckets>")
	fmt.Fprintln(w, "</ListAllMyBucketsResult>")
}

func isValidBucketName(bucketName string) (bool, string) {
	charsetAndLength := regexp.MustCompile("^[-.a-z0-9]{3,63}$")
	if !charsetAndLength.MatchString(bucketName) {
		return false, "Contains invalid characters and/or too short/long"
	}
	ipFormat := regexp.MustCompile("^[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+$")
	if ipFormat.MatchString(bucketName) {
		return false, "Name must not match ip-format"
	}
	startHyphen := regexp.MustCompile("^-.*")
	endHyphen := regexp.MustCompile("^.*-$")
	if startHyphen.MatchString(bucketName) || endHyphen.MatchString(bucketName) {
		return false, "Name must not start or end with a hyphen"
	}
	consecutiveHyphens := regexp.MustCompile(".*--.*")
	if consecutiveHyphens.MatchString(bucketName) {
		return false, "Name must not contain consecutive hyphens"
	}
	return true, ""
}

func putBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("BucketName")
	isValid, errMsg := isValidBucketName(bucketName)
	if !isValid {
		writeHttpError(w, http.StatusBadRequest, "BucketNameInvalid", "Bucket name is invalid - "+errMsg)
		return
	}

	bucketPath := filepath.Join(rootDir, bucketName)
	_, err := os.Stat(bucketPath)
	if !(err != nil && os.IsNotExist(err)) {
		writeHttpError(w, http.StatusConflict, "BucketNameUnavailable", "Bucket with this name already exists")
		return
	}

	err = os.Mkdir(bucketPath, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketCreationError", "Could not create bucket")
		return
	}

	info, err := os.Stat(bucketPath)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketAccessError", "Could not access created bucket")
		return
	}

	bucketMetadata, err := os.OpenFile(filepath.Join(rootDir, "buckets.csv"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()
	modTime := info.ModTime()
	modTimeToString := fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", modTime.Year(), modTime.Month(), modTime.Day(), modTime.Hour(), modTime.Minute(), modTime.Second())

	csvWriter := csv.NewWriter(bucketMetadata)
	err = csvWriter.Write([]string{bucketName, modTimeToString, modTimeToString, "Active"})
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
		return
	}
	csvWriter.Flush()

	objectMetadata, err := os.OpenFile(filepath.Join(bucketPath, "objects.csv"), os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not create object metadata")
		return
	}
	defer objectMetadata.Close()
	_, err = objectMetadata.Write([]byte("ObjectKey,Size,ContentType,LastModified\n"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update object metadata")
		return
	}

	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<CreateBucketResult>")
	fmt.Fprintln(w, "\t<Name>"+bucketName+"</Name>")
	fmt.Fprintln(w, "\t<CreationTime>"+modTimeToString+"</CreationTime>")
	fmt.Fprintln(w, "\t<Status>Active</Status>")
	fmt.Fprintln(w, "</CreateBucketResult>")
}

func deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("BucketName")

	bucketMetadata, err := os.Open(filepath.Join(rootDir, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	exists := false
	var bkts [][]string
	for err == nil && len(fields) == 4 {
		if fields[0] == bucketName {
			exists = true
			fields[3] = "Deleted"
		}
		bkts = append(bkts, fields)
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}
	if !exists {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Could not delete - bucket does not exist")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(rootDir, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	defer objectMetadata.Close()
	csvReader = csv.NewReader(objectMetadata)
	_, err = csvReader.Read()
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	_, err = csvReader.Read()
	if err != io.EOF {
		if err == nil {
			writeHttpError(w, http.StatusConflict, "BucketNotEmpty", "Could not delete - bucket not empty")
		} else {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		}
		return
	}

	err = os.Remove(filepath.Join(rootDir, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not delete object metadata")
		return
	}
	err = os.Remove(filepath.Join(rootDir, bucketName))
	successfullyDeleted := true
	if err != nil {
		successfullyDeleted = false
	}
	metadataWrite, err := os.OpenFile(filepath.Join(rootDir, "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer metadataWrite.Close()
	csvWriter := csv.NewWriter(metadataWrite)
	for _, bkt := range bkts {
		if bkt[0] != bucketName || !successfullyDeleted {
			err = csvWriter.Write(bkt)
			if err != nil {
				writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
				return
			}
		}
	}
	csvWriter.Flush()
	w.WriteHeader(http.StatusNoContent)
}

func getObject(w http.ResponseWriter, r *http.Request) {
	bucketMetadata, err := os.Open(filepath.Join(rootDir, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	bucketName := r.PathValue("BucketName")
	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	bucketFound := false
	for err == nil {
		if fields[0] == bucketName {
			bucketFound = true
			break
		}
		fields, err = csvReader.Read()
	}
	if err != io.EOF && !bucketFound {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}
	if !bucketFound {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Bucket does not exist")
		return
	}
	objectKey := r.PathValue("ObjectKey")
	if objectKey == "objects.csv" {
		writeHttpError(w, http.StatusForbidden, "MetadataAccessDenied", "Public metadata access is forbidden")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(rootDir, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	defer objectMetadata.Close()
	csvReader = csv.NewReader(objectMetadata)
	fields, err = csvReader.Read()
	var objectInfo []string
	for err == nil {
		if fields[0] == objectKey {
			objectInfo = fields
			break
		}
		fields, err = csvReader.Read()
	}
	if err != io.EOF && len(objectInfo) == 0 {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		return
	}
	if len(objectInfo) == 0 {
		writeHttpError(w, http.StatusNotFound, "ObjectNotFound", "Object does not exist")
		return
	}

	content, err := os.ReadFile(filepath.Join(rootDir, bucketName, objectKey))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectAccessError", "Could not access object")
		return
	}

	w.Header().Add("Content-Length", objectInfo[1])
	w.Write(content)
}

func putObject(w http.ResponseWriter, r *http.Request) {
	bucketMetadata, err := os.Open(filepath.Join(rootDir, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	bucketName := r.PathValue("BucketName")
	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	var bkts [][]string
	bucketFound := false
	for err == nil {
		if fields[0] == bucketName {
			bucketFound = true
			now := time.Now()
			fields[2] = fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
		}
		bkts = append(bkts, fields)
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}
	if !bucketFound {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Bucket does not exist")
		return
	}
	objectKey := r.PathValue("ObjectKey")
	if objectKey == "objects.csv" {
		writeHttpError(w, http.StatusForbidden, "MetadataAccessDenied", "Metadata editing is forbidden")
		return
	}
	if len(objectKey) > 1024 {
		writeHttpError(w, http.StatusBadRequest, "ObjectKeyInvalid", "Object key is too long (> 1024)")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(rootDir, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	defer objectMetadata.Close()
	csvReader = csv.NewReader(objectMetadata)
	fields, err = csvReader.Read()
	var objs [][]string
	objectFound := false
	for err == nil {
		if fields[0] == objectKey {
			objectFound = true
			fields[1] = r.Header.Get("Content-Length")
			fields[2] = r.Header.Get("Content-Type")
			if len(fields[2]) == 0 {
				fields[2] = "text/plain"
			}
			now := time.Now()
			fields[3] = fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
		}
		objs = append(objs, fields)
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		return
	}
	if !objectFound {
		now := time.Now()
		newRec := []string{objectKey, r.Header.Get("Content-Length"), r.Header.Get("Content-Type"), fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())}
		if len(newRec[2]) == 0 {
			newRec[2] = "text/plain"
		}
		objs = append(objs, newRec)
	}

	object, err := os.OpenFile(filepath.Join(rootDir, bucketName, objectKey), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectAccessError", "Could not access object")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "RequestBodyError", "Could not read request body")
		return
	}
	_, err = object.Write(body)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectWriteError", "Could not write to object")
		return
	}

	objMetadataWrite, err := os.OpenFile(filepath.Join(rootDir, bucketName, "objects.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update object metadata")
		return
	}
	defer objMetadataWrite.Close()
	csvWriter := csv.NewWriter(objMetadataWrite)
	for _, obj := range objs {
		err = csvWriter.Write(obj)
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to object metadata")
			return
		}
	}
	csvWriter.Flush()

	bktMetadataWrite, err := os.OpenFile(filepath.Join(rootDir, "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
		return
	}
	defer bktMetadataWrite.Close()
	csvWriter = csv.NewWriter(bktMetadataWrite)
	for _, bkt := range bkts {
		err = csvWriter.Write(bkt)
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to bucket metadata")
			return
		}
	}
	csvWriter.Flush()
}

func deleteObject(w http.ResponseWriter, r *http.Request) { // PROHIBIT METADATA DELETION
	bucketMetadata, err := os.Open(filepath.Join(rootDir, "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access bucket metadata")
		return
	}
	defer bucketMetadata.Close()

	bucketName := r.PathValue("BucketName")
	csvReader := csv.NewReader(bucketMetadata)
	fields, err := csvReader.Read()
	var bkts [][]string
	bucketFound := false
	for err == nil {
		if fields[0] == bucketName {
			bucketFound = true
		}
		bkts = append(bkts, fields)
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read bucket metadata")
		return
	}
	if !bucketFound {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Bucket does not exist")
		return
	}
	objectKey := r.PathValue("ObjectKey")
	if objectKey == "objects.csv" {
		writeHttpError(w, http.StatusForbidden, "MetadataAccessDenied", "Metadata deleting is forbidden")
		return
	}

	objectMetadata, err := os.Open(filepath.Join(rootDir, bucketName, "objects.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access object metadata")
		return
	}
	defer objectMetadata.Close()
	csvReader = csv.NewReader(objectMetadata)
	fields, err = csvReader.Read()
	var objs [][]string
	objectFound := false
	for err == nil {
		if fields[0] == objectKey {
			objectFound = true
		} else {
			objs = append(objs, fields)
		}
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not read object metadata")
		return
	}
	if !objectFound {
		writeHttpError(w, http.StatusNotFound, "ObjectNotFound", "Object does not exist")
		return
	}

	err = os.Remove(filepath.Join(rootDir, bucketName, objectKey))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "ObjectDeletionError", "Could not delete object")
		return
	}

	objMetadataWrite, err := os.OpenFile(filepath.Join(rootDir, bucketName, "objects.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update object metadata")
		return
	}
	defer objMetadataWrite.Close()
	csvWriter := csv.NewWriter(objMetadataWrite)
	for _, obj := range objs {
		err = csvWriter.Write(obj)
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to object metadata")
			return
		}
	}
	csvWriter.Flush()

	bktMetadataWrite, err := os.OpenFile(filepath.Join(rootDir, "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update bucket metadata")
		return
	}
	defer bktMetadataWrite.Close()
	csvWriter = csv.NewWriter(bktMetadataWrite)
	for i, bkt := range bkts {
		if bkt[0] == bucketName {
			now := time.Now()
			bkts[i][2] = fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
		}
		err = csvWriter.Write(bkts[i])
		if err != nil {
			writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not write to bucket metadata")
			return
		}
	}
	csvWriter.Flush()
	w.WriteHeader(http.StatusNoContent)
}

func badRequest(w http.ResponseWriter, r *http.Request) {
	writeHttpError(w, http.StatusBadRequest, "BadRequest", "Wrong http-method and/or URL-address of the request")
}

func main() {
	http.HandleFunc("/", badRequest)

	http.HandleFunc("GET /{$}", getBuckets)

	http.HandleFunc("PUT /{BucketName}", putBucket)
	http.HandleFunc("PUT /{BucketName}/{$}", putBucket)

	http.HandleFunc("DELETE /{BucketName}", deleteBucket)
	http.HandleFunc("DELETE /{BucketName}/{$}", deleteBucket)

	http.HandleFunc("GET /{BucketName}/{ObjectKey}", getObject)
	http.HandleFunc("GET /{BucketName}/{ObjectKey}/{$}", getObject)

	http.HandleFunc("PUT /{BucketName}/{ObjectKey}", putObject)
	http.HandleFunc("PUT /{BucketName}/{ObjectKey}/{$}", putObject)

	http.HandleFunc("DELETE /{BucketName}/{ObjectKey}", deleteObject)
	http.HandleFunc("DELETE /{BucketName}/{ObjectKey}/{$}", deleteObject)

	portFlag := flag.String("port", "8080", "specify port number")
	dirFlag := flag.String("dir", "data", "specify the root directory for the buckets")
	helpFlag := flag.Bool("help", false, "provides usage information")
	flag.Parse()

	if *helpFlag {
		fmt.Println("Simple Storage Service.")
		fmt.Println()
		fmt.Println("**Usage:**")
		fmt.Println("\ttriple-s [-port <N>] [-dir <S>]")
		fmt.Println("\ttriple-s --help")
		fmt.Println()
		fmt.Println("**Options:**")
		fmt.Println("- --help\tShow this screen.")
		fmt.Println("- --port N\tPort number")
		fmt.Println("- --dir S\tPath to the directory")
		os.Exit(0)
	}

	port, err := strconv.Atoi(*portFlag)
	if err == nil && port == 0 {
		log.Fatal("Port 0 is reserved and cannot be used")
	}

	rootDir = *dirFlag
	_, err = os.Stat(rootDir)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	if err != nil {
		err = os.Mkdir(rootDir, 0o755)
		if err != nil {
			log.Fatal("Could not create directory")
		}
		metadata, err := os.OpenFile(filepath.Join(rootDir, "buckets.csv"), os.O_CREATE|os.O_WRONLY, 0o755)
		if err != nil {
			log.Fatal("Could not initiate metadata")
		}
		_, err = metadata.Write([]byte("Name,CreationTime,LastModifiedTime,Status\n"))
		if err != nil {
			log.Fatal("Could not write to metadata")
		}
	} else {
		_, err := os.Stat(filepath.Join(rootDir, "buckets.csv"))
		if err != nil && os.IsNotExist(err) {
			metadata, err := os.OpenFile(filepath.Join(rootDir, "buckets.csv"), os.O_CREATE|os.O_WRONLY, 0o755)
			if err != nil {
				log.Fatal("Could not initiate metadata")
			}
			_, err = metadata.Write([]byte("Name,CreationTime,LastModifiedTime,Status\n"))
			if err != nil {
				log.Fatal("Could not write to metadata")
			}
		} else if err != nil {
			log.Fatal("Error with metadata")
		}
	}
	log.Fatal(http.ListenAndServe(":"+*portFlag, nil))
}
