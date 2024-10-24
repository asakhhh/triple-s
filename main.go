package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

func writeHttpError(w http.ResponseWriter, code int, errorCode string, message string) {
	w.WriteHeader(code)
	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<Error>")
	fmt.Fprintln(w, "\t<Code>" + errorCode + "</Code>")
	fmt.Fprintln(w, "\t<Message>")
	fmt.Fprintln(w, "\t\t" + message)
	fmt.Fprintln(w, "\t</Message>")
	fmt.Fprintln(w, "</Error>")
}

// TODO: GOFUMPT
func getBuckets(w http.ResponseWriter, r *http.Request) {
	file, err := os.Open(filepath.Join("data", "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access metadata")
		return
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	var bkts []map[string]string
	_, err = csvReader.Read() // header record
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access metadata")
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
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Invalid content in metadata")
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
		writeHttpError(w, http.StatusBadRequest, "BucketNameInvalid", "Bucket name is invalid - " + errMsg)
		return
	}

	bucketPath := filepath.Join("data", bucketName)
	_, err := os.Stat(bucketPath)
	if !(err != nil && os.IsNotExist(err)) {
		writeHttpError(w, http.StatusConflict, "BucketNameUnavailable", "Bucket with this name already exists")
		return
	}

	err = os.Mkdir(bucketPath, 0755)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketCreationError", "Error while creating bucket")
		return
	}

	info, err := os.Stat(bucketPath)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketAccessError", "Error while accessing the created bucket")
		return
	}

	file, err := os.OpenFile(filepath.Join("data", "buckets.csv"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access metadata")
		return
	}
	defer file.Close()
	modTime := info.ModTime()
	modTimeToString := strconv.Itoa(modTime.Year()) + "-" + strconv.Itoa(int(modTime.Month())) +
		"-" + strconv.Itoa(modTime.Day()) + "T" +
		strconv.Itoa(modTime.Hour()) + ":" + strconv.Itoa(modTime.Minute())

	csvWriter := csv.NewWriter(file)
	err = csvWriter.Write([]string{bucketName, modTimeToString, modTimeToString, "Active"})
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update metadata")
		return
	}
	csvWriter.Flush()

	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<CreateBucketResult>")
	fmt.Fprintln(w, "\t<Name>"+bucketName+"</Name>")
	fmt.Fprintln(w, "\t<CreationTime>"+modTimeToString+"</CreationTime>")
	fmt.Fprintln(w, "\t<Status>Active</Status>")
	fmt.Fprintln(w, "</CreateBucketResult>")
}

func deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("BucketName")

	metadata, err := os.Open(filepath.Join("data", "buckets.csv"))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access metadata")
		return
	}
	defer metadata.Close()

	csvReader := csv.NewReader(metadata)
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
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access metadata")
		return
	}
	if !exists {
		writeHttpError(w, http.StatusNotFound, "BucketNotFound", "Could not delete - bucket does not exist")
		return
	}

	bkt, err := os.Open(filepath.Join("data", bucketName))
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "BucketAccessError", "Could not access bucket")
		return
	}
	defer bkt.Close()
	_, err = bkt.Readdirnames(1)
	if err != io.EOF {
		writeHttpError(w, http.StatusConflict, "BucketNotEmpty", "Could not delete - bucket not empty")
		return
	}

	err = os.Remove(filepath.Join("data", bucketName))
	successfullyDeleted := true
	if err != nil {
		successfullyDeleted = false
	}
	metadataWrite, err := os.OpenFile(filepath.Join("data", "buckets.csv"), os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not access metadata")
		return
	}
	defer metadataWrite.Close()
	csvWriter := csv.NewWriter(metadataWrite)
	for _, bkt := range bkts {
		if bkt[0] != bucketName || !successfullyDeleted {
			err = csvWriter.Write(bkt)
			if err != nil {
				writeHttpError(w, http.StatusInternalServerError, "MetadataError", "Could not update metadata")
				return
			}
		}
	}
	csvWriter.Flush()
	w.WriteHeader(http.StatusNoContent)
}

func getObject(w http.ResponseWriter, r *http.Request) {

}

func putObject(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "PUT Object")
}

func deleteObject(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "DELETE Object")
}

func badRequest(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte("400 - Bad request - wrong method or url"))
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

	log.Fatal(http.ListenAndServe(":1024", nil))
}
