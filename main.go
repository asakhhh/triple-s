package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

// TODO: GOFUMPT
func getBuckets(w http.ResponseWriter, r *http.Request) {
	file, err := os.Open(filepath.Join("data", "buckets.csv"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - Could not access metadata"))
		return
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	var bkts []map[string]string

	_, err = csvReader.Read() // header record
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - Could not access metadata"))
		return
	}

	fields, err := csvReader.Read()
	for err == nil && len(fields) == 4 {
		bkt := make(map[string]string)
		bkt["Name"] = fields[0]
		bkt["CreationTime"] = fields[1]
		bkt["LastModifiedTime"] = fields[2]
		bkt["Status"] = fields[3]
		bkts = append(bkts, bkt)
		fields, err = csvReader.Read()
	}
	if err != io.EOF {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - wrong metadata"))
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

func isValidBucketName(bucketName string) bool {
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return false
	}
	for _, c := range bucketName {
		if c >= 'a' && c <= 'z' {
			continue
		}
		if c >= '0' && c <= '9' {
			continue
		}
		if c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func putBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("BucketName")
	if !isValidBucketName(bucketName) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Bad Request - invalid bucket name"))
		return
	}

	bucketPath := filepath.Join("data", bucketName)
	_, err := os.Stat(bucketPath)
	if !(err != nil && os.IsNotExist(err)) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("409 - Conflict - bucket name already exists"))
		return
	}

	err = os.Mkdir(bucketPath, 0755)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - could not create bucket"))
		return
	}

	info, err := os.Stat(bucketPath)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - could not access created bucket"))
		return
	}

	file, err := os.OpenFile(filepath.Join("data", "buckets.csv"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - Could not access metadata"))
		return
	}
	defer file.Close()
	modTime := info.ModTime()
	modTimeToString := strconv.Itoa(modTime.Year()) + "-" + strconv.Itoa(int(modTime.Month())) + "-" + strconv.Itoa(modTime.Day()) + "T" + strconv.Itoa(modTime.Hour()) + ":" + strconv.Itoa(modTime.Minute())
	csvWriter := csv.NewWriter(file)
	err = csvWriter.Write([]string{bucketName, modTimeToString, modTimeToString, "Active"})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - Could not update metadata"))
		return
	}
	csvWriter.Flush()
	file.Close()

	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<CreateBucketResult>")
	fmt.Fprintln(w, "\t<Name>"+bucketName+"</Name>")
	fmt.Fprintln(w, "\t<CreationTime>"+modTimeToString+"</CreationTime>")
	fmt.Fprintln(w, "\t<Status>Active</Status>")
	fmt.Fprintln(w, "</CreateBucketResult>")
}

func deleteBucket(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "DELETE Bucket")
}

func getObject(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "GET Object")
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
