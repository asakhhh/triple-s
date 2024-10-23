package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

// TODO: GOFUMPT
func getBuckets(w http.ResponseWriter, r *http.Request) {
	file, err := os.Open(filepath.Join("storage", "buckets.csv"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Could not access metadata"))
		return
	}

	csvReader := csv.NewReader(file)
	var bkts []map[string]string

	_, err = csvReader.Read() // header record
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Could not access metadata"))
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
		w.Write([]byte("500 - Internal error while parsing metadata"))
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

func getBucket(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "GET Bucket")
}

func putBucket(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "PUT Bucket")
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

	http.HandleFunc("GET /{BucketName}", getBucket)
	http.HandleFunc("GET /{BucketName}/{$}", getBucket)

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
