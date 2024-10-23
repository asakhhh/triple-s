package main

import (
	"encoding/csv"
	"fmt"
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

	fmt.Fprintln(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
	fmt.Fprintln(w, "<ListAllMyBucketsResult>")
	fmt.Fprintln(w, "\t<Buckets>")
	// buckets
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

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
	w.Write([]byte("405 - Method not allowed - incorrect request"))
}

func main() {
	http.HandleFunc("/", methodNotAllowed)

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
