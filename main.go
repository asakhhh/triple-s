package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.EscapedPath())
	if r.Method == "PUT" {
		bucketName := r.URL.Path[len("/"):]
		fmt.Println(bucketName)
		if 3 <= len(bucketName) && len(bucketName) <= 63 {
			err := os.Mkdir(bucketName, 0600)
			if err != nil {
				fmt.Println("couldn't create folder")
			}
		} else {
			fmt.Fprintf(w, "Bad folder naming")
		}
	} else if r.Method == "GET" {
	} else if r.Method == "DELETE" {
	} else {
	}
}

func main() {
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":1024", nil))
}
