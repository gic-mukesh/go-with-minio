package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var minioClient *minio.Client
var ctx context.Context

const (
	BUCKET_NAME          = "my-test-bucket"
	LOCATION             = "us-east-1"
	ENDPOINT             = "localhost:8000"
	ACCESS_KEY_ID        = "minioadmin"
	SECRET_ACCESS_KEY    = "minioadmin"
	USE_SSL              = false
	MAX_UPLOAD_SIZE      = 10 << 20      // 10 MB
	PRESIGNED_URL_EXPIRY = time.Hour * 1 // presigned url will expire in 1 hour
)

func init() {

	// Initialize minio client object.
	var err error
	minioClient, err = minio.New(ENDPOINT, &minio.Options{
		Creds:  credentials.NewStaticV4(ACCESS_KEY_ID, SECRET_ACCESS_KEY, ""),
		Secure: USE_SSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	ctx = context.Background()
	// Make a new bucket called mymusic.

	err = minioClient.MakeBucket(ctx, BUCKET_NAME, minio.MakeBucketOptions{Region: LOCATION})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, BUCKET_NAME)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", BUCKET_NAME)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", BUCKET_NAME)
	}

}

func uploadToMinio(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, MAX_UPLOAD_SIZE)

	if err := r.ParseMultipartForm(MAX_UPLOAD_SIZE); err != nil {

		http.Error(w, "The uploaded file is too big. Please choose an file that's less than 1MB in size", http.StatusBadRequest)
		return

	}

	file, fileHeader, err := r.FormFile("file")

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer file.Close()

	// Upload the zip file
	objectName := fileHeader.Filename

	info, err := minioClient.PutObject(ctx, BUCKET_NAME, objectName, file, -1, minio.PutObjectOptions{})
	// Upload the zip file with FPutObject
	if err != nil {
		log.Fatalln(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	respondWithJson(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("Successfully uploaded %s of size %d", objectName, info.Size),
	})

	log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)
}

func findOnMinio(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filename := r.URL.Query().Get("filename")
	if filename == "" {
		http.Error(w, "A valid filename should be provided in URL query param", http.StatusBadRequest)
		return
	}

	presignedURL, err := minioClient.PresignedGetObject(ctx, BUCKET_NAME, filename, PRESIGNED_URL_EXPIRY, make(url.Values))

	if err != nil {
		http.Error(w, fmt.Sprintf("Unable to find file %q on the minio server", filename), http.StatusBadRequest)
		return
	}
	respondWithJson(w, http.StatusOK, presignedURL.String())
}

func deleteFromMinio(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filename := r.URL.Query().Get("filename")
	if filename == "" {
		http.Error(w, "A valid filename should be provided in URL query param", http.StatusBadRequest)
		return
	}

	err := minioClient.RemoveObject(ctx, BUCKET_NAME, filename, minio.RemoveObjectOptions{})
}

func respondWithJson(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func main() {

	mux := http.NewServeMux()

	mux.HandleFunc("/upload-to-minio", uploadToMinio)
	mux.HandleFunc("/find-on-minio/", findOnMinio)
	mux.HandleFunc("/delete-from-minio/", deleteFromMinio)

	err := http.ListenAndServe(":4000", mux)

	if err != nil {
		log.Fatal(err)
	}
}
