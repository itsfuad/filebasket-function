package handler

import (
	"fmt"
	"os"
	"sync"
	"time"


	//load dotenv
	"github.com/joho/godotenv"

	"github.com/appwrite/sdk-for-go/appwrite"
	"github.com/appwrite/sdk-for-go/models"
	"github.com/open-runtimes/types-for-go/v4/openruntimes"
)

func Main(Context openruntimes.Context) openruntimes.Response {
	
	godotenv.Load()

	client := appwrite.NewClient(
		appwrite.WithEndpoint(os.Getenv("APPWRITE_FUNCTION_API_ENDPOINT")),
		appwrite.WithProject(os.Getenv("APPWRITE_FUNCTION_PROJECT_ID")),
		appwrite.WithKey(Context.Req.Headers["x-appwrite-key"]),
	)

	if Context.Req.Path == "/health" {
		health := appwrite.NewHealth(client)
		status, err := health.Get()
		if err != nil {
			return Context.Res.Text("Error: " + err.Error())
		}
		return Context.Res.Json(status)
	}

	if Context.Req.Path == "/cleanup" {

		DATABASE_ID  := os.Getenv("DATABASE_ID")
		COLLECTION_ID := os.Getenv("COLLECTION_ID")
		BUCKET_ID := os.Getenv("BUCKET_ID")

		dbs := appwrite.NewDatabases(client)
		storage := appwrite.NewStorage(client)

		// get all files
		files, err := dbs.ListDocuments(DATABASE_ID, COLLECTION_ID)
		if err != nil {
			return Context.Res.Text("Error: " + err.Error())
		}

		var wg sync.WaitGroup
		mu := &sync.Mutex{} // To safely append to texts concurrently
		texts := ""

		// get current time
		for _, file := range files.Documents {

			createdTime, err := time.Parse("2006-01-02T15:04:05.000+00:00", file.CreatedAt)
			if err != nil {
				return Context.Res.Text("Error: " + err.Error())
			}

			currentTime := time.Now()
			diff := currentTime.Sub(createdTime)

			// Check if the file is older than 6 hours
			if diff.Hours() >= 6 {
				wg.Add(1)
				go func(file models.Document) {
					defer wg.Done()
					// delete file and entry concurrently
					_, err := storage.DeleteFile(BUCKET_ID, file.Id)
					if err != nil {
						mu.Lock()
						texts += fmt.Sprintf("Error deleting file: %s - %s\n", file.Id, err.Error())
						mu.Unlock()
						return
					}

					_, err = dbs.DeleteDocument(file.DatabaseId, file.CollectionId, file.Id)
					if err != nil {
						mu.Lock()
						texts += fmt.Sprintf("Error deleting file entry: %s - %s\n", file.Id, err.Error())
						mu.Unlock()
						return
					}

					// Safely append the result
					mu.Lock()
					texts += fmt.Sprintf("File: %s - Deleted\n", file.Id)
					mu.Unlock()
				}(file)
			}
		}

		// Wait for all goroutines to complete
		wg.Wait()

		if texts != "" {
			return Context.Res.Text(texts)
		}

		return Context.Res.Text("No files to delete")
	}

	return Context.Res.Text("Yay! Your function executed successfully!")
}