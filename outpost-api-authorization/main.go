package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := "mongodb://admin:password@mongodb:27017"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Successfully connected to client")
	time.Sleep(time.Duration(10))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Error pinging MongoDB server: %v", err)
	}
	fmt.Println("Successfully connected and pinged MongoDB!")
	listCtx, listCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer listCancel()
	databases, err := client.ListDatabases(listCtx, struct{}{})
	if err != nil {
		log.Fatalf("Error listing databases: %v", err)
	}
	fmt.Println("\n## 📚 Databases:")
	for _, db := range databases.Databases {
		fmt.Printf("- %s (Size: %.2f MB)\n", db.Name, float64(db.SizeOnDisk)/1024/1024)
	}
}
