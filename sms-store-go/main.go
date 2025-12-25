package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SmsRecord struct {
	PhoneNumber string `json:"phoneNumber" bson:"phoneNumber"`
	Message     string `json:"message" bson:"message"`
	Status      string `json:"status" bson:"status"`
}

var collection *mongo.Collection

func main() {
	client, _ := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	collection = client.Database("meesho_db").Collection("sms_history")
	fmt.Println("Success: Connected to MongoDB")

	go startKafkaConsumer()

	http.HandleFunc("/v1/user/", getHistory)

	fmt.Println("Go SMS Store running on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func startKafkaConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "sms-log-topic",
		GroupID: "sms-store-group",
	})
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Kafka Error:", err)
			continue
		}
		var record SmsRecord
		json.Unmarshal(m.Value, &record)
		collection.InsertOne(context.TODO(), record)
		fmt.Printf("Logged SMS for %s\n", record.PhoneNumber)
	}
}

func getHistory(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if !strings.HasSuffix(path, "/messages") {
		http.Error(w, "Use /v1/user/{phone}/messages", http.StatusNotFound)
		return
	}

	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 3 {
		http.Error(w, "Invalid Path", http.StatusBadRequest)
		return
	}
	phone := segments[2]

	cursor, _ := collection.Find(context.TODO(), bson.M{"phoneNumber": phone})
	var results []SmsRecord
	cursor.All(context.TODO(), &results)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}