package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

	fmt.Println("Go Service running on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func startKafkaConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "sms-log-topic",
		GroupID: "sms-store-group",
	})

	for {
		m, _ := reader.ReadMessage(context.Background())
		var record SmsRecord
		json.Unmarshal(m.Value, &record)
		collection.InsertOne(context.TODO(), record)
		if(record.PhoneNumber != "") {
			fmt.Printf("What the FUCK!!\n")
			fmt.Printf("Saved SMS for %s to MongoDB\n", record.PhoneNumber)
		}
	}
}

func getHistory(w http.ResponseWriter, r *http.Request) {
	phone := r.URL.Path[len("/v1/user/"):len(r.URL.Path)-len("/messages")]

	cursor, _ := collection.Find(context.TODO(), bson.M{"phoneNumber": phone})
	var results []SmsRecord
	cursor.All(context.TODO(), &results)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}