package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/gorilla/mux"
)

const storageAccountName = "chrisglearningstore"
const storageAccountKey = "JyHvy0AXXkr+KqUOfbceW+xuqiQBJYDhylJk+6JgpLJGqm4M/8yuCA6+cN+4jLnX/mdTIoXARrUuEwIuulzcuQ=="
const storageQueueName = "goqueue"

type QueueNotificationMessage struct {
	NotificationType     int
	ApplicationReference string
	BankReference        string
	EventDate            string
	EventType            string
	EventComment         string
	RequestType          string
	MessageStatus        string
	Message              string
	EventId              int
}

func getAccountURL(storageAccountName, storageQueueName string) (*url.URL, error) {
	_url, err := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net/%s", storageAccountName, storageQueueName))
	if err != nil {
		err = fmt.Errorf(" error parsing url %v", err)
	}

	return _url, err
}

func getCredentials(storageAccountName, storageAccountKey, storageQueueName string) (azqueue.Credential, error) {

	_url, err := getAccountURL(storageAccountName, storageQueueName)
	if err != nil {
		log.Fatal("Error getting Acount URL: ", err)
	}

	fmt.Printf("Using queue URL: %v\n", _url)

	credential, err := azqueue.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	if err != nil {
		err = fmt.Errorf(" error parsing url %v", err)
	}

	fmt.Printf("Using queue credentials: %v\n", credential.AccountName())

	return credential, err
}

func enqueMessage(storageAccountName, storageAccountKey, storageQueueName string, queueMsg QueueNotificationMessage) error {

	ctx := context.TODO()
	jsonMsg, err := json.Marshal(&queueMsg)
	if err != nil {
		return err
	}
	msgString := string(jsonMsg)
	credential, err := getCredentials(storageAccountName, storageAccountKey, storageQueueName)
	if err != nil {
		return err
	}
	_url, err := getAccountURL(storageAccountName, storageQueueName)
	if err != nil {
		return err
	}

	queueUrl := azqueue.NewQueueURL(*_url, azqueue.NewPipeline(credential, azqueue.PipelineOptions{}))
	msgUrl := queueUrl.NewMessagesURL()
	msgUrl.Enqueue(ctx, msgString, 30, 0)
	return nil
}

func createQueueMessage(w http.ResponseWriter, r *http.Request) {
	// get the body of our POST request
	// return the string response containing the request body
	fmt.Printf(" new message posted")
	reqBody, _ := ioutil.ReadAll(r.Body)
	var msg QueueNotificationMessage
	json.Unmarshal(reqBody, &msg)
	err := enqueMessage(storageAccountName, storageAccountKey, storageQueueName, msg)
	if err != nil {
		fmt.Fprintf(w, "error enqueing msg :\n%+v", err)
		fmt.Printf("error enqueing msg :\n%+v", err)
	}
	fmt.Fprintf(w, "recieved :\n%+v", string(reqBody))
	fmt.Printf(" enqueued msg\n%+v", string(reqBody))
}

func handleRequests() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/", createQueueMessage).Methods("POST")
	log.Fatal(http.ListenAndServe(":8080", myRouter))
}

func main() {
	handleRequests()
}
