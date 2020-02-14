package main

import (
	"context"
	"flag"
	"log"
	"os"

	firebase "firebase.google.com/go"
	loadbusstations "github.com/eltouf/bmtimetable-database/functions/load-bus-stations"
	"google.golang.org/api/option"
)

var filepath string

func init() {
	flag.StringVar(&filepath, "file", "", "help message for flagname")
	flag.Parse()
}

func main() {
	//Get File to Parse
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Can't open file %v", err)
	}

	defer file.Close()

	loadbusstations.ProcessCsvFile(file, initApp())

}

func initApp() *firebase.App {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("error initializing user home: %v\n", err)
	}

	app, err := firebase.NewApp(
		context.Background(),
		&firebase.Config{ProjectID: "bmtimetable", StorageBucket: "bmtimetable.appspot.com"},
		option.WithCredentialsFile(home+string(os.PathSeparator)+".config"+string(os.PathSeparator)+"bmtimetable-381d0e19e193.json"),
	)

	if err != nil {
		log.Fatalf("error initializing firebase application %v", err)
	}

	log.Println("App Initialization OK")

	return app
}
