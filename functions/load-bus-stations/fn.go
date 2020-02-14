package loadbusstations

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"strconv"
	"strings"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"github.com/eltouf/bmtimetable-database/model"
)

// ProcessCsvFile ETL to add data into firestore
// Extract data from CSV File
// Transform Row into a Struct
// Load Add Or Update a document into collecions
func ProcessCsvFile(file io.Reader, app *firebase.App) {
	reader := csv.NewReader(file)
	reader.Comma = ';'

	head, err := readHead(reader)

	if err != nil {
		return
	}

	firestore := initFirestore(app)
	defer firestore.Close()

	row, err := reader.Read()
	station, err := transform(row, head)
	load(station, firestore)
}

func readHead(reader *csv.Reader) (map[string]uint8, error) {
	record, err := reader.Read()

	if err != nil {
		return nil, err
	}

	head := make(map[string]uint8, len(record))

	for k, v := range record {
		head[strings.ToLower(v)] = uint8(k)
	}

	log.Println(head)

	return head, nil
}
func transform(row []string, head map[string]uint8) (*model.BusStation, error) {

	cityCode, err := strconv.Atoi(row[head["code_commune"]])
	if err != nil {
		return nil, err
	}

	return &model.BusStation{
		Gid:      row[head["gid"]],
		Name:     row[head["libelle"]],
		Ident:    row[head["ident"]],
		Group:    row[head["groupe"]],
		City:     row[head["commune"]],
		CityCode: uint16(cityCode),
	}, nil
}
func load(station *model.BusStation, client *firestore.Client) {
	_, err := client.Collection("stations").Doc(station.Gid).Set(context.Background(), station)

	if err != nil {
		// Handle any errors in an appropriate way, such as returning them.
		log.Printf("An error has occurred: %s", err)
	}
}

func initFirestore(app *firebase.App) *firestore.Client {
	client, err := app.Firestore(context.Background())

	if err != nil {
		log.Fatalf("error initializing firestore client %v", err)
	}

	log.Println("Firestore Initialization OK")

	return client
}
