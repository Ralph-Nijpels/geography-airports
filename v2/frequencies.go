package airports

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"

	"github.com/minio/minio-go"

	"go.mongodb.org/mongo-driver/bson"

	application "github.com/ralph-nijpels/geography-application/v2"
	datatypes "github.com/ralph-nijpels/geography-datatypes"
)

// Frequencies is the representation of the collection of frequencies as found in
// import / export tables
type Frequencies struct {
	context *application.AppContext
	parent  *Airports
}

// Frequency is the external representation of a single frequency at an airport
type Frequency struct {
	FrequencyType string  `bson:"frequency-type" json:"frequency-type"`
	Description   string  `bson:"description" json:"description,omitempty"`
	Frequency     float64 `bson:"frequency-mhz" json:"frequency-mhz"`
}

// NewFrequencies initializes the collection of frequencies
func (airports *Airports) NewFrequencies() *Frequencies {
	frequencies := Frequencies{
		context: airports.context,
		parent:  airports,
	}
	return &frequencies
}

// RetrieveFromURL downloads the file into the etc directory
func (frequencies *Frequencies) RetrieveFromURL() error {
	// Get the data
	resp, err := http.Get(frequencies.context.FrequenciesURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy the file to S3
	s3Client := frequencies.context.S3Client
	_, err = s3Client.PutObject("csv", "frequencies", resp.Body, -1,
		minio.PutObjectOptions{ContentType: "text/csv"})

	return err
}

func (frequencies *Frequencies) importCSVLine(lineNumber int, line []string) error {
	// Skipping empty lines
	if len(line) == 0 {
		return nil
	}

	// Fetch the airport
	airport, err := frequencies.parent.GetByAirportCode(line[2])
	if err != nil {
		return fmt.Errorf("Frequencies[%d].AirportCode(%s): %v", lineNumber, line[2], err)
	}

	frequencyMhz, _ := datatypes.Frequency(line[5], false)
	// build internal representation
	frequency := Frequency{
		FrequencyType: line[3],
		Description:   line[4],
		Frequency:     frequencyMhz}

	// replace or add frequency...
	found := false
	for i := range airport.Frequencies {
		if airport.Frequencies[i].FrequencyType == frequency.FrequencyType {
			airport.Frequencies[i] = &frequency
			found = true
			break
		}
	}
	if !found {
		airport.Frequencies = append(airport.Frequencies, &frequency)
	}

	// Dump in mongo
	dbConnection, collection, err := frequencies.parent.collection()
	if err != nil {
		return fmt.Errorf("frequencies.importCSVLine[%d]: %v", lineNumber, err)
	}
	defer dbConnection.DBClose()

	_, err = collection.UpdateOne(
		dbConnection.DBContext,
		bson.D{{Key: "icao-airport-code", Value: airport.AirportCode}},
		bson.M{"$set": airport})
	if err != nil {
		return fmt.Errorf("frequencies[%d]: %v", lineNumber, err)
	}

	return nil
}

// ImportCSV imports runways into the airport collection
func (frequencies *Frequencies) ImportCSV() error {

	// Open the frequencies.csv file
	s3Client := frequencies.context.S3Client
	csvFile, err := s3Client.GetObject(
		"csv", "frequencies",
		minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer csvFile.Close()

	// Skip the headerline
	reader := csv.NewReader(bufio.NewReader(csvFile))
	_, err = reader.Read()
	if err != nil {
		return err
	}

	// open the logfile
	_, err = frequencies.context.LogFile("frequencies")
	if err != nil {
		return err
	}
	defer frequencies.context.LogClose()
	frequencies.context.LogPrintln("Start Import")

	// Read the data
	// lineNumbers start at 1 and we've done the header
	lineNumber := 2
	line, err := reader.Read()
	for err == nil {
		err = frequencies.importCSVLine(lineNumber, line)
		frequencies.context.LogError(err)
		line, err = reader.Read()
		lineNumber++
	}

	if err != io.EOF {
		return err
	}

	frequencies.context.LogPrintln("End Import")

	return nil
}
