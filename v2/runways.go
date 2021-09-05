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

// Runways is the representation of the collection of runways. The runways are implemented
// as an element of the Airport, but it has some methods of it's own.
type Runways struct {
	context *application.AppContext
	parent  *Airports
}

// Runway is the database representation of a runway belonging to an Airport
type Runway struct {
	Length  int         `bson:"length" json:"length"`
	Width   int         `bson:"width" json:"width"`
	Surface string      `bson:"surface" json:"surface"`
	Lighted bool        `bson:"lighted" json:"lighted"`
	Closed  bool        `bson:"closed" json:"closed"`
	LowEnd  *RunwaySide `bson:"low-end" json:"low-end"`
	HighEnd *RunwaySide `bson:"high-end" json:"high-end,omitempty"`
}

// RunwaySide expresses the two sides that a Runway usually has (except heliports)
type RunwaySide struct {
	RunwayCode string  `bson:"runway-code" json:"runway-code"`
	Latitude   float64 `bson:"latitude" json:"latitude,omitempty"`
	Longitude  float64 `bson:"longitude" json:"longitude,omitempty"`
	Elevation  int     `bson:"elevation" json:"elevation,omitempty"`
	Heading    int     `bson:"heading" json:"heading,omitempty"`
	Threshold  int     `bson:"threshold" json:"threshold,omitempty"`
}

// NewRunways initializes the collection of runways
func (airports *Airports) NewRunways() *Runways {
	runways := Runways{
		context: airports.context,
		parent:  airports,
	}
	return &runways
}

// RetrieveFromURL downloads the file into the etc directory
func (runways *Runways) RetrieveFromURL() error {
	// Get the data
	resp, err := http.Get(runways.context.RunwaysURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy the file to S3
	s3Client := runways.context.S3Client
	_, err = s3Client.PutObject(
		"csv", "runways", resp.Body, -1,
		minio.PutObjectOptions{ContentType: "text/csv"})

	return err
}

func (runways *Runways) importCSVLine(lineNumber int, line []string) error {
	// Skipping empty lines
	if len(line) == 0 {
		return nil
	}

	// Fetch the airport
	airport, err := runways.parent.GetByAirportCode(line[2])
	if err != nil {
		return fmt.Errorf("Runway[%d].AirportCode(%s): %v", lineNumber, line[2], err)
	}

	runwayLength, err := datatypes.RunwayLength(line[3], false)
	if err != nil {
		return fmt.Errorf("Runway[%d].Length(%s): %v", lineNumber, line[3], err)
	}

	runwayWidth, err := datatypes.RunwayWidth(line[4], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Width(%s): %v", lineNumber, line[4], err)
	}

	runwayLighted, err := datatypes.RunwayLighted(line[6], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lighted(%s): %v", lineNumber, line[6], err)
	}

	runwayClosed, err := datatypes.RunwayClosed(line[7], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Closed(%s): %v", lineNumber, line[7], err)
	}

	// build internal representation
	runway := Runway{
		Length:  runwayLength,
		Width:   runwayWidth,
		Surface: line[5],
		Lighted: runwayLighted,
		Closed:  runwayClosed}

	// Check for any low-end identifier
	if len(line[8]) == 0 {
		return fmt.Errorf("Runway[%d].Lowend.Code(%s): Missing", lineNumber, line[8])
	}

	lowendCode, err := datatypes.RunwayCode(line[8], false, false)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lowend.Code(%s): %v", lineNumber, line[8], err)
	}

	lowendLatitude, err := datatypes.Latitude(line[9], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lowend.Latitude(%s): %v", lineNumber, line[9], err)
	}

	lowendLongitude, err := datatypes.Longitude(line[10], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lowend.Longitude(%s): %v", lineNumber, line[10], err)
	}

	lowendElevation, err := datatypes.Elevation(line[11], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lowend.Elevation(%s): %v", lineNumber, line[11], err)
	}

	lowendHeading, err := datatypes.RunwayHeading(line[12], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lowend.Heading(%s): %v", lineNumber, line[12], err)
	}

	lowendThreshold, err := datatypes.RunwayThreshold(line[13], true)
	if err != nil {
		return fmt.Errorf("Runway[%d].Lowend.Threshold(%s): %v", lineNumber, line[13], err)
	}

	runway.LowEnd = &RunwaySide{
		RunwayCode: lowendCode,
		Latitude:   lowendLatitude,
		Longitude:  lowendLongitude,
		Elevation:  lowendElevation,
		Heading:    lowendHeading,
		Threshold:  lowendThreshold}

	if len(line[14]) > 0 {
		highendCode, err := datatypes.RunwayCode(line[14], false, false)
		if err != nil {
			return fmt.Errorf("Runway[%d].Highend.Code(%s): %v", lineNumber, line[14], err)
		}

		highendLatitude, err := datatypes.Latitude(line[15], true)
		if err != nil {
			return fmt.Errorf("Runway[%d].Highend.Latitude(%s): %v", lineNumber, line[15], err)
		}

		highendLongitude, err := datatypes.Longitude(line[16], true)
		if err != nil {
			return fmt.Errorf("Runway[%d].Highend.Longitude(%s): %v", lineNumber, line[16], err)
		}

		highendElevation, err := datatypes.Elevation(line[17], true)
		if err != nil {
			return fmt.Errorf("Runway[%d].Highend.Elevation(%s): %v", lineNumber, line[17], err)
		}

		highendHeading, err := datatypes.RunwayHeading(line[18], true)
		if err != nil {
			return fmt.Errorf("Runway[%d].Highend.Heading(%s): %v", lineNumber, line[18], err)
		}

		highendThreshold, err := datatypes.RunwayThreshold(line[19], true)
		if err != nil {
			return fmt.Errorf("Runway[%d].Highend.Threshold(%s): %v", lineNumber, line[19], err)
		}

		if highendCode != runway.LowEnd.RunwayCode {
			runway.HighEnd = &RunwaySide{
				RunwayCode: highendCode,
				Latitude:   highendLatitude,
				Longitude:  highendLongitude,
				Elevation:  highendElevation,
				Heading:    highendHeading,
				Threshold:  highendThreshold}
		}
	}

	// replace or add runway...
	found := false
	for i := range airport.Runways {
		if airport.Runways[i].LowEnd.RunwayCode == runway.LowEnd.RunwayCode {
			airport.Runways[i] = &runway
			found = true
			break
		}
	}
	if !found {
		airport.Runways = append(airport.Runways, &runway)
	}

	// Dump in mongo
	dbConnection, collection, err := runways.parent.collection()
	if err != nil {
		return fmt.Errorf("runways.importCSVLine[%d]: %v", lineNumber, err)
	}
	defer dbConnection.DBClose()

	_, err = collection.UpdateOne(
		dbConnection.DBContext,
		bson.D{{Key: "icao-airport-code", Value: airport.AirportCode}},
		bson.M{"$set": airport})
	if err != nil {
		return fmt.Errorf("runways.importCSVLine[%d]: %v", lineNumber, err)
	}

	return nil
}

// ImportCSV imports runways into the airport collection
func (runways *Runways) ImportCSV() error {

	// Open the runways.csv file
	s3Client := runways.context.S3Client
	csvFile, err := s3Client.GetObject(
		"csv", "runways",
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
	_, err = runways.context.LogFile("runways")
	if err != nil {
		return err
	}
	defer runways.context.LogClose()

	runways.context.LogPrintln("Start Import")

	// Read the data
	// lineNumbers start at 1 and we've done the header
	lineNumber := 2
	line, err := reader.Read()
	for err == nil {
		err = runways.importCSVLine(lineNumber, line)
		runways.context.LogError(err)
		line, err = reader.Read()
		lineNumber++
	}

	if err != io.EOF {
		return err
	}

	runways.context.LogPrintln("End Import")

	return nil
}
