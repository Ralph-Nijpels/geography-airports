package airports

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"net/http"

	"github.com/minio/minio-go"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	application "github.com/ralph-nijpels/geography-application/v2" 
	datatypes "github.com/ralph-nijpels/geography-datatypes"
	countries "github.com/ralph-nijpels/geography-countries/v2"
)


// Airports is the representation of the collection of Airports in the geography database
type Airports struct {
	context    *application.AppContext
	countries  *countries.Countries
}

// Airport is the external representation for an ICAO-airport including both a bson (for mongo)
// and a json (for REST/GRAPHQL) representation
type Airport struct {
	Airport      primitive.ObjectID `bson:"_id" json:"-"`
	AirportCode  string             `bson:"icao-airport-code" json:"icao-airport-code"`
	AirportName  string             `bson:"airport-name" json:"airport-name"`
	AirportType  string             `bson:"airport-type" json:"airport-type"`
	Latitude     float64            `bson:"latitude" json:"latitude"`
	Longitude    float64            `bson:"longitude" json:"longitude"`
	Elevation    float64            `bson:"elevation" json:"elevation,omitempty"`
	Country      primitive.ObjectID `bson:"country-id" json:"-"`
	CountryCode  string             `bson:"iso-country-code" json:"iso-country-code"`
	RegionCode   string             `bson:"iso-region-code" json:"iso-region-code,omitempty"`
	Municipality string             `bson:"municipality" json:"municipality,omitempty"`
	IATA         string             `bson:"iata-airport-code" json:"iata-airport-code,omitempty"`
	Website      string             `bson:"website" json:"website,omitempty"`
	Wikipedia    string             `bson:"wikipedia" json:"wikipedia,omitempty"`
	Runways      []*Runway          `bson:"runways" json:"runways,omitempty"`
	Frequencies  []*Frequency       `bson:"frequencies" json:"frequencies,omitempty"`
}

func (airports *Airports)collection()(*application.MongoClient, *mongo.Collection, error) {
	dbConnection, err := airports.context.DBOpen()
	if err != nil {
		return nil, nil, fmt.Errorf("airports.collection: %v", err)
	}

	collection := dbConnection.DBClient.Database("flight-schedule").Collection("airports")
	return dbConnection, collection, nil
}

// NewAirports sets up the connection to the database
func NewAirports(application *application.AppContext, countries *countries.Countries) (*Airports, error) {
	airports := Airports{
		context:   application,
		countries: countries}

	// access the collection
	dbConnection, collection, err := airports.collection()
	if err != nil {
		return nil, fmt.Errorf("airports.NewAirports: %v", err)
	}
	defer dbConnection.DBClose()

	// ensure the indices
	airportIndex1 := mongo.IndexModel{Keys: bson.M{"icao-airport-code": 1}}
	collection.Indexes().CreateOne(dbConnection.DBContext, airportIndex1)
	airportIndex2 := mongo.IndexModel{Keys: bson.M{"iata-airport-code": 1}}
	collection.Indexes().CreateOne(dbConnection.DBContext, airportIndex2)

	return &airports, nil
}

// GetByAirportCode retieves an Airport from the database based on its ICAO-Code
func (airports *Airports) GetByAirportCode(airportCode string) (*Airport, error) {
	var result Airport

	parameter, err := datatypes.ICAOAirportCode(airportCode, false, false)
	if err != nil {
		return nil, fmt.Errorf("airports.GetByAirportCode(AirportCode): %v", err)
	}

	dbConnection, collection, err := airports.collection()
	if err != nil {
		return nil, fmt.Errorf("airports.GetByAirportCode: %v", err)
	}
	defer dbConnection.DBClose()

	err = collection.FindOne(dbConnection.DBContext,
		bson.D{{Key: "icao-airport-code", Value: parameter}}).Decode(&result)

	if err != nil {
		return nil, fmt.Errorf("airports.GetByAirportCode: not found")
	}

	return &result, nil
}

// GetByIATACode retrieves an Airport from the database based on its IATA-Code
func (airports *Airports) GetByIATACode(iataCode string) (*Airport, error) {
	var result Airport

	parameter, err := datatypes.IATAAirportCode(iataCode, false, false)
	if err != nil {
		return nil, fmt.Errorf("airports.GetByIATACode(AirportCode): %v", err)
	}

	dbConnection, collection, err := airports.collection()
	if err != nil {
		return nil, fmt.Errorf("airports.GetByIATACode: %v", err)
	}
	defer dbConnection.DBClose()

	err = collection.FindOne(dbConnection.DBContext,
		bson.D{{Key: "iata-airport-code", Value: parameter}}).Decode(&result)

	if err != nil {
		return nil, fmt.Errorf("airports.GetByIATACode: not Found")
	}

	return &result, nil
}

// GetList retrieves a list of Airports based on filter arguments
func (airports *Airports) GetList(countryCode string, regionCode string,
	fromICAO string, untilICAO string, fromIATA string, untilIATA string) ([]*Airport, error) {

	var result []*Airport
	var query = bson.D{{}}

	parameter, err := datatypes.ISOCountryCode(countryCode, false, true)
	if err != nil {
		return nil, fmt.Errorf("GetList.CountryCode(%s): %v", countryCode, err)
	}
	if len(parameter) != 0 {
		query = append(query, bson.E{Key: "iso-country-code", Value: parameter})
	}

	parameter, err = datatypes.ISORegionCode(regionCode, false, true)
	if err != nil {
		return nil, fmt.Errorf("GetList.RegionCode(%s): %v", regionCode, err)
	}
	if len(parameter) != 0 {
		query = append(query, bson.E{Key: "iso-region-code", Value: parameter})
	}

	parameter, err = datatypes.ICAOAirportCode(fromICAO, true, true)
	if err != nil {
		return nil, fmt.Errorf("GetList.FromICAO(%s): %v", fromICAO, err)
	}
	if len(parameter) != 0 {
		query = append(query, bson.E{Key: "icao-airport-code", Value: bson.D{{Key: "$gte", Value: parameter}}})
	}

	parameter, err = datatypes.ICAOAirportCode(untilICAO, true, true)
	if err != nil {
		return nil, fmt.Errorf("GetList.UntilICAO(%s): %v", untilICAO, err)
	}
	if len(parameter) != 0 {
		query = append(query, bson.E{Key: "icao-airport-code", Value: bson.D{{Key: "$lte", Value: parameter}}})
	}

	parameter, err = datatypes.IATAAirportCode(fromIATA, true, true)
	if err != nil {
		return nil, fmt.Errorf("GetList.FromIATA(%s): %v", fromIATA, err)
	}
	if len(parameter) != 0 {
		query = append(query, bson.E{Key: "iata-airport-code", Value: bson.D{{Key: "$gte", Value: parameter}}})
	}

	parameter, err = datatypes.IATAAirportCode(untilIATA, true, true)
	if err != nil {
		return nil, fmt.Errorf("GetList.UntilIATA(%s): %v", untilIATA, err)
	}
	if len(parameter) != 0 {
		query = append(query, bson.E{Key: "iata-airport-code", Value: bson.D{{Key: "$lte", Value: parameter}}})
	}

	findOptions := options.Find()
	findOptions.SetLimit(airports.context.MaxResults + 1)

	dbConnection, collection, err := airports.collection()
	if err != nil {
		return nil, fmt.Errorf("airports.GetList: %v", err)
	}
	defer dbConnection.DBClose()

	cur, err := collection.Find(dbConnection.DBContext, query, findOptions)
	if err != nil {
		return nil, fmt.Errorf("airports.GetList: not found")
	}

	for cur.Next(dbConnection.DBContext) {
		var airport Airport
		cur.Decode(&airport)
		result = append(result, &airport)
	}

	cur.Close(dbConnection.DBContext)

	if int64(len(result)) > airports.context.MaxResults {
		return nil, fmt.Errorf("airports.GetList: too many results")
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("airports.GetList: not found")
	}

	return result, nil
}

// RetrieveFromURL downloads the file into the etc directory
func (airports *Airports) RetrieveFromURL() error {
	// Get the data
	resp, err := http.Get(airports.context.AirportsURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy the file to S3
	s3Client := airports.context.S3Client
	_, err = s3Client.PutObject("csv", "airports", resp.Body, -1,
		minio.PutObjectOptions{ContentType: "text/csv"})

	return err
}

func (airports *Airports) importCSVLine(lineNumber int, line []string) error {

	// Skipping empty lines
	if len(line) == 0 {
		return nil
	}

	// Skip non-ICAO Airports
	airportCode, err := datatypes.ICAOAirportCode(line[1], false, false)
	if err != nil {
		return fmt.Errorf("Airport[%d].ICAO-Airport(%s): %v", lineNumber, line[1], err)
	}

	// Fill only valid IATA codes
	airportIATA, err := datatypes.IATAAirportCode(line[13], false, true)
	if err != nil {
		return fmt.Errorf("Airport[%d].IATA-Airport(%s): %v", lineNumber, line[13], err)
	}

	// Check for valid Country
	country, err := airports.countries.GetByCountryCode(line[8])
	if err != nil {
		return fmt.Errorf("Airport[%d].Country(%s): %v", lineNumber, line[8], err)
	}

	// Check for valid Region
	// The region key in the file is composed from the CountryCode and RegionCode
	regionKey := strings.Split(line[9], "-")
	if len(regionKey) != 2 {
		return fmt.Errorf("Airport[%d].Region(%s): %s", lineNumber, line[9], "Bad region key")
	}
	var region *countries.Region
	for i := range country.Regions {
		if country.Regions[i].RegionCode == regionKey[1] {
			region = country.Regions[i]
		}
	}
	if region == nil {
		return fmt.Errorf("Airport[%d].Region(%s): %v", lineNumber, line[9], "not found")
	}

	// Check Lattitude
	latitude, err := datatypes.Latitude(line[4], false)
	if err != nil {
		return fmt.Errorf("Airport[%d].Latitude: %v", lineNumber, err)
	}

	// Check Longitude
	longitude, err := datatypes.Longitude(line[5], false)
	if err != nil {
		return fmt.Errorf("Airport[%d].Longitude: %v", lineNumber, err)
	}

	// Check Elevation
	elevation, err := datatypes.Elevation(line[6], true)
	if err != nil {
		return fmt.Errorf("Airport[%d].Elevation: %v", lineNumber, err)
	}

	// Define an insert structure without the ID to prevent race-conditions
	// in the upsert function.
	type insertAirport struct {
		AirportCode  string             `bson:"icao-airport-code"`
		AirportName  string             `bson:"airport-name"`
		AirportType  string             `bson:"airport-type"`
		Latitude     float64            `bson:"latitude"`
		Longitude    float64            `bson:"longitude"`
		Elevation    int                `bson:"elevation"`
		Country      primitive.ObjectID `bson:"country-id"`
		CountryCode  string             `bson:"iso-country-code"`
		RegionCode   string             `bson:"iso-region-code"`
		Municipality string             `bson:"municipality"`
		IATA         string             `bson:"iata-airport-code"`
		Website      string             `bson:"website"`
		Wikipedia    string             `bson:"wikipedia"`
	}
	
	// Build internal representation
	airport := insertAirport{
		AirportCode:  airportCode,
		AirportName:  line[3],
		AirportType:  line[2],
		Latitude:     latitude,
		Longitude:    longitude,
		Elevation:    elevation,
		Country:      country.Country,
		CountryCode:  country.CountryCode,
		RegionCode:   region.RegionCode,
		Municipality: line[10],
		IATA:         airportIATA,
		Website:      line[15],
		Wikipedia:    line[16],
	}

	// Dump in mongo
	dbConnection, collection, err := airports.collection()
	if err != nil {
		return fmt.Errorf("airports.importCSVLine: %v", err)
	}
	defer dbConnection.DBClose()

	_, err = collection.UpdateOne(dbConnection.DBContext,
		bson.D{{Key: "icao-airport-code", Value: airport.AirportCode}},
		bson.M{"$set": airport},
		options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("airports.importCSVLine: %v", err)
	}

	return nil
}

// ImportCSV imports a csv file into the Airports collection
func (airports *Airports) ImportCSV() error {

	// Open the airports.csv file
	s3Client := airports.context.S3Client
	csvFile, err := s3Client.GetObject(
		"csv", "airports",
		minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer csvFile.Close()

	// Open the logfile
	_, err = airports.context.LogFile("airports")
	if err != nil {
		return err
	}
	defer airports.context.LogClose()

	// Skip the headerline
	reader := csv.NewReader(bufio.NewReader(csvFile))
	_, err = reader.Read()
	if err != nil {
		return err
	}

	airports.context.LogPrintln("Start Import")

	// Read the data
	// Line Numbers start at 1 and we've done the header
	lineNumber := 2
	line, err := reader.Read()
	for err == nil {
		err = airports.importCSVLine(lineNumber, line)
		airports.context.LogError(err)
		line, err = reader.Read()
		lineNumber++
	}

	if err != io.EOF {
		return err
	}

	airports.context.LogPrintln("End Import")

	return nil
}
