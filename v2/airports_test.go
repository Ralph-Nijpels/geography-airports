package airports

import (
	"fmt"
	"testing"

	application "github.com/ralph-nijpels/geography-application/v2"
	countries "github.com/ralph-nijpels/geography-countries/v2"
)

// TestImportCSV checks if the import function works. It assumes that the CSV itself is ready to go
// in the S3 bucket and will change the data in the MongoDB (it will actually import stuff)
// Note: there is no test-set of data, so it will time-out. For now we consider that a good thing.
func TestImportCSV(t *testing.T) {
	fmt.Println("Testing ImportCSV..")

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("internal error: %v", err)
	} 
	defer context.Destroy()


	countries, err := countries.NewCountries(context)
	if err != nil {
		t.Errorf("internal error: %v", err)
	}

	airports, err := NewAirports(context, countries)
	if err != nil {
		t.Errorf("internal error: %v", err)
	}

	err = airports.ImportCSV()
	if err != nil {
		t.Errorf("%v", err)
	}
}

