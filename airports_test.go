package airports

import (
	"fmt"
	"testing"

	application "github.com/ralph-nijpels/geography-application"
	countries "github.com/ralph-nijpels/geography-countries"
)

func TestImportCSV(t *testing.T) {
	fmt.Println("Testing ImportCSV..")

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}
	defer context.Destroy()

	countries := countries.NewCountries(context)
	airports  := NewAirports(context, countries)

	err = airports.ImportCSV()
	if err != nil {
		t.Errorf("%v", err)
	}
}

