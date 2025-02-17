package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	hooli "buf.build/gen/go/polygon-io/hooli/protocolbuffers/go"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"
)

// Company struct that we will write to CSV
type Company struct {
	Ticker         string
	Name           string
	SICCode        string
	Description    string
	TotalEmployees uint64
}

// Parquet
type ParquetData struct {
	Ticker      string `parquet:"Ticker"`
	Name        string `parquet:"Name"`
	Currency    string `parquet:"Currency"`
	SICCode     string `parquet:"SICCode"`
	Phone       string `parquet:"Phone"`
	Homepage    string `parquet:"Homepage"`
	Description string `parquet:"Description"`
	Employees   uint64 `parquet:"Employees"`
	RoundLot    uint64 `parquet:"RoundLot"`
}

// Read Protobuf File and convert to Company Struct
func readProtobufFile(filePath string) ([]Company, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var companiesList hooli.DailyTickerFile
	err = proto.Unmarshal(data, &companiesList)
	if err != nil {
		return nil, err
	}

	var companies []Company
	for _, ticker := range companiesList.GetTickers() {
		companies = append(companies, Company{
			Ticker:         ticker.GetSymbol(),
			Name:           ticker.GetCompany(),
			SICCode:        ticker.GetCode(),
			Description:    ticker.GetDescription(),
			TotalEmployees: ticker.GetSize(),
		})
	}

	return companies, nil
}

// Read Parquet File and convert to Company Struct
func readParquetFile(filePath string) ([]Company, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := parquet.NewReader(file)
	defer reader.Close()

	var companies []Company
	for {
		var companiesList ParquetData
		if err := reader.Read(&companiesList); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}

		companies = append(companies, Company{
			Ticker:         companiesList.Ticker,
			Name:           companiesList.Name,
			SICCode:        companiesList.SICCode,
			Description:    companiesList.Description,
			TotalEmployees: companiesList.Employees,
		})
	}

	return companies, nil
}

func writeToCSV(companies []Company) error {
	// Create the output CSV file
	fileName := "reference-tickers_" + time.Now().Format("2006-01-02") + ".csv"
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header
	err = writer.Write([]string{"Ticker", "Name", "SICCode", "Description", "TotalEmployees"})
	if err != nil {
		return err
	}

	// Write the rows
	for _, company := range companies {
		err := writer.Write([]string{
			company.Ticker,
			company.Name,
			company.SICCode,
			company.Description,
			strconv.FormatUint(uint64(company.TotalEmployees), 10),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Function to sort companies by ticker
func sortByTicker(companies []Company) {
	sort.SliceStable(companies, func(i, j int) bool {
		return companies[i].Ticker < companies[j].Ticker
	})
}

func main() {
	// Read from Parquet file
	parquetCompanies, err := readParquetFile("hampton-deville.parquet")
	if err != nil {
		log.Fatalf("Error reading Parquet file: %v", err)
	}

	// Read from Protobuf file
	protoCompanies, err := readProtobufFile("reference-tickers.hooli")
	if err != nil {
		log.Fatalf("Error reading Protobuf file: %v", err)
	}

	// Append all companies together
	companies := append(parquetCompanies, protoCompanies...)

	// Sort by Ticker
	sortByTicker(companies)

	// Write to CSV
	err = writeToCSV(companies)
	if err != nil {
		log.Fatalf("Error writing CSV file: %v", err)
	}

	fmt.Println("file generated")
}
