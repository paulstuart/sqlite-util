package main

import (
	"fmt"
	"log"
	"os"

	"github.com/paulstuart/sqlite"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <db-file> <sql-file>\n", os.Args[0])
		os.Exit(1)
	}
	polygon := sqlite.FuncReg{"polygon", sqlite.ToPolygon, true}
	db, err := sqlite.Open(os.Args[1], sqlite.WithFunctions(polygon))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	file, err := os.ReadFile(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(string(file)); err != nil {
		log.Fatal(err)
	}
}
