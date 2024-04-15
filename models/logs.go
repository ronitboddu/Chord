package models

import (
	"fmt"
	"log"
	"os"
)

func WriteLookupLog(str string, filename string) {
	file, err := os.OpenFile("D:/RIT/Capstone/gRPC test/Test2/logs/"+filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println("Could not open lookup_log.txt")
		return
	}

	defer file.Close()

	_, err2 := file.WriteString(str)

	if err2 != nil {
		fmt.Println("Could not write text to lookup_log.txt")
	}
}

func ClearLog(filename string) {
	_, err := os.OpenFile("D:/RIT/Capstone/gRPC test/Test2/logs/"+filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println("Could not open lookup_log.txt")
		return
	}
	err = os.Truncate("D:/RIT/Capstone/gRPC test/Test2/logs/"+filename, 0)
	if err != nil {
		log.Fatal(err)
	}
}
