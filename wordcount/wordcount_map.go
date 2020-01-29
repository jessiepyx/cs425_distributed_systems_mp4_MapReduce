package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

// input a string, output a list of pair,
func MapPhase(in string) {

	var result []string
	var a = strings.FieldsFunc(in, func(r rune) bool { return strings.ContainsRune(" \t\n", r) })
	for i := 0; i < len(a); i++ {
		result = append(result, a[i]+","+"1")
	}
	res, err := json.Marshal(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(res))
}

func main() {
	if len(os.Args) == 2 {
		inputStr := os.Args[1]
		MapPhase(inputStr)
	}
}
