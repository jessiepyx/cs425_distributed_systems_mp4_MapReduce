package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// input a file of list of pair, output a pair
func ReducePhase(filename string) {
	f, err := os.Open(filename)
	keys := strings.Split(filename, "_")
	key := keys[len(keys)-1]
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(f)
	res := 0
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		res++
	}
	if err := f.Close(); err != nil {
		log.Fatalln(err)
	}
	fmt.Print(key + "," + strconv.Itoa(res))
}

func main() {
	if len(os.Args) == 2 {
		ReducePhase(os.Args[1])
	}
}
