package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

var mainChannel = make(chan string)

type RPCTask struct {
	fileName string
	ip       string
	call     rpc.Call
}

func (mj *MapleJuiceServer) ScheduleMapleTask(cmd []string) {
	start := time.Now().UnixNano() / int64(time.Millisecond)

	application := cmd[1]
	taskNum, _ := strconv.Atoi(cmd[2])
	filenamePrefix := cmd[3]
	inputDir := cmd[4]

	fmt.Println("Starting partitioning")
	// Partition input data (hash partitioning)
	outFiles := map[string]*os.File{}
	for i := 0; i < taskNum; i++ {
		inputFile := path.Join(inputDir, strconv.Itoa(i))
		f, err := os.OpenFile(inputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		outFiles["input"+strconv.Itoa(i)] = f
		if err != nil {
			log.Fatal(err)
		}
	}
	file, err := os.Open(path.Join(inputDir, mj.config.InputFile))
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(file)
	var line string
	lineNum := 1
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		if _, err = outFiles["input"+strconv.Itoa(lineNum%taskNum)].WriteString(line); err != nil {
			log.Fatal(err)
		}
		lineNum++
	}
	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}
	for i := 0; i < taskNum; i++ {
		if err := outFiles["input"+strconv.Itoa(i)].Close(); err != nil {
			log.Fatalln(err)
		}
	}
	if err := file.Close(); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Done partitioning")

	fmt.Println("Start scheduling")
	// Schedule tasks (in turn)
	mj.file_server.put(application, application)
	tasks := map[string]string{}
	it := mj.file_server.table.storage.Iterator()
	for i := 0; i < taskNum; i++ {
		inputFile := path.Join(inputDir, strconv.Itoa(i))
		mj.file_server.put(inputFile, strconv.Itoa(i))
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		tasks[strconv.Itoa(i)] = node.(TableItem).ip
	}
	fmt.Println("Done scheduling")

	fmt.Println("Start RPC")
	// Asynchronous RPC
	port := mj.config.Port
	var calls []RPCTask
	var unfinishedTasks []string
	var failedIP []string
	mapleResults := make([]string, len(tasks))
	cnt := 0
	for inputFile, ip := range tasks {
		client, err := rpc.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Println("Need rescheduling:  ", err)
			unfinishedTasks = append(unfinishedTasks, inputFile)
			failedIP = append(failedIP, ip)
			continue
		}
		args := map[string]string{
			"input":         inputFile,
			"application":   application,
			"output_prefix": filenamePrefix,
		}
		calls = append(calls, RPCTask{inputFile, ip, *client.Go("MJRPCServer.Maple", args, &mapleResults[cnt], nil)})
		cnt++
	}

	// Synchronization
	for _, tmp := range calls {
		replyCall := <-tmp.call.Done
		if replyCall.Error != nil {
			log.Println("Need rescheduling:  ", replyCall.Error)
			unfinishedTasks = append(unfinishedTasks, tmp.fileName)
			failedIP = append(failedIP, tmp.ip)
			continue
		}
	}

	// Reschedule unfinished tasks
	tasks = map[string]string{}
	it = mj.file_server.table.storage.Iterator()
	for i := 0; i < len(unfinishedTasks); i++ {
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		for _, ip := range failedIP {
			if node.(TableItem).ip == ip {
				if it.Next() == false {
					it.First()
				}
				node = it.Value()
			}
		}
		tasks[unfinishedTasks[i]] = node.(TableItem).ip
	}
	var newCalls []rpc.Call
	newResults := make([]string, len(unfinishedTasks))
	cnt = 0
	for inputFile, ip := range tasks {
		client, err := rpc.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Fatal(err)
		}
		args := map[string]string{
			"input":         inputFile,
			"application":   application,
			"output_prefix": filenamePrefix,
		}
		newCalls = append(newCalls, *client.Go("MJRPCServer.Maple", args, &newResults[cnt], nil))
		cnt++
	}

	for _, call := range newCalls {
		replyCall := <-call.Done
		if replyCall.Error != nil {
			log.Fatalln("Reschedule failed: ", replyCall.Error)
		}
	}
	fmt.Println("Done RPC")

	end := time.Now().UnixNano() / int64(time.Millisecond)
	log.Println("Maple elapsed", end - start, "milliseconds.")
}

func (mj *MapleJuiceServer) ScheduleJuiceTask(cmd []string) {
	start := time.Now().UnixNano() / int64(time.Millisecond)

	application := cmd[1]
	taskNum, _ := strconv.Atoi(cmd[2])
	filenamePrefix := cmd[3]
	output := cmd[4]

	fmt.Println("Start searching")
	// Find intermediate files
	files := mj.file_server.table.listFilesByPrefix(filenamePrefix)
	fmt.Println("Done searching")

	fmt.Println("Start scheduling")
	// Schedule tasks (in turn)
	mj.file_server.put(application, application)
	var tasks []map[string]string
	for i := 0; i < taskNum; i++ {
		tasks = append(tasks, map[string]string{})
	}
	it := mj.file_server.table.storage.Iterator()
	for i, filename := range files {
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		tasks[i%taskNum][filename] = node.(TableItem).ip
	}
	fmt.Println("Done scheduling")

	fmt.Println("Start RPC")
	// Asynchronous RPC
	port := mj.config.Port
	var calls []RPCTask
	var unfinishedTasks []string
	var failedIP []string
	juiceResults := make([]string, len(files))
	cnt := 0
	for _, m := range tasks {
		for inputFile, ip := range m {
			client, err := rpc.Dial("tcp", ip+":"+port)
			if err != nil {
				log.Println("Need rescheduling:  ", err)
				unfinishedTasks = append(unfinishedTasks, inputFile)
				failedIP = append(failedIP, ip)
				continue
			}
			args := map[string]string{
				"input":       inputFile,
				"application": application,
			}
			calls = append(calls, RPCTask{inputFile, ip, *client.Go("MJRPCServer.Juice", args, &juiceResults[cnt], nil)})
			cnt++
		}
	}

	// Synchronization
	for _, tmp := range calls {
		replyCall := <-tmp.call.Done
		if replyCall.Error != nil {
			log.Println("Need rescheduling:  ", replyCall.Error)
			unfinishedTasks = append(unfinishedTasks, tmp.fileName)
			failedIP = append(failedIP, tmp.ip)
			continue
		}
	}

	// Reschedule unfinished tasks
	var newTasks []map[string]string
	for i := 0; i < len(unfinishedTasks); i++ {
		newTasks = append(newTasks, map[string]string{})
	}
	it = mj.file_server.table.storage.Iterator()
	for i, filename := range unfinishedTasks {
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		for _, ip := range failedIP {
			if node.(TableItem).ip == ip {
				if it.Next() == false {
					it.First()
				}
				node = it.Value()
			}
		}
		newTasks[i%len(unfinishedTasks)][filename] = node.(TableItem).ip
	}
	var newCalls []rpc.Call
	newResults := make([]string, len(unfinishedTasks))
	cnt = 0
	for _, m := range newTasks {
		for inputFile, ip := range m {
			client, err := rpc.Dial("tcp", ip+":"+port)
			if err != nil {
				log.Fatal(err)
			}
			args := map[string]string{
				"input":       inputFile,
				"application": application,
			}
			newCalls = append(newCalls, *client.Go("MJRPCServer.Juice", args, &newResults[cnt], nil))
			cnt++
		}
	}
	for _, call := range newCalls {
		replyCall := <-call.Done
		if replyCall.Error != nil {
			log.Fatalln("Reschedule failed ", replyCall.Error)
		}
	}
	fmt.Println("Done RPC")

	fmt.Println("Start sorting")
	// Sort results and write to DFS
	var results []string
	for _, s := range juiceResults {
		if s != "" {
			results = append(results, s)
		}
	}
	for _, s := range newResults {
		if s != "" {
			results = append(results, s)
		}
	}
	sortedResults := sets.NewString()
	for _, kvPair := range results {
		sortedResults.Insert(kvPair)
	}
	content := strings.Join(sortedResults.List(), "\n") + "\n"
	mj.file_server.append(content, output)
	fmt.Println("Done sorting")

	// Delete intermediate files
	if len(cmd) == 6 && cmd[5] == "1" {
		for _, file := range files {
			mj.file_server.delete(file)
		}
	}

	end := time.Now().UnixNano() / int64(time.Millisecond)
	log.Println("Juice elapsed", end - start, "milliseconds.")
}

func Run(s *FileServer, mj *MapleJuiceServer) {
	s.daemon.Run()
	for {
		buf := bufio.NewReader(os.Stdin)
		sentence, err := buf.ReadBytes('\n')
		if err != nil {
			fmt.Println(err)
		} else {
			cmd := strings.Split(string(bytes.Trim([]byte(sentence), "\n")), " ")
			fmt.Println("command: " + cmd[0])
			if cmd[0] == "member" {
				fmt.Println(s.daemon.member_list.members)
			} else if cmd[0] == "leave" {
				s.daemon.Leave()
			} else if cmd[0] == "hash" {
				fmt.Println(myHash)
			} else if cmd[0] == "ip" {
				fmt.Println(s.daemon.ip)
			} else if cmd[0] == "id" {
				fmt.Println(s.daemon.id)
			} else if cmd[0] == "put" {
				if len(cmd) == 3 {
					start := time.Now()
					s.temptPut(cmd[1], cmd[2])
					fmt.Println(" time to put file is", time.Since(start))
				}
			} else if cmd[0] == "get" {
				if len(cmd) == 3 {
					start := time.Now()
					s.get(cmd[1], cmd[2])
					fmt.Println(" time to get file is", time.Since(start))
				}
			} else if cmd[0] == "delete" {
				if len(cmd) == 2 {
					start := time.Now()
					s.delete(cmd[1])
					fmt.Println(" time to delete file is", time.Since(start))
				}
			} else if cmd[0] == "store" {
				s.table.listMyFiles()
			} else if cmd[0] == "ls" {
				fmt.Println(s.table.listLocations(cmd[1]))
			} else if cmd[0] == "all" {
				s.table.listAllFiles()
			} else if cmd[0] == "maple" {
				if len(cmd) == 5 {
					go mj.ScheduleMapleTask(cmd)
				}
			} else if cmd[0] == "juice" {
				if len(cmd) == 5 || len(cmd) == 6 {
					go mj.ScheduleJuiceTask(cmd)
				}
			}
		}
	}
}

func main() {
	file_server := NewFileServer()
	go RunRPCServer(&file_server)
	mj_server := NewMapleJuiceServer(&file_server)
	go RunMJRPCServer(&mj_server)
	Run(&file_server, &mj_server)
	<-mainChannel
}
