package main

import (
	"bufio"
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"
)

type MJConf struct {
	Port      string `yaml:"port"`
	FilePath  string `yaml:"file_path"`
	AppPath   string `yaml:"app_path"`
	InputFile string `yaml:"input_file"`
}

func NewMJConf() MJConf {
	var c MJConf
	yamlFile, err := ioutil.ReadFile("conf_maplejuice.yaml")
	if err != nil {
		log.Fatal("ERROR get yaml file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatal("ERROR unmarshal: %v", err)
	}
	return c
}

type MapleJuiceServer struct {
	config      MJConf
	file_server *FileServer
}

func NewMapleJuiceServer(file_server *FileServer) MapleJuiceServer {
	var f MapleJuiceServer
	f.config = NewMJConf()
	f.file_server = file_server
	return f
}

func (s MJRPCServer) Maple(args map[string]string, mapleResult *string) error {
	return s.mjServer.Maple(args, mapleResult)
}

func (s MapleJuiceServer) Maple(args map[string]string, mapleResult *string) error {
	inputFile := args["input"]
	application := args["application"]
	outputPrefix := args["output_prefix"]

	//fmt.Println("Get application from DFS")
	// Get application executable from DFS
	s.file_server.get(application, path.Join(s.config.AppPath, application))

	//fmt.Println("Get input from DFS")
	// Get input file from DFS
	s.file_server.get(inputFile, path.Join(s.config.AppPath, inputFile))

	//fmt.Println("Call maple function")
	// Call maple function (10 lines at a time)
	f, err := os.Open(path.Join(s.config.AppPath, inputFile))
	if err != nil {
		log.Fatalln(err)
	}
	reader := bufio.NewReader(f)
	for {
		var content []string
		for i := 0; i < 10; i++ {
			str, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			content = append(content, str)
		}
		if len(content) == 0 {
			break
		}
		cmd := exec.Command(path.Join(s.config.AppPath, application), strings.Join(content, "\n"))
		ret, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("Application error: ", err)
			return err
		}

		//fmt.Println("Get maple results")
		// Get resulting key-value pairs
		var results []string
		err = json.Unmarshal(ret, &results)
		if err != nil {
			log.Println(err)
		}
		*mapleResult = strings.Join(results, "\n")

		//fmt.Println("Write maple results")
		// Append intermediate result to DFS
		reg, err := regexp.Compile("[^a-zA-Z0-9]+")
		if err != nil {
			log.Fatalln(err)
		}
		for _, pair := range results {
			tmp := strings.Split(pair, ",")
			key := reg.ReplaceAllString(tmp[0], "")
			s.file_server.append(pair + "\n", outputPrefix+"_"+key)
			//time.Sleep(time.Millisecond * 200)
		}
	}
	if err := f.Close(); err != nil {
		log.Fatalln(err)
	}

	return nil
}

func (s MJRPCServer) Juice(args map[string]string, juiceResult *string) error {
	return s.mjServer.Juice(args, juiceResult)
}

func (s MapleJuiceServer) Juice(args map[string]string, juiceResult *string) error {
	inputFile := args["input"]
	application := args["application"]

	//fmt.Println("Get application from DFS")
	// Get application executable from DFS
	s.file_server.get(application, path.Join(s.config.AppPath, application))

	//fmt.Println("Get input from DFS")
	// Get input file from DFS
	s.file_server.get(inputFile, path.Join(s.config.AppPath, inputFile))

	time.Sleep(time.Second)

	//fmt.Println("Call juice function")
	// Call juice function
	var ret []byte
	var err error
	for {
		cmd := exec.Command(path.Join(s.config.AppPath, application), path.Join(s.config.AppPath, inputFile))
		ret, err = cmd.CombinedOutput()
		if err == nil {
			break
		}
		log.Println(err)
	}

	//fmt.Println("Get juice results")
	// Get the resulting key-value pair
	*juiceResult = string(ret)
	return nil
}

type MJRPCServer struct {
	mjServer *MapleJuiceServer
}

func RunMJRPCServer(mjServer *MapleJuiceServer) {
	server := MJRPCServer{
		mjServer: mjServer,
	}
	err := rpc.Register(server)
	if err != nil {
		log.Fatal("Failed to register RPC instance")
	}
	listener, err := net.Listen("tcp", ":"+server.mjServer.config.Port)
	for {
		rpc.Accept(listener)
	}
}