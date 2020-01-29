package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"gopkg.in/yaml.v2"
	"hash/fnv"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

var leaveChannel = make(chan string)
var promptChannel = make(chan string)
var myHash uint32

type ServerConf struct {
	Port string `yaml:"port"`
	Path string `yaml:"path"`
}

func NewServerConf() ServerConf {
	var c ServerConf
	yamlFile, err := ioutil.ReadFile("conf_server.yaml")
	if err != nil {
		log.Fatal("ERROR get yaml file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatal("ERROR unmarshal: %v", err)
	}
	return c
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() % 1000000007
}

type Table struct {
	storage treemap.Map //virtual ring
	server  *FileServer
	lastest map[string]int64
}

func compare(a, b interface{}) int {
	if a.(uint32) < b.(uint32) {
		return -1
	} else if a.(uint32) > b.(uint32) {
		return 1
	} else {
		return 0
	}
}

func NewTable(sev *FileServer) Table {
	var tb Table
	tb.server = sev
	tb.storage = *treemap.NewWith(compare)
	tb.AddToTable(sev.ip)
	tb.lastest = map[string]int64{}
	myHash = hash(sev.ip)
	return tb
}

type TableItem struct {
	ip    string
	files []string
}

func (t *Table) AddToTable(ip string) {
	pos := hash(ip)
	t.storage.Put(pos, TableItem{ip: ip, files: []string{}})
}

func (r RPCServer) PutEntry(sdfs string, success *bool) error {
	return r.file_server.table.PutEntry(sdfs, success)
}

func (t *Table) PutEntry(sdfs string, success *bool) error {
	t.lastest[sdfs] = time.Now().UnixNano()
	floorKey, _ := t.storage.Floor(hash(sdfs))

	if floorKey == nil {
		f, _ := t.storage.Max()
		floorKey = f
	}
	//fmt.Println(hash(sdfs))
	//fmt.Println(floorKey)
	next := floorKey
	for i := 0; i < 4; i++ {
		next, _ = t.storage.Ceiling(next)
		if next == nil {
			f, _ := t.storage.Min()
			next = f
		}
		v, found := t.storage.Get(next)
		if found {
			tmp := v.(TableItem)
			if !contains(tmp.files, sdfs) {
				tmp.files = append(tmp.files, sdfs)
			}
			t.storage.Put(next, tmp)
		}

		next = next.(uint32) + 1
	}
	return nil
}

func (r RPCServer) DelEntry(sdfs string, success *bool) error {
	return r.file_server.table.DelEntry(sdfs, success)
}

func (t *Table) DelEntry(sdfs string, success *bool) error {
	for _, k := range t.storage.Keys() {
		s, found := t.storage.Get(k)
		if found {
			tmp := s.(TableItem)
			for i, file := range tmp.files {
				if file == sdfs {
					tmp.files = append(tmp.files[:i], tmp.files[i+1:]...)
					fmt.Println("File entry for", sdfs, "deleted from", tmp.ip)
				}
			}
			t.storage.Put(k, tmp)
		}
	}
	return nil
}

// search for ips that has file
func (t *Table) search(sdfs string) []string {
	hashVal := hash(sdfs)
	floorKey, _ := t.storage.Floor(hashVal)
	if floorKey == nil {
		f, _ := t.storage.Max()
		floorKey = f
	}
	next := floorKey
	var ips []string
	for i := 0; i < 4; i++ {
		next, _ = t.storage.Ceiling(next)
		if next == nil {
			f, _ := t.storage.Min()
			next = f
		}
		tmp, found := t.storage.Get(next)
		if found {
			ips = append(ips, tmp.(TableItem).ip)
		}
		next = next.(uint32) + 1
	}
	return ips
}

// remove failed nodes from table
func (t *Table) removeFromTable(failed []string) {
	for _, ip := range failed {
		curHash := hash(ip)

		nextAlive := t.findNextAlive(failed, curHash)
		secAlive := t.findNextAlive(failed, nextAlive)
		thrAlive := t.findNextAlive(failed, secAlive)
		forAlive := t.findNextAlive(failed, thrAlive)

		// only nextAlive handles the re-replication
		if myHash == nextAlive {
			//fmt.Println(nextAlive, secAlive, thrAlive, forAlive)
			n0s, _ := t.storage.Get(curHash) // failed node
			n1s, _ := t.storage.Get(nextAlive)
			n2s, _ := t.storage.Get(secAlive)
			n3s, _ := t.storage.Get(thrAlive)
			n4s, _ := t.storage.Get(forAlive)

			i1 := n1s.(TableItem).ip
			i2 := n2s.(TableItem).ip
			i3 := n3s.(TableItem).ip
			i4 := n4s.(TableItem).ip
			//fmt.Println(i1, i2, i3, i4)

			f0 := sets.NewString()
			for _, f := range n0s.(TableItem).files {
				f0.Insert(f)
			}
			f1 := sets.NewString()
			for _, f := range n1s.(TableItem).files {
				f1.Insert(f)
			}
			f2 := sets.NewString()
			for _, f := range n2s.(TableItem).files {
				f2.Insert(f)
			}
			f3 := sets.NewString()
			for _, f := range n3s.(TableItem).files {
				f3.Insert(f)
			}

			tmp1 := f0.Intersection(f1)
			tmp2 := tmp1.Intersection(f2)
			to1 := f0.Difference(f1)
			to2 := tmp1.Difference(f2)
			to3 := tmp2.Difference(f3)
			to4 := tmp2.Intersection(f3)
			//fmt.Println(to1, to2, to3, to4)

			// nextAlive
			var success bool
			for filename := range to1 {
				err := t.server.LocalRep(filename, &success)
				if err != nil {
					log.Println(err)
					continue
				}
			}

			// secAlive
			port := t.server.config.Port
			client, err := rpc.Dial("tcp", i2+":"+port)
			if err != nil {
				log.Println(err)
			} else {
				for filename := range to2 {
					err = client.Call("RPCServer.LocalRep", filename, &success)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}

			// thrAlive
			client, err = rpc.Dial("tcp", i3+":"+port)
			if err != nil {
				log.Println(err)
			} else {
				for filename := range to3 {
					err = client.Call("RPCServer.LocalRep", filename, &success)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}

			// forAlive
			client, err = rpc.Dial("tcp", i4+":"+port)
			if err != nil {
				log.Println(err)
			} else {
				for filename := range to4 {
					err = client.Call("RPCServer.LocalRep", filename, &success)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}

			t.storage.Remove(curHash)

			//fmt.Println(failed)
			// inform each member to update table
			for _, v := range t.storage.Values() {
				p := v.(TableItem).ip
				if !contains(failed, p) {
					if p == i1 {
						_ = t.PutRepEntry(map[uint32][]string{
							nextAlive: to1.List(),
							secAlive:  to2.List(),
							thrAlive:  to3.List(),
							forAlive:  to4.List(),
						}, &success)
					} else {
						client, err := rpc.Dial("tcp", p+":"+port)
						if err != nil {
							log.Println(err)
							continue
						}
						err = client.Call("RPCServer.PutRepEntry", map[uint32][]string{
							nextAlive: to1.List(),
							secAlive:  to2.List(),
							thrAlive:  to3.List(),
							forAlive:  to4.List(),
						}, &success)
					}
				}
			}
		} else {
			t.storage.Remove(curHash)
		}
	}
}

func (r RPCServer) PutRepEntry(args map[uint32][]string, success *bool) error {
	return r.file_server.table.PutRepEntry(args, success)
}

func (t *Table) PutRepEntry(args map[uint32][]string, success *bool) error {
	for k, extend := range args {
		v, found := t.storage.Get(k)
		if found {
			tmp := v.(TableItem)
			tmp.files = append(tmp.files, extend...)
			t.storage.Put(k, tmp)
		}
	}
	return nil
}

func (t *Table) findNextAlive(failed []string, curHash uint32) uint32 {
	var hashed []uint32 // hashed list of failed nodes
	for _, k := range failed {
		hashed = append(hashed, hash(k))
	}
	floorKey, _ := t.storage.Floor(curHash)
	if floorKey == nil {
		f, _ := t.storage.Max()
		floorKey = f
	}
	next := floorKey
	var alive uint32 // first alive node after failed node
	for {
		next = next.(uint32) + 1
		next, _ = t.storage.Ceiling(next)
		if next == nil {
			f, _ := t.storage.Min()
			next = f
		}

		if !containsInt(hashed, next.(uint32)) {
			alive = next.(uint32)
			break
		}
	}
	return alive
}

func (t *Table) listFilesByPrefix(prefix string) []string {
	fileset := sets.NewString()
	for _, s := range t.storage.Values() {
		for _, f := range s.(TableItem).files {
			if strings.HasPrefix(f, prefix) {
				fileset.Insert(f)
			}
		}
	}
	return fileset.UnsortedList()
}

func (t *Table) listAllFiles() {
	for i, s := range t.storage.Values() {
		for _, f := range s.(TableItem).files {
			fmt.Println(i, s.(TableItem).ip, f)
		}
	}
}

func (t *Table) listMyFiles() {
	v, found := t.storage.Get(myHash)
	if found {
		for _, rec := range v.(TableItem).files {
			fmt.Println(rec)
		}
	} else {
		log.Fatal("Failed to list my files")
	}
}

func (t *Table) listLocations(filename string) []string {
	var locations []string
	for _, s := range t.storage.Values() {
		for _, file := range s.(TableItem).files {
			if file == filename {
				locations = append(locations, s.(TableItem).ip)
			}
		}
	}
	return locations
}

type FileServer struct {
	ip     string
	daemon Daemon
	table  Table
	config ServerConf
}

func NewFileServer() FileServer {
	var f FileServer
	f.config = NewServerConf()
	f.ip = FindLocalhostIp()
	if f.ip == "" {
		log.Fatal("ERROR get localhost IP")
	}
	f.table = NewTable(&f)
	f.daemon = NewDaemon(&f.table)
	return f
}

func (r RPCServer) LocalRep(filename string, success *bool) error {
	return r.file_server.LocalRep(filename, success)
}

func (s *FileServer) LocalRep(filename string, success *bool) error {
	var content string
	locations := s.table.listLocations(filename)
	if len(locations) == 0 {
		return errors.New("no replica available")
	} else {
		for _, ip := range locations {
			var buffer []byte
			if ip == s.ip {
				err := s.LocalGet(filename, &buffer)
				if err != nil {
					continue
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
				if err != nil {
					continue
				}
				err = client.Call("RPCServer.LocalGet", filename, &buffer)
				if err != nil {
					continue
				}
			}
			content = string(buffer)
		}
	}
	err := s.LocalPut(map[string]string{"filename": filename, "content": content}, success)
	return err
}

func (r RPCServer) LocalPut(args map[string]string, success *bool) error {
	return r.file_server.LocalPut(args, success)
}

func (s *FileServer) LocalPut(args map[string]string, success *bool) error {
	err := ioutil.WriteFile(s.config.Path+args["filename"], []byte(args["content"]), os.ModePerm)
	return err
}

func (r RPCServer) LocalAppend(args map[string]string, success *bool) error {
	return r.file_server.LocalAppend(args, success)
}

func (s *FileServer) LocalAppend(args map[string]string, success *bool) error {
	f, err := os.OpenFile(s.config.Path+args["filename"], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(args["content"])); err != nil {
		return err
	}
	err = f.Close()
	return err
}

func (s *FileServer) confirm(local string, remote string) {
	buf := bufio.NewReader(os.Stdin)
	go func() {
		time.Sleep(time.Second * 30)
		promptChannel <- "ok"
	}()
	for {
		select {
		case <-promptChannel:
			fmt.Println("Timeout")
			return
		default:
			sentence, err := buf.ReadBytes('\n')
			cmd := strings.Split(string(bytes.Trim([]byte(sentence), "\n")), " ")
			if err == nil && len(cmd) == 1 {
				if cmd[0] == "y" || cmd[0] == "yes" {
					s.put(local, remote)
				} else if cmd[0] == "n" || cmd[0] == "no" {
					return
				}
			}
		}
	}
}

func (s *FileServer) temptPut(local string, remote string) {
	_, ok := s.table.lastest[remote]
	if ok && time.Now().UnixNano()-s.table.lastest[remote] < int64(time.Minute) {
		fmt.Println("Confirm update? (y/n)")
		s.confirm(local, remote)
	} else {
		s.put(local, remote)
	}
}

// local: local file name
// remote: remote file name
func (s *FileServer) put(local string, remote string) {
	target_ips := s.table.search(remote)
	//fmt.Println(target_ips)
	for _, ip := range target_ips {
		content, err := ioutil.ReadFile(local)
		if err != nil {
			fmt.Println("Local file", local, "doesn't exist!")
			return
		} else {
			client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
			if err != nil {
				log.Println(err)
				continue
			}
			var success bool
			err = client.Call("RPCServer.LocalPut", map[string]string{
				"filename": remote,
				"content":  string(content),
			}, &success)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
	var success bool
	err := s.table.PutEntry(remote, &success)
	if err != nil {
		log.Println(err)
	}
	for id, _ := range s.daemon.member_list.members {
		client, err := rpc.Dial("tcp", strings.Split(id, "_")[0]+":"+s.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		err = client.Call("RPCServer.PutEntry", remote, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func containsInt(s []uint32, e uint32) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (r RPCServer) LocalGet(filename string, content *[]byte) error {
	return r.file_server.LocalGet(filename, content)
}

func (s *FileServer) LocalGet(filename string, content *[]byte) error {
	var err error
	*content, err = ioutil.ReadFile(s.config.Path + filename)
	return err
}

func (s *FileServer) get(sdfs string, local string) {
	locations := s.table.listLocations(sdfs)
	if len(locations) == 0 {
		fmt.Println("The file is not available!")
	} else {
		for _, ip := range locations {
			var buffer []byte
			if ip == s.ip {
				err := s.LocalGet(sdfs, &buffer)
				if err != nil {
					continue
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
				if err != nil {
					continue
				}
				err = client.Call("RPCServer.LocalGet", sdfs, &buffer)
				if err != nil {
					continue
				}
			}
			err := ioutil.WriteFile(local, buffer, os.ModePerm)
			if err != nil {
				continue
			}
			break
		}
	}
}

func (r RPCServer) LocalDel(filename string, success *bool) error {
	return r.file_server.LocalDel(filename, success)
}

func (s *FileServer) LocalDel(filename string, success *bool) error {
	err := os.Remove(s.config.Path + filename)
	return err
}

func (s *FileServer) delete(sdfs string) {
	locations := s.table.listLocations(sdfs)
	if len(locations) == 0 {
		fmt.Println("The file is not available!")
	} else {
		//fmt.Println(locations)
		var success bool
		for _, ip := range locations {
			if ip == s.ip {
				err := s.LocalDel(sdfs, &success)
				if err != nil {
					log.Println(err)
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
				if err != nil {
					log.Println(err)
					continue
				}
				err = client.Call("RPCServer.LocalDel", sdfs, &success)
				if err != nil {
					log.Println(err)
					continue
				}
			}
		}
		err := s.table.DelEntry(sdfs, &success)
		if err != nil {
			log.Println(err)
		}
		for id, _ := range s.daemon.member_list.members {
			client, err := rpc.Dial("tcp", strings.Split(id, "_")[0]+":"+s.config.Port)
			if err != nil {
				log.Println(err)
				continue
			}
			err = client.Call("RPCServer.DelEntry", sdfs, &success)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
}

func (s *FileServer) append(content string, remote string) {
	target_ips := s.table.search(remote)
	//fmt.Println(target_ips)
	for _, ip := range target_ips {
		client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		var success bool
		err = client.Call("RPCServer.LocalAppend", map[string]string{
			"filename": remote,
			"content":  content,
		}, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	var success bool
	err := s.table.PutEntry(remote, &success)
	if err != nil {
		log.Println(err)
	}
	for id, _ := range s.daemon.member_list.members {
		client, err := rpc.Dial("tcp", strings.Split(id, "_")[0]+":"+s.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		err = client.Call("RPCServer.PutEntry", remote, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

type RPCServer struct {
	file_server *FileServer
}

func RunRPCServer(file_server *FileServer) {
	server := RPCServer{
		file_server: file_server,
	}
	err := rpc.Register(server)
	if err != nil {
		log.Fatal("Failed to register RPC instance")
	}
	listener, err := net.Listen("tcp", ":"+file_server.config.Port)
	for {
		rpc.Accept(listener)
	}
}
