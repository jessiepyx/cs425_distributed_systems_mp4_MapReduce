package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ADDED     = 0
	JOINED    = 1
	SUSPECTED = 2
	LEAVING   = 4
	LEFT      = 5
)

func FindLocalhostIp() string {
	addrs, _ := net.InterfaceAddrs()
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return ""
}

type DaemonConf struct {
	Introducer      string        `yaml:"introducer"`
	Port            string        `yaml:"port"`
	Gossip_interval time.Duration `yaml:"gossip_interval"`
	Suspect_time    int           `yaml:"suspect_time"`
	Fail_time       int           `yaml:"fail_time"`
	Remove_time     int           `yaml:"remove_time"`
}

func NewDaemonConf() DaemonConf {
	var c DaemonConf
	yamlFile, err := ioutil.ReadFile("conf_daemon.yaml")
	if err != nil {
		log.Fatal("ERROR get yaml file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatal("ERROR unmarshal: %v", err)
	}
	return c
}

var mux sync.Mutex

type MemberList struct {
	daemon_ptr *Daemon
	table      *Table
	members    map[string]map[string]int
	fail_nodes []string
}

func NewMemberList(d *Daemon, f *Table) MemberList {
	var m MemberList
	m.daemon_ptr = d
	m.table = f
	m.members = map[string]map[string]int{}
	return m
}

func (m *MemberList) UpdateMembership(message map[string]map[string]int) {
	for id, msg := range message {
		if strings.Split(id, "_")[0] == m.daemon_ptr.config.Introducer {
			_, ok := m.members[m.daemon_ptr.config.Introducer]
			if ok {
				delete(m.members, m.daemon_ptr.config.Introducer)
			}
		}
		if msg["status"] == LEFT {
			_, ok := m.members[id]
			if ok && m.members[id]["status"] != LEFT {
				m.members[id] = map[string]int{
					"heartbeat": msg["heartbeat"],
					"timestamp": int(time.Now().UnixNano() / int64(time.Millisecond)),
					"status":    LEFT,
				}
				log.Println("[member left]", id, ":", m.members[id])
			}
		} else if msg["status"] == JOINED {
			_, ok := m.members[id]
			if !ok || msg["heartbeat"] > m.members[id]["heartbeat"] {
				m.members[id] = map[string]int{
					"heartbeat": msg["heartbeat"],
					"timestamp": int(time.Now().UnixNano() / int64(time.Millisecond)),
					"status":    JOINED,
				}
			}
			if !ok {
				m.table.AddToTable(strings.Split(id, "_")[0])
				log.Println("[member joined]", id, ":", m.members[id])
			}
		} else {
			log.Println("ERROR unknown status")
		}
	}
}

func (m *MemberList) DetectFailure() {
	for id, info := range m.members {
		if info["status"] == JOINED {
			if int(time.Now().UnixNano() / int64(time.Millisecond))-info["timestamp"] > m.daemon_ptr.config.Suspect_time {
				info["status"] = SUSPECTED
				log.Println("[suspected]", id, ":", m.members[id])
			}
		} else if info["status"] == SUSPECTED {
			if int(time.Now().UnixNano() / int64(time.Millisecond))-info["timestamp"] > m.daemon_ptr.config.Fail_time {
				delete(m.members, id)
				m.fail_nodes = append(m.fail_nodes, strings.Split(id, "_")[0])
				if len(m.fail_nodes) == 1 {
					time.AfterFunc(4 * time.Second, func() {
						m.table.removeFromTable(m.fail_nodes)
						m.fail_nodes = m.fail_nodes[:0]
					})
				}
				log.Println("[failed]", id, ":", m.members[id])
			}
		} else if info["status"] == LEFT {
			if int(time.Now().UnixNano() / int64(time.Millisecond))-info["timestamp"] > m.daemon_ptr.config.Remove_time {
				delete(m.members, id)
				m.table.removeFromTable([]string{strings.Split(id, "_")[0]})
				log.Println("[removed]", id, ":", m.members[id])
			}
		}
	}
}

func (m *MemberList) FindGossipDest(status int) string {
	var candidates []string
	for id, info := range m.members {
		if info["status"] == JOINED || info["status"] == status {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s).Intn(len(candidates))
	return strings.Split(candidates[r], "_")[0]
}

type Daemon struct {
	ip                string
	id                string
	heartbeat_counter int
	timestamp         int64
	status            int
	member_list       MemberList
	config            DaemonConf
}

func NewDaemon(filetableptr *Table) Daemon {
	var d Daemon
	d.config = NewDaemonConf()
	d.ip = FindLocalhostIp()
	if d.ip == "" {
		log.Fatal("ERROR get localhost IP")
	}
	t := time.Now().UnixNano() / int64(time.Millisecond)
	d.id = d.ip + "_" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	d.heartbeat_counter = 1
	d.timestamp = t
	d.status = JOINED
	d.member_list = NewMemberList(&d, filetableptr)
	if d.ip != d.config.Introducer {
		id := d.member_list.daemon_ptr.config.Introducer
		d.member_list.members[id] = map[string]int{
			"heartbeat": 0,
			"timestamp": 0,
			"status":    ADDED,
		}
		log.Println("[introducer added]", id, ":", d.member_list.members[id])
	}
	return d
}

func (d *Daemon) Heartbeat(leaving bool) {
	d.heartbeat_counter += 1
	d.timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	if leaving {
		d.status = LEFT
	}
}

func (d *Daemon) MakeMessage(dest string) map[string]map[string]int {
	message := map[string]map[string]int{}
	for id, info := range d.member_list.members {
		if (info["status"] == JOINED || info["status"] == LEFT) && strings.Split(id, "_")[0] != dest {
			message[id] = map[string]int{
				"heartbeat": info["heartbeat"],
				"status":    info["status"],
			}
		}
	}
	message[d.id] = map[string]int{
		"heartbeat": d.heartbeat_counter,
		"status":    d.status,
	}
	return message
}

func (d *Daemon) Gossip() {
	fmt.Println("Start to gossip")
	for {
		mux.Lock()
		d.member_list.DetectFailure()
		if d.status == LEAVING {
			dest := d.member_list.FindGossipDest(JOINED)
			if dest != "" {
				d.Heartbeat(true)
				message := d.MakeMessage(dest)
				conn, err := net.Dial("tcp", dest+":"+d.config.Port)
				if err == nil {
					marshaled, err := json.Marshal(message)
					if err == nil {
						_, _ = conn.Write([]byte(string(marshaled) + "\n"))
					}
					_ = conn.Close()
				}
			}
			break
		} else if d.status == JOINED {
			dest := d.member_list.FindGossipDest(ADDED)
			if dest != "" {
				d.Heartbeat(false)
				message := d.MakeMessage(dest)
				conn, err := net.Dial("tcp", dest+":"+d.config.Port)
				if err == nil {
					marshaled, err := json.Marshal(message)
					if err == nil {
						_, _ = conn.Write([]byte(string(marshaled) + "\n"))
					}
					_ = conn.Close()
				}
			}
		} else {
			log.Println("ERROR unknown status")
		}
		mux.Unlock()
		time.Sleep(d.config.Gossip_interval * time.Millisecond)
	}
	defer func() { leaveChannel <- "OK" }()
}

func (d *Daemon) Listen() {
	fmt.Println("Start to listen")
	ln, err := net.Listen("tcp", ":"+d.config.Port)
	if err != nil {
		log.Fatal(err)
	}
	for ; d.status == JOINED; {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		if message != "" {
			var struct_msg map[string]map[string]int
			err := json.Unmarshal([]byte(message), &struct_msg)
			if err != nil {
				log.Fatal(err)
			}
			mux.Lock()
			d.member_list.UpdateMembership(struct_msg)
			mux.Unlock()
		}
		_ = conn.Close()
	}
}

func (d *Daemon) Leave() {
	d.status = LEAVING
	log.Println("Leaving...")
	<-leaveChannel
	log.Println("Left")
}

func (d *Daemon) Run() {
	go d.Gossip()
	go d.Listen()
	fmt.Println("Daemon running...")
}
