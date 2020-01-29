#!/bin/bash
git pull
bash clean.sh
go run FileServer.go daemon.go MapleJuice.go main.go