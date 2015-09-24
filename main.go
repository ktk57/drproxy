package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/pubmatic/pub-adserver/go/drproxy/conf"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	// Open the file to for the logs
	outputFile, err := os.Create(conf.Config.Global.ErrorFilePath)
	if err != nil {
		fmt.Printf("\nERROR os.Create() failed for %s with error : %s. Program Exiting.", conf.Config.Global.ErrorFilePath, err.Error())
		os.Exit(1)
	}

	// Set the logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(outputFile)

	// Use all the cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Printf("boot.conf.init.success:\nGOGC=%s\n\n***************Configuration:***************\n%+v\n*****************END****************\n", os.Getenv("GOGC"), conf.Config)

	// For profiling blocking
	//runtime.SetBlockProfileRate(1)
	/*
		s := http.Server{
			Addr: PORT,
			//Handler:        drproxy.New(HEADER, QSIZEPERURL, MAXCONNECTIONS, MINCONNPERHOST, MAXCONNPERHOST, GROWBY, CHANBUFF, time.Millisecond*TIMEOUT),
			Handler:        New(HEADER, QSIZEPERURL, MAXCONNECTIONS, MINCONNPERHOST, MAXCONNPERHOST, GROWBY, CHANBUFF, time.Millisecond*TIMEOUT),
			ReadTimeout:    time.Second * 1,
			WriteTimeout:   time.Second * 1,
			MaxHeaderBytes: 1 << 32,
		}
	*/
	http.Handle("/", New(conf.Config.Global.Header, conf.Config.Global.QSizePerURL, conf.Config.Global.MaxConn, conf.Config.Global.MaxConnPerHost, conf.Config.Global.Chanbuff, time.Millisecond*time.Duration(conf.Config.Global.Timeout), time.Second*time.Duration(conf.Config.Global.LogFrequency)))

	// Sleep to ensure that the consumer go routine is spawned and waiting to serve requests
	time.Sleep(2 * time.Second)

	log.Printf("Starting the server on port %s", conf.Config.Global.Port)
	//log.Fatalln(s.ListenAndServe())
	log.Fatalln(http.ListenAndServe(conf.Config.Global.Port, nil))
}
