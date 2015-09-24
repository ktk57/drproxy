package conf

import (
	"code.google.com/p/gcfg"
	"flag"
	"fmt"
	"log"
)

const (
	confPath = "config.gcfg"
)

// Config contains the values read from the config file at boot time
var Config struct {
	Global struct {
		Port           string
		Header         string
		QSizePerURL    int
		MaxConn        int
		MaxConnPerHost int
		Chanbuff       int
		// TIMEOUT is time in millisecond, this should be greater than adserver
		Timeout int
		// This should be in seconds
		LogFrequency  int
		ErrorFilePath string
	}
}

func init() {
	confFile := flag.String("conf", confPath, "Configuration file path")
	flag.Parse()
	fmt.Println("conffile:", *confFile)
	err := gcfg.ReadFileInto(&Config, *confFile)
	if err != nil {
		log.Panic("ERROR: conf.init:", err.Error())
	}
}
