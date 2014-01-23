package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/bitly/nsq/util"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	flagSet = flag.NewFlagSet("nsqadmin", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress = flagSet.String("http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	templateDir = flagSet.String("template-dir", "", "path to templates directory")

	graphiteURL   = flagSet.String("graphite-url", "", "graphite HTTP address")
	proxyGraphite = flagSet.Bool("proxy-graphite", false, "proxy HTTP requests to graphite")

	useStatsdPrefixes = flagSet.Bool("use-statsd-prefixes", true, "expect statsd prefixed keys in graphite (ie: 'stats_counts.')")
	statsdPrefix      = flagSet.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement, must match nsqd)")
	statsdInterval    = flagSet.Duration("statsd-interval", 60*time.Second, "time interval nsqd is configured to push to statsd (must match nsqd)")

	notificationHTTPEndpoint = flag.String("notification-http-endpoint", "", "HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent")

	nsqlookupdHTTPAddresses = util.StringArray{}
	nsqdHTTPAddresses       = util.StringArray{}
)

func init() {
	flagSet.Var(&nsqlookupdHTTPAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flagSet.Var(&nsqdHTTPAddresses, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
}

func main() {
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(util.Version("nsqadmin"))
		return
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	options := NewNSQAdminOptions()
	util.ResolveOptions(options, flagSet, cfg)
	nsqadmin := NewNSQAdmin(options)

	log.Println(util.Version("nsqadmin"))

	nsqadmin.Main()
	<-exitChan
	nsqadmin.Exit()
}
