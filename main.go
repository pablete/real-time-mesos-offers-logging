package main

import (
	"flag"

	"github.com/golang/protobuf/proto"
	"github.com/mesos/mesos-go/api/v0/mesosproto"
	"github.com/mesos/mesos-go/api/v0/mesosutil"
	"github.com/mesos/mesos-go/api/v0/scheduler"
	"github.com/pablete/real-time-mesos-offers-logging/example_scheduler"

	"os"

	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/pablete/real-time-mesos-offers-logging/server"
	"net"
)

var (
	master   = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	port     = flag.String("port", "9093", "Server port")
	hostname = flag.String("hostname", "172.19.0.1", "IP of the hostname that the scheduler will communicate with")
)

func init() {
	flag.Parse()
}

func main() {
	executorInfo := &mesosproto.ExecutorInfo{
		ExecutorId: mesosutil.NewExecutorID("default"),
		Name:       proto.String("Real Time Offer Viewer"),
		Source:     proto.String("real_time_offers_"),
	}

	//Channel that will receive offers
	offerCh := make(chan mesosproto.Offer)

	//Channel to provoque a shutdown
	q := make(chan bool)

	//Web server
	go server.New(fmt.Sprintf(":%s", *port), *hostname, q, offerCh)

	//Scheduler
	my_scheduler := &example_scheduler.LogScheduler{
		ExecutorInfo: executorInfo,
		WsCh:         offerCh,
	}

	//Framework
	webui := *hostname + ":" + *port
	frameworkInfo := &mesosproto.FrameworkInfo{
		User:     proto.String("root"), // Mesos-go will fill in user.
		Name:     proto.String("Log Offers Scheduler"),
		Hostname: hostname,
		WebuiUrl: &webui,
	}

	//Scheduler Driver
	hostIP := net.ParseIP(*hostname)
	config := scheduler.DriverConfig{
		Scheduler:        my_scheduler,
		Framework:        frameworkInfo,
		Master:           *master,
		Credential:       (*mesosproto.Credential)(nil),
		BindingAddress:   hostIP,
		PublishedAddress: hostIP,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Fatalf("Unable to create a SchedulerDriver: %v\n", err.Error())
		os.Exit(-3)
	}

	if stat, err := driver.Run(); err != nil {
		log.Fatalf("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
		os.Exit(-4)
	}
}
