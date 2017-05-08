package example_scheduler

import (
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/mesos/mesos-go/api/v0/mesosproto"
	"github.com/mesos/mesos-go/api/v0/scheduler"
)

type LogScheduler struct {
	ExecutorInfo *mesosproto.ExecutorInfo
	WsCh         chan mesosproto.Offer
}

//StatusUpdate is called by a running task to provide status information to the
//scheduler.
func (s *LogScheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesosproto.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

// ResourceOffers iterates all offers and sends them through a channel to a
// Dispatcher in server package that will distribute it between connected clients
func (s *LogScheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesosproto.Offer) {
	for _, offer := range offers {
		s.WsCh <- *offer
		driver.DeclineOffer(offer.Id, &mesosproto.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (s *LogScheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesosproto.FrameworkID, masterInfo *mesosproto.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
}

func (s *LogScheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesosproto.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
}

func (s *LogScheduler) Disconnected(scheduler.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
}

func (sched *LogScheduler) OfferRescinded(s scheduler.SchedulerDriver, id *mesosproto.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (sched *LogScheduler) FrameworkMessage(s scheduler.SchedulerDriver, exId *mesosproto.ExecutorID, slvId *mesosproto.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *LogScheduler) SlaveLost(s scheduler.SchedulerDriver, id *mesosproto.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
}

func (sched *LogScheduler) ExecutorLost(s scheduler.SchedulerDriver, exId *mesosproto.ExecutorID, slvId *mesosproto.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", exId.GetValue(), slvId.GetValue(), i)
}

func (sched *LogScheduler) Error(driver scheduler.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}
