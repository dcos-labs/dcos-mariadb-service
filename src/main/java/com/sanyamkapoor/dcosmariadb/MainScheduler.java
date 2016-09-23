package com.sanyamkapoor.dcosmariadb;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MainScheduler implements Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Protos.FrameworkID frameworkId;

    public MainScheduler() {
        // @TODO construct the pipeline of jobs
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        // save framework ID as instance variable
        this.frameworkId = frameworkId;

        log.info("framework registered, FrameworkID: " + frameworkId.getValue());

        // @TODO handle registration, check old or new tasks
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        log.info("framework reregistered, FrameworkID: " + this.frameworkId.getValue());

        // @TODO handle reregistration, check old or new tasks
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        // @TODO evaluate resource offers against pending jobs
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        log.info("offer rescinded, OfferID: " + offerId.getValue());
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        log.info(String.format("status update received, TaskID: %s; Status: %s; Message: %s",
            status.getTaskId().getValue(), status.getState().toString(), status.getMessage()
        ));

        // @TODO handle task status update
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        log.info(String.format("framework message received, ExecutorID: %s; SlaveID: %s; MessageData: '%s'",
            executorId.getValue(), slaveId.getValue(), Arrays.toString(data)
        ));
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        log.warn("scheduler driver disconnected");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        log.warn("slave lost, SlaveID: " + slaveId.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        log.warn(String.format("framework message received, ExecutorID: %s; SlaveID: %s; Status: '%s'",
            executorId.getValue(), slaveId.getValue(), status
        ));
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        log.error("scheduler driver error: " + message);
    }
}
