package com.sanyamkapoor.dcosmariadb;

// utils
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.google.protobuf.TextFormat;

// mesos
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

// logging
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// dcos-commons
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
//import org.apache.mesos.scheduler.TaskKiller;
//import org.apache.mesos.scheduler.DefaultTaskKiller;

public class MainScheduler implements Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // list of objects needed to handle tasks
    private final Reconciler reconciler;
    //private final DefaultTaskKiller taskKiller;
    private final OfferAccepter offerAccepter;

    private Protos.FrameworkID frameworkId;

    // @TODO construct base deployment plans
    public MainScheduler() {
        //taskKiller = new DefaultTaskKiller();
        reconciler = new DefaultReconciler();
        offerAccepter = null;
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
        try {
            logOffers(offers);

            reconciler.reconcile(driver);

            List<Protos.OfferID> acceptedOffers = new ArrayList<Protos.OfferID>();
            if (reconciler.isReconciled()) {
                // @TODO process new offers and jobs
            }

            log.info(String.format("accepted %s/%s offers", acceptedOffers.size(), offers.size()));

            // @TODO housekeeping tasks: decline offers, garbage collect tasks, suppress offers (if needed)
            declineOffers(driver, acceptedOffers, offers);
            //taskKiller.process(driver);
            if (!hasPendingOperations()) {
                suppressOffers(driver);
            }

        } catch (Exception e) {
            log.error("error occurred while processing offers", e);
        }
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

    private void logOffers(List<Protos.Offer> offers) {
        log.info(String.format("received %s offers", offers.size()));
        for (Protos.Offer offer : offers) {
            log.debug("received offer " + TextFormat.shortDebugString(offer));
        }
    }

    private void declineOffers(SchedulerDriver driver, List<Protos.OfferID> acceptedOffers, List<Protos.Offer> offers) {
        log.info(String.format("declining %s offers", offers.size() - acceptedOffers.size()));
        for (Protos.Offer offer : offers) {
            Protos.OfferID offerId = offer.getId();
            if (!acceptedOffers.contains(offerId)) {
                log.debug("declining offer, OfferID: " + offerId.getValue());
                driver.declineOffer(offerId);
            }
        }
    }

    private boolean hasPendingOperations() {
        // @TODO check if plan has pending operations
        return true;
    }

    private void suppressOffers(SchedulerDriver driver) {
        log.info("suppressing offers");
        driver.suppressOffers();
    }

    private void reviveOffers(SchedulerDriver driver) {
        log.info("reviving offers");
        driver.reviveOffers();
    }
}
