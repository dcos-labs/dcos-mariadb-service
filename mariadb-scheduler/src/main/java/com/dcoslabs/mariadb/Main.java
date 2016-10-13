package com.dcoslabs.mariadb;

import com.dcoslabs.mariadb.specification.MariaDbServiceSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.mesos.scheduler.SchedulerUtils;
import org.apache.mesos.specification.DefaultService;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String SERVICE_NAME = "mariadb";
    private static final String ROLE = SchedulerUtils.nameToRole(SERVICE_NAME);
    private static final String PRINCIPAL = SchedulerUtils.nameToPrincipal(SERVICE_NAME);

    private static final int API_PORT = Integer.valueOf(System.getenv("PORT0"));

    public static void main(String[] args) {
        logger.info(String.format("starting %s scheduler with args %s", SERVICE_NAME, args));
        new DefaultService(API_PORT).register(new MariaDbServiceSpecification(SERVICE_NAME, ROLE, PRINCIPAL));
    }
}
