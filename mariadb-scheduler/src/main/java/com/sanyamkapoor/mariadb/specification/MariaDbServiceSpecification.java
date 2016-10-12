package com.sanyamkapoor.mariadb.specification;

import java.util.Arrays;
import java.util.List;

import org.apache.mesos.specification.ServiceSpecification;
import org.apache.mesos.specification.TaskSet;

public class MariaDbServiceSpecification implements ServiceSpecification {
    private String name;

    public MariaDbServiceSpecification(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<TaskSet> getTaskSets() {
        // @TODO set tasks here
        return Arrays.asList();
    }
}
