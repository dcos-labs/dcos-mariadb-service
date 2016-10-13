package com.dcoslabs.mariadb.specification;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.mesos.Protos;

import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.ValueUtils;
import org.apache.mesos.protobuf.DefaultVolumeSpecification;
import org.apache.mesos.specification.*;

public class MariaDbServiceSpecification implements ServiceSpecification {
    private final String NAME;
    private final String ROLE;
    private final String PRINCIPAL;

    public MariaDbServiceSpecification(String name, String role, String principal) {
        NAME = name;
        ROLE = role;
        PRINCIPAL = principal;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<TaskSet> getTaskSets() {
        // @TODO need ContainerInfo
        return Arrays.asList(
            DefaultTaskSet.create(1, "maria-boot", getCommand(), getResources(), Arrays.asList()),
            DefaultTaskSet.create(2, "maria-node", getCommand(), getResources(), Arrays.asList())
        );
    }

    private Protos.CommandInfo getCommand() {
        return Protos.CommandInfo.newBuilder().setValue("echo 'testing 123 and sleeping..' && sleep 120").build();
    }

    private Collection<ResourceSpecification> getResources() {
        return Arrays.asList(
            new DefaultResourceSpecification(
                "cpus",
                ValueUtils.getValue(ResourceUtils.getUnreservedScalar("cpus", 0.5)),
                ROLE,
                PRINCIPAL
            ),
            new DefaultResourceSpecification(
                "mem",
                ValueUtils.getValue(ResourceUtils.getUnreservedScalar("mem", 32.0)),
                ROLE,
                PRINCIPAL
            )
        );
    }

    private Collection<VolumeSpecification> getVolumes() {
        VolumeSpecification volumeSpecification = new DefaultVolumeSpecification(
            500.0,
            VolumeSpecification.Type.ROOT,
            NAME + "-container-path",
            ROLE,
            PRINCIPAL
        );
        return Arrays.asList(volumeSpecification);
    }
}
