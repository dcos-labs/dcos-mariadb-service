package com.sanyamkapoor.dcosmariadb;


import com.beust.jcommander.Parameter;

public class CommandLineArgs {
    @Parameter(names = {"--help"}, description="Show this usage prompt", help = true)
    public boolean help;

    // default value of 7 days
    @Parameter(names = {"--failover-timeout"}, description="Failover timeout for framework (in seconds)")
    public int failoverTimeout = 3600 * 7;

    @Parameter(names = {"--zookeeper-url"}, description="Comma-separated list of zookeeper cluster nodes (<host:port>), e.g z1.local:2181,z2.local:2181", required = true)
    public String zookeeperUrl;
}
