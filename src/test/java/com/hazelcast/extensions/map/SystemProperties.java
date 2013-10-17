package com.hazelcast.extensions.map;

public class SystemProperties {

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.multicast.group", "224.3.3.3");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("hazelcast.wait.seconds.before.join", "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }
    
}
