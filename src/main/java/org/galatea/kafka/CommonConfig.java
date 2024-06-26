package org.galatea.kafka;

import java.util.UUID;

public class CommonConfig {


    public final static String BOOTSTRAP_SERVER = new StringBuilder().append("localhost").append(":").append("9093").toString();
    public final static String GROUP_ID = "trade_consumer_5";
}
