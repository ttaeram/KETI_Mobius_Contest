package com.mobius.dt.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "feeder.mqtt")
public class MqttFeederProperties {

    private boolean enabled = false;

    private String broker = "127.0.0.1";
    private int port = 1883;
    private int qos = 1;

    private String origin = "CAdmin";
    private String cseId = "Mobius";
    private String ae;
    private String region;

    private String s1;
    private String s2;
    private String s3;

    private long rateMs;
    private boolean loop;
}
