package com.mobius.dt.infrastructure.mqtt;

import com.mobius.dt.config.MqttFeederProperties;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
@RequiredArgsConstructor
public class MqttClientFactory {

    private final MqttFeederProperties props;

    // MQTT 클라이언트 빈을 생성
    @Bean(destroyMethod = "disconnect")
    public MqttClient mqttClient() throws Exception {

        String uri = "tcp://" + props.getBroker() + ":" + props.getPort();
        MqttClient cli = new MqttClient(uri, "Meta-Sejong-csv-feeder-" + UUID.randomUUID());
        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);
        opt.setKeepAliveInterval(30);
        cli.connect(opt);
        return cli;
    }
}
