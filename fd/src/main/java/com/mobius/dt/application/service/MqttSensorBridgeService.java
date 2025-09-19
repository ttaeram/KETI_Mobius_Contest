package com.mobius.dt.application.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobius.dt.infrastructure.onem2m.Onem2mClient;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name="mqtt.enabled", havingValue="true")
@Slf4j
public class MqttSensorBridgeService {

    private final IMqttClient client;
    private final Onem2mClient onem2m;
    private final ObjectMapper om = new ObjectMapper();

    @Value("${mqtt.topics.sensor-list}") String sensorTopicsCsv;

    private static final Pattern P_UNDERSCORE =
            Pattern.compile("^Meta-Sejong_(\\d+)_Sensor(\\d+)_data$");

    // 빈 초기화 시점에 토픽 목록을 구독 설정, 각 토픽으로 들어오는 메시지를 handle(...) 콜백으로 처리
    @PostConstruct
    public void init() throws Exception {
        for (String t : sensorTopicsCsv.split("\\s*,\\s*")) {
            if (t.isBlank()) continue;
            client.subscribe(t, (topic, msg) -> handle(topic, new String(msg.getPayload(), StandardCharsets.UTF_8)));
            log.info("Subscribed MQTT: {}", t);
        }
    }

    // MQTT 메시지 처리 로직
    private void handle(String topic, String payload) {
        try {
            // 1) 토픽에서 region/sensor 추출
            String region = null;
            Integer sensor = null;
            var m = P_UNDERSCORE.matcher(topic);
            if (m.matches()) {
                region = m.group(1);
                sensor = Integer.parseInt(m.group(2));
            } else {
                log.debug("Skip: unknown topic format {}", topic);
                return;
            }

            // 2) 페이로드 파싱
            JsonNode root = om.readTree(payload);

            if (root.has("m2m:sgn")) {
                log.debug("Skip notify frame to avoid loop: topic={}", topic);
                return;
            }

            JsonNode con = root; // 평문 JSON
            double temp = con.path("temp").asDouble();
            String ts = con.path("ts").asText(OffsetDateTime.now().toString());
            String sid = con.path("sid").asText("R"+region+"-S"+sensor);

            // 3) oneM2M(Mobius)에 데이터 게시
            onem2m.postSensorData(region, sensor, Map.of("ts", ts, "temp", temp, "sid", sid));
            log.debug("MQTT→Mobius OK: topic={} R{} S{} temp={}", topic, region, sensor, temp);

        } catch (Exception e) {
            log.warn("MQTT→Mobius bridge failed (topic={}): {}", topic, e.toString());
        }
    }
}
