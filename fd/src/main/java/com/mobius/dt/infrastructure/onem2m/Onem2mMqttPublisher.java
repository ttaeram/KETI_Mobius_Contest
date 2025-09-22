package com.mobius.dt.infrastructure.onem2m;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobius.dt.config.MqttFeederProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class Onem2mMqttPublisher {

    private final MqttClient mqttClient;
    private final MqttFeederProperties props;
    private final ObjectMapper om = new ObjectMapper();

    private String reqTopic() {

        // /onem2m/req/<origin>/<cseId>/json
        return "/oneM2M/req/" + props.getOrigin() + "/" + props.getCseId() + "/json";
    }

    private String sensorDataPath(int sensorNo) {

        // /Mobius/<AE>/<Region>/Sensor{n}/data
        return "/Mobius/" + props.getAe() + "/" + props.getRegion() + "/Sensor" + sensorNo + "/data";
    }

    private Map<String, Object> cinPc(Map<String, Object> con) {

        return Map.of("m2m:cin", Map.of("cnf", "application/json", "con", con));
    }

    private Map<String, Object> reqCreateCin(String toPath, Map<String, Object> con) {

        Map<String, Object> req = new LinkedHashMap<>();
        req.put("op", 1);
        req.put("to",  toPath);
        req.put("fr", props.getOrigin());
        req.put("rqi", UUID.randomUUID().toString());
        req.put("rvi", "3");
        req.put("ty", 4);
        req.put("pc", cinPc(con));
        return req;
    }

    public void publishSensorCin(int sensorNo, double temp, Integer fireAlarm, String ts) {

        String to = sensorDataPath(sensorNo);
        String topic = reqTopic();

        Map<String, Object> con = new LinkedHashMap<>();
        con.put("temp", Math.round(temp * 10.0) / 10.0);
        con.put("fire_alarm", fireAlarm != null ? fireAlarm : 0);
        con.put("ts", (ts == null || ts.isBlank()) ? OffsetDateTime.now().toString() : ts);
        con.put("sid", props.getRegion() + "-S" + sensorNo);

        try {
            String payload = om.writeValueAsString(reqCreateCin(to, con));
            MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
            msg.setQos(props.getQos());
            mqttClient.publish(topic, msg);
            log.debug("MQTT -> oneM2M CIN: topic={} to={} con={}", topic, to, con);
        } catch (Exception e) {

            log.warn("MQTT publish failed: {}", e.toString());
        }
    }

    // 센서 데이터 전체 전송 코드
    private Map<String, Object> ensureSidTs(int sensorNo, Map<String, Object> con) {
        Map<String, Object> out = new LinkedHashMap<>(con == null ? Map.of() : con);
        Object sid = out.get("sid");
        if (sid == null || sid.toString().isBlank()) {
            out.put("sid", props.getRegion() + "-S" + sensorNo);
        }
        Object ts = out.get("ts");
        if (ts == null || ts.toString().isBlank()) {
            out.put("ts", OffsetDateTime.now().toString());
        }
        return out;
    }

    private Map<String, Object> cinPcStringified(Map<String, Object> con) throws Exception {
        // 호환성 ↑ : con을 문자열 JSON으로 감싸 전송
        return Map.of("m2m:cin", Map.of("cnf", "application/json", "con", om.writeValueAsString(con)));
    }

    private Map<String, Object> reqCreateCin(String toPath, Map<String, Object> con, boolean stringifyCon) throws Exception {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("op", 1);
        req.put("to",  toPath);
        req.put("fr", props.getOrigin());
        req.put("rqi", UUID.randomUUID().toString());
        req.put("rvi", "3");
        req.put("ty", 4);
        req.put("pc", stringifyCon ? cinPcStringified(con) : cinPc(con));
        return req;
    }

    public void publishSensorCinRaw(int sensorNo, Map<String, Object> con) {
        publishSensorCinRaw(sensorNo, con, true, true);
    }

    public void publishSensorCinRaw(int sensorNo, Map<String, Object> con, boolean ensureSidTs, boolean stringifyCon) {
        String to = sensorDataPath(sensorNo);
        String topic = reqTopic();
        Map<String, Object> finalCon = ensureSidTs ? ensureSidTs(sensorNo, con) : (con == null ? Map.of() : con);

        try {
            String payload = om.writeValueAsString(reqCreateCin(to, finalCon, stringifyCon));
            MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
            msg.setQos(props.getQos());
            mqttClient.publish(topic, msg);
            log.debug("MQTT -> oneM2M CIN(raw): topic={} to={} stringify={} keys={}",
                    topic, to, stringifyCon, finalCon.keySet());
        } catch (Exception e) {
            log.warn("MQTT publish(raw) failed: {}", e.toString());
        }
    }
}
