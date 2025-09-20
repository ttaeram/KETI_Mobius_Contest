package com.mobius.dt.infrastructure.onem2m;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobius.dt.config.MqttFeederProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class Onem2mHttpPublisher {
    private final MqttFeederProperties props;
    private final ObjectMapper om = new ObjectMapper();

    private WebClient client() {
        HttpClient http = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(10));
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(http))
                .baseUrl(normalizeBase(props.getBaseUrl()))
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    private String normalizeBase(String base) {
        return base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    }

    private String cinCreateUrl(int sensorNo) {
        return String.format("%s/%s/%s/%s/Sensor%d/data?ty=4",
                "",
                props.getAe(),
                props.getRegion(),
                "",
                sensorNo
        ).replace("//", "/");       // 이중 슬래시 정리
    }

    private Map<String, Object> buildCon(int sensorNo, double temp, Integer fireAlarm, String ts) {
        Map<String, Object> con = new LinkedHashMap<>();
        con.put("temp", Math.round(temp * 10.0) / 10.0);
        con.put("fire_alarm", fireAlarm != null ? fireAlarm : 0);
        con.put("ts", (ts == null || ts.isBlank()) ? OffsetDateTime.now().toString() : ts);
        con.put("sid", props.getRegion() + "-S" + sensorNo);
        return con;
    }

    private Map<String, Object> buildBody(Map<String, Object> con) {
        // 호환성 높이기 위해 con은 문자열 JSON으로
        String conStr;
        try {
            conStr = om.writeValueAsString(con);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stringify con", e);
        }
        return Map.of("m2m:cin", Map.of(
                "cnf", "application/json",
                "con", conStr
        ));
    }

    public void publishSensorCin(int sensorNo, double temp, Integer fireAlarm, String ts) {
        String path = cinCreateUrl(sensorNo);
        Map<String, Object> body = buildBody(buildCon(sensorNo, temp, fireAlarm, ts));

        String ri = UUID.randomUUID().toString();
        WebClient.RequestHeadersSpec<?> req = client()
                .post()
                .uri(path)
                .header("X-M2M-Origin", props.getOrigin())
                .header("X-M2M-RI", ri)
                .header("X-M2M-RVI", "3")
                .header(HttpHeaders.CONTENT_TYPE, "application/json;ty=4")
                .bodyValue(body);

        try {
            String resp = req.retrieve()
                    .bodyToMono(String.class)
                    .block(Duration.ofSeconds(10));

            log.info("[HTTP->oneM2M] CIN created sensor={} path={} ri={} resp={}",
                    sensorNo, path, ri, resp);
        } catch (Exception e) {
            log.warn("[HTTP->oneM2M] CIN create FAILED sensor={} path={} ri={} err={}",
                    sensorNo, path, ri, e.toString());
        }
    }
}
