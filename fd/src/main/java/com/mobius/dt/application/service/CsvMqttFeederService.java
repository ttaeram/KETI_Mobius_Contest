package com.mobius.dt.application.service;

import com.mobius.dt.config.MqttFeederProperties;
import com.mobius.dt.infrastructure.onem2m.Onem2mMqttPublisher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "feeder.mqtt.enabled", havingValue = "true")
public class CsvMqttFeederService {

    // MQTT 피더 동작 관련 설정 값 주입 (CSV 경로, 발행 주기, 루프 여부 등)
    private final MqttFeederProperties props;
    // oneM2M 규격으로 센서 데이터(CIN)를 발행하는 퍼블리셔
    private final Onem2mMqttPublisher mqttPublisher;

    // 센서 CSV에서 파싱한 Row 리스트
    private List<Map<String, Object>> s1 = List.of();
    private List<Map<String, Object>> s2 = List.of();
    private List<Map<String, Object>> s3 = List.of();
    private final AtomicLong tick = new AtomicLong(0);

    // 빈 생성 직후 CSV 파일을 모두 로드
    @PostConstruct
    void loadAll() {

        s1 = loadCsvAsMaps(props.getS1());
        s2 = loadCsvAsMaps(props.getS2());
        s3 = loadCsvAsMaps(props.getS3());
        log.info("CSV loaded: S1={} S2={} S3={}", s1.size(), s2.size(), s3.size());

        if (s1.isEmpty() && s2.isEmpty() && s3.isEmpty()) {

            log.warn("No CSV rows to feed. Check file path");
        }
    }

    private static Object parseBestEffort(String s) {

        if (s == null) return null;
        String v = s.trim();
        if (v.isEmpty()) return "";

        try {

            if (v.matches("^-?\\d+$")) return Long.parseLong(v);
            if (v.matches("^-?\\d+(\\.\\d+)?$")) return Double.parseDouble(v);
        } catch (Exception ignore) {}

        if ("true".equalsIgnoreCase(v)) return 1;
        if ("false".equalsIgnoreCase(v)) return 0;
        return v;
    }

    // 주어진 파일 경로의 CSV를 읽어 Row 리스트로 변환
    private List<Map<String, Object>> loadCsvAsMaps(String file) {
        try {
            List<String> lines = Files.readAllLines(Path.of(file))
                    .stream().filter(l -> l != null && !l.isBlank()).toList();
            if (lines.isEmpty()) return List.of();

            String[] headers = Arrays.stream(lines.get(0).split(",", -1))
                    .map(String::trim).toArray(String[]::new);

            List<Map<String, Object>> out = new ArrayList<>();

            for (int i = 1; i < lines.size(); i++) {

                String[] cells = Arrays.stream(lines.get(i).split(",", -1))
                        .map(String::trim).toArray(String[]::new);
                Map<String, Object> row = new LinkedHashMap<>();

                for (int c = 0; c < headers.length; c++) {

                    String key = headers[c];
                    String val = (c < cells.length ? cells[c] : "");
                    row.put(key, parseBestEffort(val));
                }
                out.add(row);
            }

            return out;
        } catch (Exception e) {
            log.warn("CSV load failed {}: {}", file, e.toString());
            return List.of();
        }
    }

    // 고정 주기로 호출되어 각 센서별로 한 건씩 MQTT 발행
    @Scheduled(fixedRateString = "${feeder.mqtt.rate-ms}", initialDelay = 1000)
    public void tick() {

        long t = tick.getAndIncrement();
        int sent = 0;
        sent += feedOne(1, s1, t);
        sent += feedOne(2, s2, t);
        sent += feedOne(3, s3, t);

        if (sent > 0) log.debug("CSV -> on2M2M tick={} sent={}", t, sent);
    }

    // 특정 센서 번호와 해당 CSV Row 리스트에서 현재 틱에 해당하는 레코드를 발행
    private int feedOne(int sensorNo, List<Map<String, Object>> rows, long t) {

        if (rows == null || rows.isEmpty()) return 0;
        int idx = (int) (props.isLoop() ? (t % rows.size()) : Math.min(t, rows.size() - 1));
        Map<String, Object> con = new LinkedHashMap<>(rows.get(idx));

        con.putIfAbsent("sid", props.getRegion() + "-S" + sensorNo);
        con.computeIfAbsent("ts", k -> java.time.OffsetDateTime.now().toString());

        mqttPublisher.publishSensorCinRaw(sensorNo, con);
        return 1;
    }
}
