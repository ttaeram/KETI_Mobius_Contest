package com.mobius.dt.application.service;

import com.mobius.dt.config.MqttFeederProperties;
import com.mobius.dt.infrastructure.onem2m.Onem2mMqttPublisher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "feeder.mqtt.enabled", havingValue = "true")
public class CsvMqttFeederService {

    // MQTT 피더 동작 관련 설정 값 주입 (CSV 경로, 발행 주기, 루프 여부 등)
    private final MqttFeederProperties props;
    // oneM2M 규격으로 센서 데이터(CIN)를 발행하는 퍼블리셔
    private final Onem2mMqttPublisher publisher;

    @Value
    static class Row {

        Double temp;
        Integer fireAlarm;
        String ts;
    }

    // 센서 CSV에서 파싱한 Row 리스트
    private List<Row> s1 = List.of();
    private List<Row> s2 = List.of();
    private List<Row> s3 = List.of();
    private final AtomicLong tick = new AtomicLong(0);

    // 빈 생성 직후 CSV 파일을 모두 로드
    @PostConstruct
    void loadAll() {

        s1 = loadCsv(props.getS1());
        s2 = loadCsv(props.getS2());
        s3 = loadCsv(props.getS3());
        log.info("CSV loaded: S1={} S2={} S3={}", s1.size(), s2.size(), s3.size());

        if (s1.isEmpty() && s2.isEmpty() && s3.isEmpty()) {

            log.warn("No CSV rows to feed. Check file path");
        }
    }

    // 주어진 파일 경로의 CSV를 읽어 Row 리스트로 변환
    private List<Row> loadCsv(String file) {
        try {
            List<String> lines = Files.readAllLines(Path.of(file))
                    .stream().filter(l -> l != null && !l.isBlank()).collect(Collectors.toList());
            if (lines.isEmpty()) return List.of();

            String[] headers = Arrays.stream(lines.get(0).split(",", -1))
                    .map(s -> s.trim()).toArray(String[]::new);

            int tempIdx = -1, fireIdx = -1, tsIdx = -1;

            for (int i = 0; i < headers.length; i++) {

                String h = headers[i].toLowerCase(Locale.ROOT);
                if (tempIdx < 0 && (h.startsWith("temperature") || h.startsWith("temp"))) tempIdx = i;
                if (fireIdx < 0 && h.equals("fire_alarm")) fireIdx = i;
                if (tsIdx   < 0 && (h.equals("ts") || h.equals("time") || h.equals("timestamp") || h.equals("datetime"))) tsIdx = i;
            }

            if (tempIdx < 0) {

                log.warn("CSV {}: temperature column not found in header={}", file, Arrays.toString(headers));
                return List.of();
            }
            if (fireIdx < 0) {

                log.info("CSV {}: fire_alarm column not found → default 0 will be used.", file);
            }

            List<Row> out = new ArrayList<>();

            for (int i = 1; i < lines.size(); i++) {

                String[] c = Arrays.stream(lines.get(i).split(",", -1))
                        .map(String::trim).toArray(String[]::new);

                Double temperature = null;
                Integer fireAlarm = null;
                String ts = null;

                if (c.length > tempIdx && !c[tempIdx].isBlank()) {

                    try { temperature = Double.parseDouble(c[tempIdx]); } catch (Exception ignore) {}
                }

                if (fireIdx >= 0 && c.length > fireIdx && !c[fireIdx].isBlank()) {

                    try { fireAlarm = Integer.parseInt(c[fireIdx]); }
                    catch (Exception ignore) { fireAlarm = 0; }
                } else {

                    fireAlarm = 0;
                }

                if (tsIdx >= 0 && c.length > tsIdx && !c[tsIdx].isBlank()) ts = c[tsIdx];

                if (temperature != null) {
                    out.add(new Row(temperature, fireAlarm, ts));
                } else {
                    log.warn("Skip malformed line {}: {}", i + 1, lines.get(i));
                }
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
    private int feedOne(int sensorNo, List<Row> rows, long t) {

        if (rows == null || rows.isEmpty()) return 0;
        int idx = (int) (props.isLoop() ? (t % rows.size()) : Math.min(t, rows.size() - 1));
        Row r = rows.get(idx);
        publisher.publishSensorCin(sensorNo, r.getTemp(), r.getFireAlarm(), r.getTs());
        return 1;
    }
}
