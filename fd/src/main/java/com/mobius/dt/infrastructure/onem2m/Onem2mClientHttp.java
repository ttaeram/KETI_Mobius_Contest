package com.mobius.dt.infrastructure.onem2m;

import com.mobius.dt.config.FdProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class Onem2mClientHttp implements Onem2mClient {

    private final WebClient mobiusWebClient;
    private final FdProperties props;

    // oneM2M 헤더 빌더
    private HttpHeaders headers(int ty) {
        HttpHeaders h = new HttpHeaders();
        h.add("X-M2M-Origin", props.getCse().getOrigin());
        h.add("X-M2M-RI", UUID.randomUUID().toString());
        h.setContentType(MediaType.parseMediaType("application/json;ty=" + ty));
        h.setAccept(List.of(MediaType.APPLICATION_JSON));
        return h;
    }

    // AE 루트 경로
    private String aeRoot() {
        String ae = props.getAe().getName();
        return (ae.startsWith("/") ? ae : "/" + ae);
    }

    // 공통 CI 생성
    private void postCin(String path, Map<String, Object> con, String cnf) {
        Map<String, Object> body = Map.of("m2m:cin", Map.of("cnf", cnf, "con", con));
        log.debug("Mobius CIN POST {} body={}", path, body);

        var res = mobiusWebClient.post()
                .uri(path)
                .headers(h -> h.addAll(headers(4)))
                .bodyValue(body)
                .retrieve()
                .toBodilessEntity()
                .block();

        log.info("Mobius CIN OK {} status={} location={}",
                path,
                res.getStatusCode(),
                res.getHeaders().getFirst("Location"));
    }

    // 센서 데이터
    @Override
    public void postSensorData(String region, int sensor, Map<String, Object> con) {
        String path = aeRoot() + "/" + region + "/Sensor" + sensor + "/data?ty=4";
        postCin(path, con, "application/json");
    }
}
