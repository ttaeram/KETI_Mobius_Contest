package com.mobius.dt.infrastructure.onem2m.provision;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class Onem2mAdminClient {

    private final RestTemplate rt = new RestTemplate();
    private final String baseUrl;
    private final String origin;

    // 설정에서 CSE 접근 정보를 주입
    public Onem2mAdminClient(@Value("${fd.cse.base-url}") String baseUrl,
                             @Value("${fd.cse.origin}") String origin) {

        this.baseUrl = baseUrl;
        this.origin = origin;
    }

    // 공통 헤더 생성
    private HttpHeaders headers(int ty) {

        HttpHeaders h = new HttpHeaders();
        h.set("X-M2M-Origin", origin);
        h.set("X-M2M-RI", UUID.randomUUID().toString());
        h.setContentType(MediaType.parseMediaType("application/json;ty=" + ty));
        h.setAccept(List.of(MediaType.APPLICATION_JSON));
        return h;
    }

    // AE 생성 요청
    public void createAe(Onem2mProvisionPlan.AeSpec ae) {

        Map<String,Object> body = Map.of("m2m:ae", new LinkedHashMap<>() {{

            put("rn", ae.rn);
            put("api", ae.api);
            put("rr", ae.rr != null ? ae.rr : Boolean.TRUE);
            put("poa", ae.poa != null ? ae.poa : List.of());
        }});
        postIgnore409("?ty=2", body, 2);
    }

    // 컨테이너 생성 요청
    public void createCnt(String parentPath, Onem2mProvisionPlan.CntSpec c) {

        Map<String,Object> cnt = new LinkedHashMap<>();
        cnt.put("rn", c.rn);
        if (c.lbl != null) cnt.put("lbl", c.lbl);
        if (c.mni != null) cnt.put("mni", c.mni);
        if (c.mia != null) cnt.put("mia", c.mia);

        Map<String,Object> body = Map.of("m2m:cnt", cnt);
        postIgnore409(parentPath + "?ty=3", body, 3);
    }

    // 구독 생성 요청
    public void createSub(String targetCntPath, Onem2mProvisionPlan.SubSpec s) {

        Map<String,Object> sub = new LinkedHashMap<>();
        sub.put("rn", s.rn);
        if (s.enc != null) sub.put("enc", s.enc);
        if (s.nu  != null) sub.put("nu",  s.nu);
        if (s.nct != null) sub.put("nct", s.nct);

        Map<String,Object> body = Map.of("m2m:sub", sub);
        postIgnore409(targetCntPath + "?ty=23", body, 23);
    }

    // POST 요청 시 이미 존재(409 CONFLICT)는 정상으로 간주하고 무시
    private void postIgnore409(String path, Object body, int ty) {

        try {

            rt.exchange(baseUrl + path, HttpMethod.POST, new HttpEntity<>(body, headers(ty)), String.class);
        } catch (HttpStatusCodeException e) {

            if (e.getStatusCode() != HttpStatus.CONFLICT) throw e;
        }
    }
}
