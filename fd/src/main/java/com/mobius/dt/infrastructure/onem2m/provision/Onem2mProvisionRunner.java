package com.mobius.dt.infrastructure.onem2m.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "fd.provision.enabled", havingValue = "true")
public class Onem2mProvisionRunner implements ApplicationRunner {

    private final ResourceLoader loader;
    private final Onem2mAdminClient client;
    private final String yamlPath;

    public Onem2mProvisionRunner(ResourceLoader loader,
                                 Onem2mAdminClient client,
                                 @Value("${fd.provision.yaml}") String yamlPath) {

        this.loader = loader;
        this.client = client;
        this.yamlPath = yamlPath;
    }

    // 애플리케이션 기동 시점에 한 번 실행되어 YAML 플랜을 읽고 AE/컨테이너/구독 구조 생성
    @Override
    public void run(ApplicationArguments args) throws Exception {

        Resource res = loader.getResource(yamlPath);

        if (!res.exists()) throw new IllegalArgumentException("YAML not found: " + yamlPath);

        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        Onem2mProvisionPlan plan = om.readValue(res.getInputStream(), Onem2mProvisionPlan.class);

        client.createAe(plan.ae);

        String aePath = "/" + plan.ae.rn;

        if (plan.tree != null) {

            for (Onem2mProvisionPlan.CntSpec root : plan.tree) {

                ensureCntRecursive(aePath, root);
            }
        }
    }

    // 컨테이너를 생성하고, 하위 컨테이너/구독을 재귀적으로 생성
    private void ensureCntRecursive(String parent, Onem2mProvisionPlan.CntSpec spec) {

        client.createCnt(parent, spec);
        String me = parent + "/" + spec.rn;

        if (spec.subs != null) {

            for (Onem2mProvisionPlan.SubSpec s : spec.subs) client.createSub(me, s);
        }
        if (spec.cnt != null) {

            for (Onem2mProvisionPlan.CntSpec child : spec.cnt) ensureCntRecursive(me, child);
        }
    }
}
