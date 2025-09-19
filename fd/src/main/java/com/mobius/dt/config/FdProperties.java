package com.mobius.dt.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "fd")
public class FdProperties {

    private Cse cse = new Cse();
    private Ae ae = new Ae();

    @Data public static class Cse {

        private String baseUrl;
        private String origin;
        private int timeoutMs = 2000;
    }

    @Data public static class Ae {

        private String name;
    }
}
