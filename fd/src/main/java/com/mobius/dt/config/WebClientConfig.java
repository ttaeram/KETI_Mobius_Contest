package com.mobius.dt.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

@Configuration
@RequiredArgsConstructor
public class WebClientConfig {

    private final FdProperties props;

    // Mobius(oneM2M CSE) 호출 전용 WebClient 빈
    @Bean
    public WebClient mobiusWebClient() {

        int timeout = props.getCse().getTimeoutMs();
        // Reactor Netty HttpClient 설정: 커넥트 타임아웃 + Read 타임아웃 핸들러
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeout)
                .doOnConnected(c -> c.addHandlerLast(new ReadTimeoutHandler(timeout, TimeUnit.MILLISECONDS)));

        // WebClient 생성: 베이스 URL과 커넥터 적용
        return WebClient.builder()
                .baseUrl(props.getCse().getBaseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
