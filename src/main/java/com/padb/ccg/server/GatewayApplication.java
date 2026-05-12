package com.padb.ccg.server;

import com.padb.ccg.auth.AuthProperties;
import com.padb.ccg.proxy.BedrockProperties;
import com.padb.ccg.proxy.RateLimitProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * 网关应用入口，基于 Spring Boot WebFlux。
 * 扫描 {@code com.padb.ccg} 包下的所有组件并启用配置属性绑定。
 */
@SpringBootApplication(scanBasePackages = "com.padb.ccg")
@EnableConfigurationProperties({AuthProperties.class, BedrockProperties.class, RateLimitProperties.class})
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
