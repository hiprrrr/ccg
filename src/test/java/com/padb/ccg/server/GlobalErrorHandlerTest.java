package com.padb.ccg.server;

import com.padb.ccg.core.exception.GatewayException;
import com.padb.ccg.core.exception.ProviderException;
import com.padb.ccg.core.exception.RateLimitExceededException;
import com.padb.ccg.core.exception.UnauthorizedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeoutException;

class GlobalErrorHandlerTest {

    private GlobalErrorHandler handler;

    @BeforeEach
    void setUp() {
        handler = new GlobalErrorHandler(new ObjectMapper());
    }

    @Test
    void shouldMapUnauthorizedTo403() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var result = handler.handle(exchange, new UnauthorizedException("not allowed"));

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.FORBIDDEN;
    }

    @Test
    void shouldMapRateLimitTo429() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var result = handler.handle(exchange, new RateLimitExceededException("too many"));

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.valueOf(429);
    }

    @Test
    void shouldMapProviderExceptionTo502() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var result = handler.handle(exchange, new ProviderException("downstream error"));

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.BAD_GATEWAY;
    }

    @Test
    void shouldMapTimeoutTo504() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var result = handler.handle(exchange, new TimeoutException("timed out"));

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.GATEWAY_TIMEOUT;
    }

    @Test
    void shouldMapUnknownErrorTo500() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var result = handler.handle(exchange, new RuntimeException("boom"));

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR;
    }

    @Test
    void shouldUnwrapNestedExceptions() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var nested = new RuntimeException(new RuntimeException(new UnauthorizedException("nested auth error")));
        var result = handler.handle(exchange, nested);

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.FORBIDDEN;
    }

    @Test
    void shouldMapCustomGatewayException() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());
        var custom = new GatewayException("custom", 418) {};
        var result = handler.handle(exchange, custom);

        StepVerifier.create(result).verifyComplete();
        assert exchange.getResponse().getStatusCode() == HttpStatus.valueOf(418);
    }
}
