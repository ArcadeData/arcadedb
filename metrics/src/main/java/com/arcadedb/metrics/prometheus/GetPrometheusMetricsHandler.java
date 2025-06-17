package com.arcadedb.metrics.prometheus;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.MimeMappings;

public class GetPrometheusMetricsHandler extends AbstractServerHttpHandler {

  private final PrometheusMeterRegistry registry;
  private final Boolean                 requireAuthentication;

  public GetPrometheusMetricsHandler(HttpServer httpServer, PrometheusMeterRegistry registry, Boolean requireAuthentication) {
    super(httpServer);
    this.registry = registry;
    this.requireAuthentication = requireAuthentication;
  }

  @Override
  public ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, final JSONObject payload) {
    String response = registry.scrape();

    exchange.getResponseHeaders()
        .put(Headers.CONTENT_TYPE, MimeMappings.DEFAULT.getMimeType("txt"));

    return new ExecutionResponse(200, response);
  }

  @Override
  public boolean isRequireAuthentication() {
    return requireAuthentication;
  }
}
