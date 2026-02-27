/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.SocketFactory;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.DeleteApiTokenHandler;
import com.arcadedb.server.http.handler.DeleteGroupHandler;
import com.arcadedb.server.http.handler.DeleteUserHandler;
import com.arcadedb.server.http.handler.GetApiDocsHandler;
import com.arcadedb.server.http.handler.GetApiTokensHandler;
import com.arcadedb.server.http.handler.GetDatabasesHandler;
import com.arcadedb.server.http.handler.GetGroupsHandler;
import com.arcadedb.server.http.handler.GetUsersHandler;
import com.arcadedb.server.http.handler.GetDynamicContentHandler;
import com.arcadedb.server.http.handler.GetExistsDatabaseHandler;
import com.arcadedb.server.http.handler.GetOpenApiHandler;
import com.arcadedb.server.http.handler.GetQueryHandler;
import com.arcadedb.server.http.handler.GetReadyHandler;
import com.arcadedb.server.http.handler.GetServerHandler;
import com.arcadedb.server.http.handler.GetSessionsHandler;
import com.arcadedb.server.http.handler.PostApiTokenHandler;
import com.arcadedb.server.http.handler.PostBeginHandler;
import com.arcadedb.server.http.handler.PostGroupHandler;
import com.arcadedb.server.http.handler.PostUserHandler;
import com.arcadedb.server.http.handler.PutUserHandler;
import com.arcadedb.server.http.handler.PostCommandHandler;
import com.arcadedb.server.http.handler.PostCommitHandler;
import com.arcadedb.server.http.handler.PostLoginHandler;
import com.arcadedb.server.http.handler.PostLogoutHandler;
import com.arcadedb.server.http.handler.PostQueryHandler;
import com.arcadedb.server.http.handler.PostRollbackHandler;
import com.arcadedb.server.http.handler.PostServerCommandHandler;
import com.arcadedb.server.http.handler.PostTimeSeriesQueryHandler;
import com.arcadedb.server.http.handler.PostTimeSeriesWriteHandler;
import com.arcadedb.server.http.handler.GetTimeSeriesLatestHandler;
import com.arcadedb.server.http.handler.GetPromQLQueryHandler;
import com.arcadedb.server.http.handler.GetPromQLQueryRangeHandler;
import com.arcadedb.server.http.handler.GetPromQLLabelsHandler;
import com.arcadedb.server.http.handler.GetPromQLLabelValuesHandler;
import com.arcadedb.server.http.handler.GetPromQLSeriesHandler;
import com.arcadedb.server.http.handler.GetGrafanaHealthHandler;
import com.arcadedb.server.http.handler.GetGrafanaMetadataHandler;
import com.arcadedb.server.http.handler.PostGrafanaQueryHandler;
import com.arcadedb.server.http.handler.PostPrometheusWriteHandler;
import com.arcadedb.server.http.handler.PostPrometheusReadHandler;
import com.arcadedb.server.http.ssl.SslUtils;
import com.arcadedb.server.http.ssl.TlsProtocol;
import com.arcadedb.server.http.ws.WebSocketConnectionHandler;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.server.ai.AiActivateHandler;
import com.arcadedb.server.ai.AiChatHandler;
import com.arcadedb.server.ai.AiChatsHandler;
import com.arcadedb.server.ai.AiConfigHandler;
import com.arcadedb.server.ai.ChatStorage;
import com.arcadedb.server.mcp.MCPConfigHandler;
import com.arcadedb.server.mcp.MCPHttpHandler;
import com.arcadedb.server.security.ServerSecurityException;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.PathHandler;
import org.xnio.Options;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;
import java.util.logging.*;

import static com.arcadedb.GlobalConfiguration.NETWORK_SSL_KEYSTORE;
import static com.arcadedb.GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD;
import static com.arcadedb.GlobalConfiguration.NETWORK_SSL_TRUSTSTORE;
import static com.arcadedb.GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD;
import static com.arcadedb.server.http.ssl.KeystoreType.JKS;
import static com.arcadedb.server.http.ssl.KeystoreType.PKCS12;
import static io.undertow.UndertowOptions.SHUTDOWN_TIMEOUT;

public class HttpServer implements ServerPlugin {
  private final    ArcadeDBServer         server;
  private final    HttpSessionManager     sessionManager;
  private final    HttpAuthSessionManager authSessionManager;
  private final    WebSocketEventBus      webSocketEventBus;
  private          Undertow               undertow;
  private volatile String                 listeningAddress;
  private          int                    httpPortListening;

  public HttpServer(final ArcadeDBServer server) {
    this.server = server;
    this.sessionManager = new HttpSessionManager(
        server.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_SESSION_EXPIRE_TIMEOUT) * 1_000L);
    this.authSessionManager = new HttpAuthSessionManager(
        server.getConfiguration().getValueAsLong(GlobalConfiguration.SERVER_HTTP_AUTH_SESSION_EXPIRE_TIMEOUT) * 1_000L,
        server.getConfiguration().getValueAsLong(GlobalConfiguration.SERVER_HTTP_AUTH_SESSION_ABSOLUTE_TIMEOUT) * 1_000L);
    this.webSocketEventBus = new WebSocketEventBus(this.server);
  }

  @Override
  public void stopService() {
    webSocketEventBus.stop();

    if (undertow != null) {
      try {
        undertow.stop();
      } catch (final Exception e) {
        // IGNORE IT
      }
    }

    sessionManager.close();
    authSessionManager.close();
  }

  @Override
  public void startService() {
    final ContextConfiguration configuration = server.getConfiguration();
    final String host = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
    final int[] httpPortRange = extractPortRange(configuration.getValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT));
    final int[] httpsPortRange = getHttpsPortRange(configuration);

    LogManager.instance().log(this, Level.INFO, "- Starting HTTP Server (host=%s port=%s httpsPort=%s)...",
        host, //
        Arrays.toString(httpPortRange), //
        httpsPortRange != null ? Arrays.toString(httpsPortRange) : "-");

    final PathHandler routes = setupRoutes();

    int httpsPortListening = httpsPortRange != null ? httpsPortRange[0] : 0;
    for (httpPortListening = httpPortRange[0]; httpPortListening <= httpPortRange[1]; ++httpPortListening) {
      try {
        undertow = buildUndertowServer(configuration, host, routes, httpsPortListening);
        undertow.start();

        LogManager.instance().log(this, Level.INFO, "- HTTP Server started (host=%s port=%d httpsPort=%s)", host, httpPortListening,
            httpsPortListening > 0 ? httpsPortListening : "-");

        listeningAddress = host.equals("0.0.0.0") ?
            server.getHostAddress() + ":" + httpPortListening :
            host + ":" + httpPortListening;
        return;

      } catch (final Exception e) {
        handleServerStartException(e, httpsPortListening);
      }
    }

    handleServerStartFailure(httpPortRange);
  }

  private int[] getHttpsPortRange(final ContextConfiguration configuration) {
    final Object configuredHTTPSPort = configuration.getValue(GlobalConfiguration.SERVER_HTTPS_INCOMING_PORT);
    return configuredHTTPSPort != null && !configuredHTTPSPort.toString().isEmpty() ? extractPortRange(configuredHTTPSPort) : null;
  }

  private PathHandler setupRoutes() {
    final PathHandler routes = new PathHandler();
    final RoutingHandler basicRoutes = Handlers.routing();

    routes.addPrefixPath("/ws", new WebSocketConnectionHandler(this, webSocketEventBus));
    routes.addPrefixPath("/api/v1", basicRoutes
        .post("/begin/{database}", new PostBeginHandler(this))
        .post("/command/{database}", new PostCommandHandler(this))
        .post("/commit/{database}", new PostCommitHandler(this))
        .get("/databases", new GetDatabasesHandler(this))
        .get("/exists/{database}", new GetExistsDatabaseHandler(this))
        .post("/login", new PostLoginHandler(this))
        .post("/logout", new PostLogoutHandler(this))
        .get("/query/{database}/{language}/{command}", new GetQueryHandler(this))
        .get("/sessions", new GetSessionsHandler(this))
        .post("/query/{database}", new PostQueryHandler(this))
        .post("/rollback/{database}", new PostRollbackHandler(this))
        .get("/server", new GetServerHandler(this))
        .post("/server", new PostServerCommandHandler(this))
        .get("/ready", new GetReadyHandler(this))
        .get("/openapi.json", new GetOpenApiHandler(this))
        .get("/docs", new GetApiDocsHandler(this))
        .get("/server/api-tokens", new GetApiTokensHandler(this))
        .post("/server/api-tokens", new PostApiTokenHandler(this))
        .delete("/server/api-tokens", new DeleteApiTokenHandler(this))
        .get("/server/users", new GetUsersHandler(this))
        .post("/server/users", new PostUserHandler(this))
        .put("/server/users", new PutUserHandler(this))
        .delete("/server/users", new DeleteUserHandler(this))
        .get("/server/groups", new GetGroupsHandler(this))
        .post("/server/groups", new PostGroupHandler(this))
        .delete("/server/groups", new DeleteGroupHandler(this))
        .post("/ts/{database}/write", new PostTimeSeriesWriteHandler(this))
        .post("/ts/{database}/query", new PostTimeSeriesQueryHandler(this))
        .get("/ts/{database}/latest", new GetTimeSeriesLatestHandler(this))
        .get("/ts/{database}/grafana/health", new GetGrafanaHealthHandler(this))
        .get("/ts/{database}/grafana/metadata", new GetGrafanaMetadataHandler(this))
        .post("/ts/{database}/grafana/query", new PostGrafanaQueryHandler(this))
        .post("/ts/{database}/prom/write", new PostPrometheusWriteHandler(this))
        .post("/ts/{database}/prom/read", new PostPrometheusReadHandler(this))
        .get("/ts/{database}/prom/api/v1/query", new GetPromQLQueryHandler(this))
        .get("/ts/{database}/prom/api/v1/query_range", new GetPromQLQueryRangeHandler(this))
        .get("/ts/{database}/prom/api/v1/labels", new GetPromQLLabelsHandler(this))
        .get("/ts/{database}/prom/api/v1/label/{name}/values", new GetPromQLLabelValuesHandler(this))
        .get("/ts/{database}/prom/api/v1/series", new GetPromQLSeriesHandler(this))
    );

    // MCP routes are always registered; the handler checks isEnabled() at request time to support runtime toggling
    final var mcpConfig = server.getMCPConfiguration();
    routes.addExactPath("/api/v1/mcp", new MCPHttpHandler(this, server, mcpConfig));
    routes.addExactPath("/api/v1/mcp/config", new MCPConfigHandler(this, mcpConfig));

    // AI routes are always registered; the chat handler checks isConfigured() at request time
    final var aiConfig = server.getAiConfiguration();
    final var chatStorage = new ChatStorage(server.getRootPath());
    final var aiChatsHandler = new AiChatsHandler(this, chatStorage);
    routes.addPrefixPath("/api/v1/ai", Handlers.routing()//
        .get("/config", new AiConfigHandler(this, aiConfig))//
        .post("/activate", new AiActivateHandler(this, aiConfig))//
        .post("/chat", new AiChatHandler(this, server, aiConfig, chatStorage))//
        .get("/chats", aiChatsHandler)//
        .get("/chats/{id}", aiChatsHandler)//
        .put("/chats/{id}", aiChatsHandler)//
        .delete("/chats/{id}", aiChatsHandler)//
    );

    if (!"production".equals(GlobalConfiguration.SERVER_MODE.getValueAsString())) {
      routes.addPrefixPath("/", Handlers.routing().setFallbackHandler(new GetDynamicContentHandler(this)));
    }

    for (final ServerPlugin plugin : server.getPlugins()) {
      plugin.registerAPI(this, routes);
    }

    return routes;
  }

  private Undertow buildUndertowServer(final ContextConfiguration configuration, final String host, final PathHandler routes,
      int httpsPortListening) throws Exception {
    final long maxEntitySize = configuration.getValueAsLong(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE);
    final Undertow.Builder builder = Undertow.builder()//
        .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
        .setServerOption(UndertowOptions.MAX_ENTITY_SIZE, maxEntitySize)
        .addHttpListener(httpPortListening, host)//
        .setHandler(routes)//
        .setSocketOption(Options.READ_TIMEOUT, configuration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT))
        .setIoThreads(configuration.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_IO_THREADS))//
        .setWorkerThreads(500)//
        .setServerOption(SHUTDOWN_TIMEOUT, 5000);

    if (configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL)) {
      final SSLContext sslContext = createSSLContext();
      builder.addHttpsListener(httpsPortListening, host, sslContext)
          .setServerOption(UndertowOptions.ENABLE_HTTP2, true);
    }

    return builder.build();
  }

  private void handleServerStartException(final Exception e, int httpsPortListening) {
    undertow = null;

    if (e.getCause() instanceof BindException) {
      LogManager.instance().log(this, Level.WARNING, "- HTTP Port %s not available", httpPortListening);
      if (httpsPortListening > 0) {
        ++httpsPortListening;
      }
    } else {
      throw new ServerException("Error on starting HTTP Server", e);
    }
  }

  private void handleServerStartFailure(final int[] httpPortRange) {
    httpPortListening = -1;
    final String msg = "Unable to listen to a HTTP port in the configured port range %d - %d".formatted(httpPortRange[0],
        httpPortRange[1]);
    LogManager.instance().log(this, Level.SEVERE, msg);
    throw new ServerException("Error on starting HTTP Server: " + msg);
  }

  private int[] extractPortRange(final Object configuredPort) {
    int portFrom;
    int portTo;

    if (configuredPort instanceof Number number) {
      portFrom = portTo = number.intValue();
    } else {
      final String[] parts = configuredPort.toString().split("-");
      if (parts.length > 2) {
        throw new IllegalArgumentException("Invalid format for http server port range");
      } else if (parts.length == 1) {
        portFrom = portTo = Integer.parseInt(parts[0]);
      } else {
        portFrom = Integer.parseInt(parts[0]);
        portTo = Integer.parseInt(parts[1]);
      }
    }

    return new int[] { portFrom, portTo };
  }

  public HttpSessionManager getSessionManager() {
    return sessionManager;
  }

  public HttpAuthSessionManager getAuthSessionManager() {
    return authSessionManager;
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public String getListeningAddress() {
    return listeningAddress;
  }

  public int getPort() {
    return httpPortListening;
  }

  public WebSocketEventBus getWebSocketEventBus() {
    return webSocketEventBus;
  }

  private SSLContext createSSLContext() throws Exception {
    ContextConfiguration configuration = server.getConfiguration();

    String keystorePath = validateStoreProperty(configuration, NETWORK_SSL_KEYSTORE, "SSL key store path is empty");
    String keystorePassword = validateStoreProperty(configuration, NETWORK_SSL_KEYSTORE_PASSWORD,
        "SSL key store password is empty");

    String truststorePath = validateStoreProperty(configuration, NETWORK_SSL_TRUSTSTORE, "SSL trust store path is empty");
    String truststorePassword = validateStoreProperty(configuration, NETWORK_SSL_TRUSTSTORE_PASSWORD,
        "SSL trust store password is empty");

    KeyStore keyStore = configureSSLForKeystore(keystorePath, keystorePassword);

    KeyStore trustStore = configureSSLForTruststore(truststorePath, truststorePassword);

    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, keystorePassword.toCharArray());
    KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

    SSLContext sslContext = SSLContext.getInstance(TlsProtocol.getLatestTlsVersion().getTlsVersion());
    sslContext.init(keyManagers, trustManagers, null);

    return sslContext;
  }

  private KeyStore configureSSLForKeystore(String keystorePath, String keystorePassword)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {

    return SslUtils.loadKeystoreFromStream(SocketFactory.getAsStream(keystorePath), keystorePassword,
        SslUtils.getDefaultKeystoreTypeForKeystore(() -> PKCS12));
  }

  private KeyStore configureSSLForTruststore(String truststorePath, String truststorePassword)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {

    return SslUtils.loadKeystoreFromStream(SocketFactory.getAsStream(truststorePath), truststorePassword,
        SslUtils.getDefaultKeystoreTypeForTruststore(() -> JKS));
  }

  private String validateStoreProperty(ContextConfiguration contextConfiguration, GlobalConfiguration configurationKey,
      String errorMessage) {
    String storePropertyValue = contextConfiguration.getValueAsString(configurationKey);
    if ((storePropertyValue == null) || storePropertyValue.isEmpty()) {
      throw new ServerSecurityException(errorMessage);
    }
    return storePropertyValue;
  }

  public boolean isConnected() {
    return undertow != null && listeningAddress != null;
  }
}
