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
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.DeleteDropUserHandler;
import com.arcadedb.server.http.handler.GetDatabasesHandler;
import com.arcadedb.server.http.handler.GetDocumentHandler;
import com.arcadedb.server.http.handler.GetDynamicContentHandler;
import com.arcadedb.server.http.handler.GetExistsDatabaseHandler;
import com.arcadedb.server.http.handler.GetQueryHandler;
import com.arcadedb.server.http.handler.GetReadyHandler;
import com.arcadedb.server.http.handler.GetServerHandler;
import com.arcadedb.server.http.handler.PostBeginHandler;
import com.arcadedb.server.http.handler.PostCloseDatabaseHandler;
import com.arcadedb.server.http.handler.PostCommandHandler;
import com.arcadedb.server.http.handler.PostCommitHandler;
import com.arcadedb.server.http.handler.PostCreateDatabaseHandler;
import com.arcadedb.server.http.handler.PostCreateDocumentHandler;
import com.arcadedb.server.http.handler.PostCreateUserHandler;
import com.arcadedb.server.http.handler.PostDropDatabaseHandler;
import com.arcadedb.server.http.handler.PostOpenDatabaseHandler;
import com.arcadedb.server.http.handler.PostQueryHandler;
import com.arcadedb.server.http.handler.PostRollbackHandler;
import com.arcadedb.server.http.handler.PostServerCommandHandler;
import com.arcadedb.server.http.ws.WebSocketConnectionHandler;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.server.security.ServerSecurityException;
import io.undertow.Handlers;
import io.undertow.Undertow;
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
import java.util.logging.*;

import static io.undertow.UndertowOptions.SHUTDOWN_TIMEOUT;

public class HttpServer implements ServerPlugin {
  private final ArcadeDBServer     server;
  private final HttpSessionManager sessionManager;
  private final JsonSerializer     jsonSerializer = new JsonSerializer();
  private final WebSocketEventBus  webSocketEventBus;
  private       Undertow           undertow;
  private       String             listeningAddress;
  private       String             host;
  private       int                httpPortListening;

  public HttpServer(final ArcadeDBServer server) {
    this.server = server;
    this.sessionManager = new HttpSessionManager(server.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_TX_EXPIRE_TIMEOUT) * 1000L);
    this.webSocketEventBus = new WebSocketEventBus(this.server);
  }

  @Override
  public void stopService() {
    webSocketEventBus.stop();

    if (undertow != null)
      try {
        undertow.stop();
      } catch (final Exception e) {
        // IGNORE IT
      }

    sessionManager.close();
  }

  @Override
  public void startService() {
    final ContextConfiguration configuration = server.getConfiguration();

    host = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);

    final Object configuredHTTPPort = configuration.getValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT);
    final int[] httpPortRange = extractPortRange(configuredHTTPPort);

    final Object configuredHTTPSPort = configuration.getValue(GlobalConfiguration.SERVER_HTTPS_INCOMING_PORT);
    final int[] httpsPortRange = configuredHTTPSPort != null && !configuredHTTPSPort.toString().isEmpty() ? extractPortRange(configuredHTTPSPort) : null;

    LogManager.instance().log(this, Level.INFO, "- Starting HTTP Server (host=%s port=%s httpsPort=%s)...", host, configuredHTTPPort,
        httpsPortRange != null ? configuredHTTPSPort : "-");

    final PathHandler routes = new PathHandler();

    final RoutingHandler basicRoutes = Handlers.routing();

    routes.addPrefixPath("/ws", new WebSocketConnectionHandler(this, webSocketEventBus));

    routes.addPrefixPath("/api/v1",//
        basicRoutes//
            .post("/begin/{database}", new PostBeginHandler(this))//
            .post("/close/{database}", new PostCloseDatabaseHandler(this))//
            .post("/command/{database}", new PostCommandHandler(this))//
            .post("/commit/{database}", new PostCommitHandler(this))//
            .post("/create/{database}", new PostCreateDatabaseHandler(this))//
            .get("/databases", new GetDatabasesHandler(this))//
            .get("/document/{database}/{rid}", new GetDocumentHandler(this))//
            .post("/document/{database}", new PostCreateDocumentHandler(this))//
            .post("/drop/{database}", new PostDropDatabaseHandler(this))//
            .get("/exists/{database}", new GetExistsDatabaseHandler(this))//
            .post("/open/{database}", new PostOpenDatabaseHandler(this))//
            .get("/query/{database}/{language}/{command}", new GetQueryHandler(this))//
            .post("/query/{database}", new PostQueryHandler(this))//
            .post("/rollback/{database}", new PostRollbackHandler(this))//
            .post("/user", new PostCreateUserHandler(this))//
            .delete("/user/{userName}", new DeleteDropUserHandler(this))//
            .get("/server", new GetServerHandler(this))//
            .post("/server", new PostServerCommandHandler(this))//
            .get("/ready", new GetReadyHandler(this))//
    );

    if (!"production".equals(GlobalConfiguration.SERVER_MODE.getValueAsString())) {
      routes.addPrefixPath("/", Handlers.routing().setFallbackHandler(new GetDynamicContentHandler(this)));
    }

    // REGISTER PLUGIN API
    for (final ServerPlugin plugin : server.getPlugins())
      plugin.registerAPI(this, routes);

    int httpsPortListening = httpsPortRange != null ? httpsPortRange[0] : 0;
    for (httpPortListening = httpPortRange[0]; httpPortListening <= httpPortRange[1]; ++httpPortListening) {
      try {
        final Undertow.Builder builder = Undertow.builder()//
            .addHttpListener(httpPortListening, host)//
            .setHandler(routes)//
            .setSocketOption(Options.READ_TIMEOUT, configuration.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT))
            .setServerOption(SHUTDOWN_TIMEOUT, 1000);

        if (configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL)) {
          final SSLContext sslContext = createSSLContext();
          builder.addHttpsListener(httpsPortListening, host, sslContext);
        }

        undertow = builder.build();
        undertow.start();

        LogManager.instance().log(this, Level.INFO, "- HTTP Server started (host=%s port=%d httpsPort=%s)", host, httpPortListening,
            httpsPortListening > 0 ? httpsPortListening : "-");
        listeningAddress = host + ":" + httpPortListening;
        return;

      } catch (final Exception e) {
        undertow = null;

        if (e.getCause() instanceof BindException) {
          // RETRY
          LogManager.instance().log(this, Level.WARNING, "- HTTP Port %s not available", httpPortListening);
          if (httpsPortListening > 0)
            ++httpsPortListening;

          continue;
        }

        throw new ServerException("Error on starting HTTP Server", e);
      }
    }

    httpPortListening = -1;
    final String msg = String.format("Unable to listen to a HTTP port in the configured port range %d - %d", httpPortRange[0], httpPortRange[1]);
    LogManager.instance().

        log(this, Level.SEVERE, msg);
    throw new

        ServerException("Error on starting HTTP Server: " + msg);
  }

  private int[] extractPortRange(final Object configuredPort) {
    int portFrom;
    int portTo;

    if (configuredPort instanceof Number)
      portFrom = portTo = ((Number) configuredPort).intValue();
    else {
      final String[] parts = configuredPort.toString().split("-");
      if (parts.length > 2)
        throw new IllegalArgumentException("Invalid format for http server port range");
      else if (parts.length == 1)
        portFrom = portTo = Integer.parseInt(parts[0]);
      else {
        portFrom = Integer.parseInt(parts[0]);
        portTo = Integer.parseInt(parts[1]);
      }
    }

    return new int[] { portFrom, portTo };
  }

  public HttpSessionManager getSessionManager() {
    return sessionManager;
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public JsonSerializer getJsonSerializer() {
    return jsonSerializer;
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
    final ContextConfiguration configuration = server.getConfiguration();
    final String keyStorePath = configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_KEYSTORE);
    if (keyStorePath == null || keyStorePath.isEmpty())
      throw new ServerSecurityException("SSL key store path is empty");

    final String trustStorePath = configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE);
    if (trustStorePath == null || trustStorePath.isEmpty())
      throw new ServerSecurityException("SSL trust store path is empty");

    final String trustStorePassword = configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD);
    if (trustStorePassword == null || trustStorePassword.isEmpty())
      throw new ServerSecurityException("SSL trust store password is empty");

    final KeyStore keyStore = configureSSL(keyStorePath, trustStorePassword);
    final KeyStore trustStore = configureSSL(trustStorePath, trustStorePassword);

    final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, trustStorePassword.toCharArray());
    final KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

    final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);
    final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

    final SSLContext sslContext;
    sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers, trustManagers, null);

    return sslContext;
  }

  private KeyStore configureSSL(final String path, final String password)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    final InputStream stream = SocketFactory.getAsStream(path);
    if (stream == null)
      throw new RuntimeException("Could not load keystore");

    try (InputStream is = stream) {
      KeyStore loadedKeystore = KeyStore.getInstance("JKS");
      loadedKeystore.load(is, password.toCharArray());
      return loadedKeystore;
    }
  }
}
