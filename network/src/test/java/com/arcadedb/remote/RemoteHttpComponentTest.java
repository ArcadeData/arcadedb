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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteHttpComponentTest {

  private TestableRemoteHttpComponent component;

  /**
   * Testable subclass that overrides requestClusterConfiguration() to avoid HTTP calls.
   */
  static class TestableRemoteHttpComponent extends RemoteHttpComponent {
    TestableRemoteHttpComponent(final String server, final int port, final String userName, final String userPassword) {
      this(server, port, userName, userPassword, new ContextConfiguration());
    }

    TestableRemoteHttpComponent(final String server, final int port, final String userName, final String userPassword,
        final ContextConfiguration configuration) {
      super(server, port, userName, userPassword, configuration);
    }

    @Override
    void requestClusterConfiguration() {
      // No-op to avoid HTTP calls during tests
    }
  }

  @BeforeEach
  void setUp() {
    component = new TestableRemoteHttpComponent("localhost", 2480, "root", "test");
  }

  @AfterEach
  void tearDown() {
    component.close();
  }

  @Test
  void getTimeout() {
    assertThat(component.getTimeout()).isGreaterThan(0);
  }

  @Test
  void setTimeout() {
    component.setTimeout(5000);
    assertThat(component.getTimeout()).isEqualTo(5000);
  }

  @Test
  void getUserName() {
    assertThat(component.getUserName()).isEqualTo("root");
  }

  @Test
  void getUserPassword() {
    assertThat(component.getUserPassword()).isEqualTo("test");
  }

  @Test
  void getConnectionStrategy() {
    assertThat(component.getConnectionStrategy()).isEqualTo(RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);
  }

  @Test
  void setConnectionStrategy() {
    component.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
    assertThat(component.getConnectionStrategy()).isEqualTo(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
  }

  @Test
  void getApiVersion() {
    assertThat(component.getApiVersion()).isEqualTo(1);
  }

  @Test
  void setApiVersion() {
    component.setApiVersion(2);
    assertThat(component.getApiVersion()).isEqualTo(2);
  }

  @Test
  void getStats() {
    assertThat(component.getStats()).isNotNull();
  }

  @Test
  void getUrl() {
    final String url = component.getUrl("command");
    assertThat(url).isEqualTo("http://localhost:2480/api/v1/command");
  }

  @Test
  void getRequestPayload() {
    final JSONObject json = new JSONObject().put("key", "value");
    final String payload = component.getRequestPayload(json);
    assertThat(payload).contains("key");
    assertThat(payload).contains("value");
  }

  @Test
  void constructorWithHttpsPrefix() {
    final TestableRemoteHttpComponent https = new TestableRemoteHttpComponent("https://myserver", 2480, "root", "test");
    try {
      assertThat(https.getUrl("command")).startsWith("https://myserver:2480");
    } finally {
      https.close();
    }
  }

  @Test
  void constructorWithHttpPrefix() {
    final TestableRemoteHttpComponent http = new TestableRemoteHttpComponent("http://myserver", 2480, "root", "test");
    try {
      assertThat(http.getUrl("command")).startsWith("http://myserver:2480");
    } finally {
      http.close();
    }
  }

  @Test
  void constructorWithNoPrefix() {
    assertThat(component.getUrl("command")).startsWith("http://localhost:2480");
  }

  @Test
  void setSameServerErrorRetriesWithNull() {
    component.setSameServerErrorRetries(null);
    // Should not throw, sets to 0
  }

  @Test
  void setSameServerErrorRetriesWithNegative() {
    component.setSameServerErrorRetries(-5);
    // Should not throw, sets to 0
  }

  @Test
  void createRequestBuilder() {
    final var builder = component.createRequestBuilder("GET", "http://localhost:2480/api/v1/server");
    assertThat(builder).isNotNull();
  }

  @Test
  void getReplicaServerList() {
    assertThat(component.getReplicaServerList()).isNotNull();
  }

  // manageException tests

  @SuppressWarnings("unchecked")
  private HttpResponse<String> createMockResponse(final int statusCode, final String body) {
    final HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(statusCode);
    when(response.body()).thenReturn(body);
    return response;
  }

  @Test
  void manageExceptionServerIsNotTheLeader() {
    final JSONObject json = new JSONObject();
    json.put("exception", ServerIsNotTheLeaderException.class.getName());
    json.put("detail", "leader.address.com");
    json.put("exceptionArgs", "leader.address.com:2480");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(ServerIsNotTheLeaderException.class);
  }

  @Test
  void manageExceptionRecordNotFound() {
    final JSONObject json = new JSONObject();
    json.put("exception", RecordNotFoundException.class.getName());
    json.put("detail", "Record #1:0 not found");

    final HttpResponse<String> response = createMockResponse(404, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  void manageExceptionQuorumNotReached() {
    final JSONObject json = new JSONObject();
    json.put("exception", QuorumNotReachedException.class.getName());
    json.put("detail", "Quorum not reached");

    final HttpResponse<String> response = createMockResponse(500, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void manageExceptionDuplicatedKey() {
    final JSONObject json = new JSONObject();
    json.put("exception", DuplicatedKeyException.class.getName());
    json.put("detail", "Duplicated key");
    json.put("exceptionArgs", "indexName|keyValue|#1:0");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void manageExceptionConcurrentModification() {
    final JSONObject json = new JSONObject();
    json.put("exception", ConcurrentModificationException.class.getName());
    json.put("detail", "Concurrent modification");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void manageExceptionTransaction() {
    final JSONObject json = new JSONObject();
    json.put("exception", TransactionException.class.getName());
    json.put("detail", "Transaction error");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(TransactionException.class);
  }

  @Test
  void manageExceptionTimeout() {
    final JSONObject json = new JSONObject();
    json.put("exception", TimeoutException.class.getName());
    json.put("detail", "Timeout");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(TimeoutException.class);
  }

  @Test
  void manageExceptionSchema() {
    final JSONObject json = new JSONObject();
    json.put("exception", SchemaException.class.getName());
    json.put("detail", "Schema error");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(SchemaException.class);
  }

  @Test
  void manageExceptionNoSuchElement() {
    final JSONObject json = new JSONObject();
    json.put("exception", NoSuchElementException.class.getName());
    json.put("detail", "No such element");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void manageExceptionSecurity() {
    final JSONObject json = new JSONObject();
    json.put("exception", SecurityException.class.getName());
    json.put("detail", "Access denied");

    final HttpResponse<String> response = createMockResponse(403, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(SecurityException.class);
  }

  @Test
  void manageExceptionServerSecurity() {
    final JSONObject json = new JSONObject();
    json.put("exception", "com.arcadedb.server.security.ServerSecurityException");
    json.put("detail", "Server security error");

    final HttpResponse<String> response = createMockResponse(403, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(SecurityException.class);
  }

  @Test
  void manageExceptionConnectException() {
    final JSONObject json = new JSONObject();
    json.put("exception", ConnectException.class.getName());
    json.put("detail", "Connection refused");

    final HttpResponse<String> response = createMockResponse(500, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void manageExceptionReplication() {
    final JSONObject json = new JSONObject();
    json.put("exception", "com.arcadedb.server.ha.ReplicationException");
    json.put("detail", "Replication error");

    final HttpResponse<String> response = createMockResponse(500, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void manageExceptionGenericException() {
    final JSONObject json = new JSONObject();
    json.put("exception", "com.some.UnknownException");
    json.put("detail", "Something went wrong");

    final HttpResponse<String> response = createMockResponse(500, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
    assertThat(result.getMessage()).contains("UnknownException");
  }

  @Test
  void manageExceptionNullDetail() {
    final JSONObject json = new JSONObject();
    json.put("exception", TransactionException.class.getName());

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).isEqualTo("Unknown");
  }

  @Test
  void manageExceptionEmptyPayload() {
    final HttpResponse<String> response = createMockResponse(500, "");
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
  }

  @Test
  void manageExceptionNullPayload() {
    final HttpResponse<String> response = createMockResponse(500, null);
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
  }

  @Test
  void manageExceptionInvalidJson() {
    final HttpResponse<String> response = createMockResponse(500, "not valid json {{{");
    final Exception result = component.manageException(response, "test");

    // When JSON parsing fails, it returns the parsing exception
    assertThat(result).isNotNull();
  }

  @Test
  void manageExceptionHttp400BadRequest() {
    final JSONObject json = new JSONObject();
    json.put("error", "Bad Request");
    // No exception field

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
    assertThat(result.getMessage()).contains("Bad Request");
  }

  @Test
  void manageExceptionHttp404NotFound() {
    final JSONObject json = new JSONObject();
    json.put("error", "Not Found");

    final HttpResponse<String> response = createMockResponse(404, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
    assertThat(result.getMessage()).contains("Not Found");
  }

  @Test
  void manageExceptionHttp500InternalServerError() {
    final JSONObject json = new JSONObject();
    json.put("error", "Internal error");

    final HttpResponse<String> response = createMockResponse(500, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
    assertThat(result.getMessage()).contains("Internal Server Error");
  }

  @Test
  void manageExceptionEmptyPayloadRetry() {
    final JSONObject json = new JSONObject();
    json.put("error", "Command text is null");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
    assertThat(result.getMessage()).isEqualTo("Empty payload received");
  }

  @Test
  void manageExceptionServerIsNotTheLeaderWithDot() {
    final JSONObject json = new JSONObject();
    json.put("exception", ServerIsNotTheLeaderException.class.getName());
    json.put("detail", "leader.address.2480");
    json.put("exceptionArgs", "leader.address:2480");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(ServerIsNotTheLeaderException.class);
  }

  @Test
  void constructorWithFourArgs() {
    final TestableRemoteHttpComponent fourArg = new TestableRemoteHttpComponent("localhost", 2480, "user", "pass");
    try {
      assertThat(fourArg.getUserName()).isEqualTo("user");
      assertThat(fourArg.getUserPassword()).isEqualTo("pass");
    } finally {
      fourArg.close();
    }
  }

  @Test
  void manageExceptionDuplicatedKeyWithoutArgs() {
    final JSONObject json = new JSONObject();
    json.put("exception", DuplicatedKeyException.class.getName());
    json.put("detail", "Duplicated key");
    // No exceptionArgs

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    // Without exceptionArgs, it falls through to generic handler
    assertThat(result).isInstanceOf(RemoteException.class);
  }

  @Test
  void manageExceptionHttpOtherCode() {
    final JSONObject json = new JSONObject();
    json.put("error", "Unauthorized");

    final HttpResponse<String> response = createMockResponse(401, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(RemoteException.class);
    assertThat(result.getMessage()).contains("HTTP Error");
  }

  @Test
  void manageExceptionServerIsNotTheLeaderNoDot() {
    final JSONObject json = new JSONObject();
    json.put("exception", ServerIsNotTheLeaderException.class.getName());
    json.put("detail", "leaderaddress");
    json.put("exceptionArgs", "leader:2480");

    final HttpResponse<String> response = createMockResponse(400, json.toString());
    final Exception result = component.manageException(response, "test");

    assertThat(result).isInstanceOf(ServerIsNotTheLeaderException.class);
  }

  // Regression test for issue #4372: leaderServer null-read in httpCommand retry path

  /**
   * When reloadClusterConfiguration() returns true but leaderServer is concurrently null
   * (topology change race), httpCommand must not set connectToServer = null and exit early.
   * Instead it falls through to getNextReplicaAddress() and retries with an available replica.
   */
  @Test
  @SuppressWarnings("unchecked")
  void httpCommandRetryWithReplicaWhenLeaderNulledConcurrently() throws Exception {
    // Primary server: closed port — triggers ConnectException (IOException) on iteration 0
    final int closedPort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      closedPort = probe.getLocalPort();
    }

    // Replica server: accepts one connection and returns HTTP/1.1 200 OK with JSON body
    try (final ServerSocket replicaSocket = new ServerSocket(0)) {
      final int replicaPort = replicaSocket.getLocalPort();

      final Thread serverThread = new Thread(() -> {
        try {
          final Socket client = replicaSocket.accept();
          final InputStream in = client.getInputStream();
          // Read until end of HTTP headers (avoid blocking the client's write)
          final byte[] buf = new byte[8192];
          int total = 0;
          while (total < buf.length) {
            final int n = in.read(buf, total, buf.length - total);
            if (n < 0)
              break;
            total += n;
            final String so_far = new String(buf, 0, total, StandardCharsets.ISO_8859_1);
            if (so_far.contains("\r\n\r\n"))
              break;
          }
          final byte[] resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
              .getBytes(StandardCharsets.UTF_8);
          client.getOutputStream().write(resp);
          client.getOutputStream().flush();
          client.close();
        } catch (final Exception ignored) {
          // best-effort cleanup
        }
      });
      serverThread.setDaemon(true);
      serverThread.start();

      // sameServerErrorRetries=2 so maxRetry=2 when leaderIsPreferable=true,
      // giving us one retry iteration after the initial ConnectException.
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 2);

      final TestableRemoteHttpComponent racingComponent = new TestableRemoteHttpComponent(
          "127.0.0.1", closedPort, "root", "test", cfg) {
        @Override
        boolean reloadClusterConfiguration() {
          // Simulate the race: leaderServer is null (never set by the no-op
          // requestClusterConfiguration), but we add the replica so the fixed code
          // can fall through to getNextReplicaAddress() instead of using null.
          try {
            final Field f = RemoteHttpComponent.class.getDeclaredField("replicaServerList");
            f.setAccessible(true);
            ((List<Pair<String, Integer>>) f.get(this)).add(new Pair<>("127.0.0.1", replicaPort));
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
          return true;
        }
      };

      try {
        // With the fix: httpCommand snapshots leaderServer, detects null, falls through
        // to getNextReplicaAddress(), retries with the replica, receives HTTP 200.
        final Object result = racingComponent.httpCommand(
            "GET", null, "server", null, null, null, true, true, null);
        assertThat(result).isNull();
      } finally {
        racingComponent.close();
      }
    }
  }

  // STICKY strategy URL-routing tests — regression for issue #4273

  @Test
  void stickyStrategyUrlUsesPinnedServer() {
    component.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
    component.setStickyTransactionServer(new Pair<>("leader-host", 2480));

    assertThat(component.getUrl("command")).isEqualTo("http://leader-host:2480/api/v1/command");
  }

  @Test
  void stickyStrategyUrlUsesCurrentServerWhenNotPinned() {
    component.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
    // No stickyTransactionServer set — falls back to currentServer (LB hostname)

    assertThat(component.getUrl("command")).isEqualTo("http://localhost:2480/api/v1/command");
  }

  @Test
  void stickyStrategyUrlRevertsAfterClearingPin() {
    component.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
    component.setStickyTransactionServer(new Pair<>("leader-host", 2480));
    assertThat(component.getUrl("command")).contains("leader-host");

    component.setStickyTransactionServer(null);
    assertThat(component.getUrl("command")).isEqualTo("http://localhost:2480/api/v1/command");
  }

  @Test
  void roundRobinStrategyIgnoresStickyServer() {
    component.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);
    component.setStickyTransactionServer(new Pair<>("leader-host", 2480));

    assertThat(component.getUrl("command")).isEqualTo("http://localhost:2480/api/v1/command");
  }
}
