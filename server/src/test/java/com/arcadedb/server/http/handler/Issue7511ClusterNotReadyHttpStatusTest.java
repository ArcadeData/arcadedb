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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.observation.ObservationRegistry;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Methods;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7511 on the HTTP transport: a group or API-token change refused because a peer of the cluster cannot
 * decode the Raft entry it would be replicated as answers {@code 409 Conflict}.
 * <p>
 * The status is the contract, not a cosmetic detail. Falling through to the generic {@code 500} would tell every
 * HTTP client, driver and load balancer that the SERVER broke - inviting a blind retry against another node,
 * where the request is refused identically - about a condition in which nothing was submitted, nothing changed,
 * and the remedy is to finish the rolling upgrade. It pairs with the {@code FAILED_PRECONDITION} the same refusal
 * gets on gRPC, which {@code Issue7511GrpcClusterNotReadyStatusTest} pins in the {@code grpcw} module.
 * <p>
 * The exception is raised inside the {@code ha-raft} module, which the server module cannot depend on, so - as
 * {@code Issue5064CommittedRemotelyHttpStatusTest} does for the same reason - the real
 * {@code AbstractServerHttpHandler} catch chain is driven with a handler whose {@code execute()} throws it.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7511ClusterNotReadyHttpStatusTest {

  private static final String CAPABILITY = "security-groups-entry";
  private static final String LAGGING    = "arcadedb2";
  private static final String REFUSAL    = "Refusing to replicate the group document: peer(s) [" + LAGGING
      + "] have not advertised the '" + CAPABILITY + "' capability. Peer '" + LAGGING
      + "': the capability route answered HTTP 404.";

  private static ClusterCapabilityNotReadyException refusal() {
    return new ClusterCapabilityNotReadyException(REFUSAL, CAPABILITY, List.of(LAGGING));
  }

  @Test
  void aClusterNotReadyRefusalMapsTo409RatherThanAGeneric500() {
    final HandledResponse response = handle(refusal());

    assertThat(response.statusCode)
        .as("a refusal in which nothing was submitted is a conflict with the cluster's state, not a server fault "
            + "(body=%s)", response.body)
        .isEqualTo(409);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Cluster is not ready for this operation");
    assertThat(json.getString("exception")).isEqualTo(ClusterCapabilityNotReadyException.class.getName());
    assertThat(json.getString("detail"))
        .as("the peer holding the cluster back is the only thing an operator can act on")
        .contains("arcadedb2");
  }

  /**
   * Guards the catch ORDER. The refusal extends {@code OperationNotAvailableException}, which extends
   * {@code CommandExecutionException} - so an arm placed after the generic ones would be unreachable and this
   * would answer 500. The two are not the same answer: "this server cannot do this at all" is a permanent
   * property of the deployment, while this clears itself when the last node is upgraded.
   */
  @Test
  void aPlainOperationNotAvailableRefusalIsNotSweptIntoTheSame409() {
    final HandledResponse response = handle(
        new ServerControlPlane.OperationNotAvailableException("High availability is not enabled on this server"));

    assertThat(response.statusCode).isNotEqualTo(409);
  }

  /** The 409 must survive the wrapping a command planner or the auto-commit wrapper puts around it. */
  @Test
  void aWrappedClusterNotReadyRefusalKeepsThe409() {
    final HandledResponse response = handle(new CommandExecutionException("Error on command execution",
        refusal()));

    assertThat(response.statusCode).isEqualTo(409);
    assertThat(new JSONObject(response.body).getString("exception"))
        .isEqualTo(ClusterCapabilityNotReadyException.class.getName());
  }

  /**
   * In {@code production} mode {@code buildErrorBody} conceals {@code detail}, where the refusal's prose lives. A
   * 409 that reaches an operator as a bare "Cluster is not ready for this operation" has told them nothing they
   * can act on, which is the silence #7511 exists to end - so the lagging peer and the capability ride
   * {@code exceptionArgs}, which every mode emits (PR #7555 review).
   * <p>
   * The per-peer REASON is deliberately absent from that field and is asserted absent: it is free-form text built
   * from a probe failure and can carry a host, a port or a JDK exception message, which is the class of content
   * {@code detail} is concealed for in the first place.
   */
  @Test
  void inProductionModeThePeerAndTheCapabilitySurviveInExceptionArgs() {
    final HandledResponse response = handle(refusal(), "production");

    assertThat(response.statusCode).isEqualTo(409);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.has("detail"))
        .as("production conceals the free-form cause chain, as it does for every other error")
        .isFalse();
    assertThat(json.getString("exceptionArgs"))
        .as("and what is left must still name the node holding the cluster back")
        .contains(LAGGING)
        .contains(CAPABILITY);
    assertThat(json.getString("exceptionArgs"))
        .as("but not the probe-failure text, which is exactly what production mode conceals detail for")
        .doesNotContain("404");
  }

  /** The bounded rendering stays bounded: a large cluster cannot turn one error body into a peer dump. */
  @Test
  void theExceptionArgsAreBoundedOnALargeCluster() {
    final List<String> manyPeers = List.of("p1", "p2", "p3", "p4", "p5", "p6", "p7");
    final String args = new ClusterCapabilityNotReadyException(REFUSAL, CAPABILITY, manyPeers).toExceptionArgs();

    assertThat(args).startsWith(CAPABILITY + "|p1,p2,p3,p4,p5");
    assertThat(args).endsWith(",+2 more");
    assertThat(args).doesNotContain("p6").doesNotContain("p7");
  }

  private record HandledResponse(int statusCode, String body) {
  }

  private HandledResponse handle(final RuntimeException toThrow) {
    return handle(toThrow, "development");
  }

  private HandledResponse handle(final RuntimeException toThrow, final String serverMode) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, serverMode);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getObservationRegistry()).thenReturn(ObservationRegistry.create());
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getServerName()).thenReturn("test");

    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);

    final Sender sender = mock(Sender.class);
    final HttpServerExchange exchange = mock(HttpServerExchange.class);
    final int[] statusCode = { 200 };
    when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
      statusCode[0] = invocation.getArgument(0);
      return exchange;
    });
    when(exchange.getStatusCode()).thenAnswer(invocation -> statusCode[0]);
    when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
    when(exchange.getResponseHeaders()).thenReturn(new HeaderMap());
    when(exchange.getRequestMethod()).thenReturn(Methods.POST);
    when(exchange.getRelativePath()).thenReturn("/server/groups");
    when(exchange.getResponseSender()).thenReturn(sender);

    new ThrowingHandler(httpServer, toThrow).handleRequest(exchange);

    final ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(sender).send(body.capture());
    return new HandledResponse(statusCode[0], body.getValue());
  }

  /** Handler whose execute() throws, standing in for a group save refused by the cluster-capability interlock. */
  private static final class ThrowingHandler extends AbstractServerHttpHandler {
    private final RuntimeException toThrow;

    private ThrowingHandler(final HttpServer httpServer, final RuntimeException toThrow) {
      super(httpServer);
      this.toThrow = toThrow;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      throw toThrow;
    }

    @Override
    public boolean isRequireAuthentication() {
      // Skip the Authorization machinery: this test targets the error-mapping catch chain only.
      return false;
    }
  }
}
