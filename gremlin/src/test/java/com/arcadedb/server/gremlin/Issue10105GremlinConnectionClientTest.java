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
package com.arcadedb.server.gremlin;

import com.arcadedb.query.RunningQuery;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10105: the client of a Gremlin request ({@code list queries} names it beside the user) was published only by
 * {@link ConcealingWebSocketChannelizer}'s own handler, a TinkerPop implementation detail: a server configured with any
 * other channelizer, which is legal outside production mode, listed every Gremlin statement with no client.
 * <p>
 * The client now travels exactly like the principal does, with the connection's {@link AuthenticatedUser} - the one object
 * TinkerPop itself binds to a connection, whatever channelizer carries it - and the authorizer publishes it beside the
 * user on the thread that submits the evaluation. The channelizer's own publication stays for the default pipeline, where it
 * adds the channel and the port the authenticator cannot see.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10105GremlinConnectionClientTest extends AbstractGremlinServerIT {
  @AfterEach
  void clearContext() {
    GremlinAuthContext.setClient(null);
    GremlinAuthContext.clear();
  }

  private GremlinServerAuthenticator authenticator() {
    final GremlinServerAuthenticator authenticator = new GremlinServerAuthenticator();
    authenticator.setup(Map.of("server", getServer(0)));
    return authenticator;
  }

  private ArcadeGremlinAuthorizer authorizer() {
    final ArcadeGremlinAuthorizer authorizer = new ArcadeGremlinAuthorizer();
    authorizer.setup(Map.of("server", getServer(0)));
    return authorizer;
  }

  private AuthenticatedUser saslLogin(final String address) throws Exception {
    final Authenticator.SaslNegotiator negotiator = authenticator().newSaslNegotiator(InetAddress.getByName(address));
    negotiator.evaluateResponse(("\u0000root\u0000" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    return negotiator.getAuthenticatedUser();
  }

  @Test
  void theUserOfAConnectionCarriesItsClientAndTheAuthorizerPublishesItBesideThePrincipal() throws Exception {
    final AuthenticatedUser user = saslLogin("127.0.0.1");
    assertThat(user.getName()).isEqualTo("root");

    // What the Gremlin Server does for every request, on the thread that then submits the evaluation
    assertThat(GremlinAuthContext.getClient()).isNull();
    authorizer().authorize(user, new Bytecode(), Map.of());

    final RunningQuery.Client client = GremlinAuthContext.getClient();
    assertThat(client).as("published beside the principal, for the pool to carry").isNotNull().isNotEqualTo(RunningQuery.Client.NONE);
    assertThat(client.connectionId()).isNotBlank();
    assertThat(client.address()).contains("127.0.0.1");
    assertThat(GremlinAuthContext.get().getName()).isEqualTo("root");
  }

  @Test
  void twoConnectionsOfOneCredentialAreTwoClients() throws Exception {
    final AuthenticatedUser first = saslLogin("127.0.0.1");
    final AuthenticatedUser second = saslLogin("127.0.0.1");

    authorizer().authorize(first, new Bytecode(), Map.of());
    final RunningQuery.Client firstClient = GremlinAuthContext.getClient();
    authorizer().authorize(second, new Bytecode(), Map.of());
    final RunningQuery.Client secondClient = GremlinAuthContext.getClient();

    assertThat(firstClient.connectionId()).as("a Gremlin Server typically runs with one credential: the user does not tell "
        + "two clients apart, the connection does").isNotEqualTo(secondClient.connectionId());
    assertThat(firstClient.address()).isEqualTo(secondClient.address());
  }

  @Test
  void aRequestOfAnotherUserNeverInheritsTheClientOfThePreviousOne() throws Exception {
    authorizer().authorize(saslLogin("127.0.0.1"), new Bytecode(), Map.of());
    assertThat(GremlinAuthContext.getClient()).isNotNull();

    // A user that was not authenticated here carries no client: what the previous request left on the thread is cleared,
    // not attributed to it
    authorizer().authorize(new AuthenticatedUser("root"), new Bytecode(), Map.of());
    assertThat(GremlinAuthContext.getClient()).isNull();
  }

  @Test
  void theCredentialsOfAnHttpBasicRequestCarryTheAddressTheHandlerReads() throws Exception {
    final AuthenticatedUser user = authenticator().authenticate(
        Map.of("username", "root", "password", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, "address", "10.0.0.7:52100"));

    authorizer().authorize(user, new Bytecode(), Map.of());

    final RunningQuery.Client client = GremlinAuthContext.getClient();
    assertThat(client).isNotNull();
    assertThat(client.address()).isEqualTo("/10.0.0.7:52100");
    assertThat(client.connectionId()).as("HTTP basic has no connection to number: the peer is the identity").isNull();
  }

  @Test
  void theRequestsOfOneHttpConnectionAreOneClientAndAnotherConnectionIsAnother() throws Exception {
    // The HTTP basic handler authenticates every request: the peer (host:port) is what ties the requests of one keep-alive
    // connection together, where a number per authentication would make every request a client of its own
    final Map<String, String> first = Map.of("username", "root", "password", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS,
        "address", "10.0.0.7:52100");
    final Map<String, String> other = Map.of("username", "root", "password", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS,
        "address", "10.0.0.7:52101");

    authorizer().authorize(authenticator().authenticate(first), new Bytecode(), Map.of());
    final RunningQuery.Client firstRequest = GremlinAuthContext.getClient();
    authorizer().authorize(authenticator().authenticate(first), new Bytecode(), Map.of());
    final RunningQuery.Client secondRequest = GremlinAuthContext.getClient();
    authorizer().authorize(authenticator().authenticate(other), new Bytecode(), Map.of());
    final RunningQuery.Client otherConnection = GremlinAuthContext.getClient();

    assertThat(secondRequest).isEqualTo(firstRequest);
    assertThat(otherConnection).isNotEqualTo(firstRequest);
  }

  @Test
  void anIpv6PeerKeepsItsBracketsAndPort() throws Exception {
    final AuthenticatedUser http = authenticator().authenticate(Map.of("username", "root", "password",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, "address", "[::1]:1234"));
    authorizer().authorize(http, new Bytecode(), Map.of());
    assertThat(GremlinAuthContext.getClient().address()).isEqualTo("/[::1]:1234");

    // SASL passes the address only
    authorizer().authorize(saslLogin("::1"), new Bytecode(), Map.of());
    assertThat(GremlinAuthContext.getClient().address()).startsWith("/").contains(":");
  }

  @Test
  void aRequestWhosePeerIsUnknownStillGetsAClientOfItsOwn() throws Exception {
    final AuthenticatedUser user = authenticator().authenticate(
        Map.of("username", "root", "password", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS));

    authorizer().authorize(user, new Bytecode(), Map.of());

    assertThat(GremlinAuthContext.getClient()).isNotEqualTo(RunningQuery.Client.NONE);
    assertThat(GremlinAuthContext.getClient().connectionId()).isNotBlank();
    assertThat(GremlinAuthContext.getClient().address()).isNull();
  }

  @Test
  void anUnknownPeerIsNoAddressRatherThanTheWordNull() throws Exception {
    final Authenticator.SaslNegotiator negotiator = authenticator().newSaslNegotiator(null);
    negotiator.evaluateResponse(("\u0000root\u0000" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));

    authorizer().authorize(negotiator.getAuthenticatedUser(), new Bytecode(), Map.of());

    assertThat(GremlinAuthContext.getClient().address()).isNull();
    assertThat(GremlinAuthContext.getClient().connectionId()).isNotBlank();
  }
}
