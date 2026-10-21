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

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10532: an exact path registered on a {@link PluginPathHandler} is matched before any prefix, so an exact claim on the
 * path of ANOTHER plugin's prefix takes that prefix's root away while it keeps answering everything under it - the half-working
 * state the registrar's refusals (#10385) exist to prevent. The refusal only read the server's mounts: it looked at the prefix
 * register for the server and never at another plugin, and a prefix claim never looked at the exact register at all.
 * <p>
 * One plugin declaring BOTH an exact path and a prefix on one path is a deliberate "the root here, everything below there" pair and
 * stays allowed. The collision test enumerates every cell of (first claim, second claim, same plugin or not) instead of the cases
 * somebody thought of, so a missing cell is a missing row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10532PluginExactAndPrefixClaimsTest {
  private static final HttpHandler UNUSED = exchange -> {
  };

  private final PluginPathHandler paths = serverPaths();
  private       Undertow          undertow;
  private       HttpClient        client;
  private       String            baseUrl;

  @AfterEach
  void stopListener() {
    if (undertow != null)
      undertow.stop();
    if (client != null)
      client.close();
  }

  /** The kind of a claim: an exact path, or a prefix. */
  private enum Kind {
    EXACT, PREFIX
  }

  static Stream<Arguments> matrix() {
    final List<Arguments> cells = new ArrayList<>();
    for (final Kind first : Kind.values())
      for (final Kind second : Kind.values())
        for (final boolean samePlugin : new boolean[] { true, false })
          cells.add(Arguments.of(first, second, samePlugin));
    return cells.stream();
  }

  /**
   * Different methods on every claim, so that a refusal is about the exact-against-prefix precedence and not about a method taken
   * twice: two claims of one kind merge into one entry, as in #10385. Only a pair of different kinds held by different plugins is
   * refused.
   */
  @ParameterizedTest(name = "{0} then {1}, same plugin: {2}")
  @MethodSource("matrix")
  void aClaimIsRefusedOnlyWhenItTakesAnotherPluginsRootAway(final Kind first, final Kind second, final boolean samePlugin) {
    plugin("PluginA", routes -> claim(routes, first, Methods.GET, "/p", say("first")));

    final String secondOwner = samePlugin ? "PluginA" : "PluginB";
    final Runnable declaringTheSecond = () -> plugin(secondOwner, routes -> claim(routes, second, Methods.POST, "/p", say("second")));

    if (first != second && !samePlugin)
      assertThatThrownBy(declaringTheSecond::run).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginA")
          .hasMessageContaining("PluginB")
          .hasMessageContaining("/p");
    else
      declaringTheSecond.run();
  }

  @Test
  void aRefusedClaimLeavesTheFirstPluginsRouteUntouched() throws Exception {
    plugin("PluginA", routes -> routes.prefix(Methods.GET, "/p", say("PLUGIN-A prefix")));

    assertThatThrownBy(() -> plugin("PluginB", routes -> routes.get("/p", say("PLUGIN-B exact"))))
        .isInstanceOf(IllegalStateException.class);

    startListener();
    assertThat(send("GET", "/p").body()).as("the root of the prefix is still the prefix's").isEqualTo("PLUGIN-A prefix");
    assertThat(send("GET", "/p/sub").body()).isEqualTo("PLUGIN-A prefix");
  }

  /** The shipped path of the report: {@code RaftHAPlugin} declares {@code /api/v1/cluster/peer/} as a prefix, with a trailing slash. */
  @Test
  void anExactPathOnTheRaftPeerPrefixIsRefusedWhateverTheTrailingSlash() {
    plugin("RaftHAPlugin", routes -> routes.prefix(Methods.DELETE, "/api/v1/cluster/peer/", say("RAFT delete-peer")));

    assertThatThrownBy(() -> plugin("Other", routes -> routes.exact(Methods.DELETE, "/api/v1/cluster/peer", say("OTHER delete"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Other")
        .hasMessageContaining("RaftHAPlugin");
    assertThatThrownBy(() -> plugin("Other", routes -> routes.exact(Methods.GET, "/api/v1/cluster/peer/", say("OTHER get"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("RaftHAPlugin");
  }

  @Test
  void aPrefixOnAnotherPluginsExactPathIsRefused() {
    plugin("PluginA", routes -> routes.get("/p", say("PLUGIN-A exact")));

    assertThatThrownBy(() -> plugin("PluginB", routes -> routes.prefix(Methods.GET, "/p/", say("PLUGIN-B prefix"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginB")
        .hasMessageContaining("PluginA")
        .hasMessageContaining("/p");
  }

  @Test
  void aRawRegistrationIsJudgedAgainstTheOtherKindToo() {
    paths.setRegisteringPlugin("LegacyPlugin");
    try {
      paths.addExactPath("/legacy", UNUSED);
    } finally {
      paths.setRegisteringPlugin(null);
    }

    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/legacy", say("prefix"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("LegacyPlugin");

    // the plugin that registered the exact path may add the prefix under it
    paths.setRegisteringPlugin("LegacyPlugin");
    try {
      paths.addPrefixPath("/legacy", UNUSED);
    } finally {
      paths.setRegisteringPlugin(null);
    }
  }

  /** The server's own exact path is held like any other: a plugin prefix on it would never answer its own root. */
  @Test
  void aPluginPrefixOnAnExactPathTheServerRegisteredIsRefused() {
    paths.addExactPath("/health-page", UNUSED);

    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/health-page", say("prefix"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginA")
        .hasMessageContaining("the server");
    // and the mirror, which was refused before: an exact path on a prefix the server mounts
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.get("/ws", say("exact"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("the server");
  }

  @Test
  void onePluginDeclaringTheRootAndTheSubtreeKeepsBothAnswers() throws Exception {
    plugin("PluginA", routes -> routes.get("/p", say("root")).prefix(Methods.GET, "/p", say("below")));
    startListener();

    assertThat(send("GET", "/p").body()).isEqualTo("root");
    assertThat(send("GET", "/p/sub").body()).isEqualTo("below");
  }

  private static void claim(final PluginRoutes routes, final Kind kind, final HttpString method, final String path,
      final HttpHandler handler) {
    if (kind == Kind.EXACT)
      routes.exact(method, path, handler);
    else
      routes.prefix(method, path, handler);
  }

  private PluginPathHandler serverPaths() {
    final PluginPathHandler serverPaths = new PluginPathHandler();
    serverPaths.addPrefixPath("/ws", UNUSED);
    serverPaths.addPrefixPath("/api/v1", UNUSED);
    return serverPaths;
  }

  /** Declares a plugin's routes the way {@code HttpServer.setupRoutes()} does: one {@link PluginRoutes} per plugin. */
  private void plugin(final String name, final Consumer<PluginRoutes> registerAPI) {
    paths.setRegisteringPlugin(name);
    try {
      registerAPI.accept(new PluginRoutes(paths));
    } finally {
      paths.setRegisteringPlugin(null);
    }
  }

  private static HttpHandler say(final String body) {
    return exchange -> exchange.getResponseSender().send(body);
  }

  private void startListener() {
    final int port = allocateFreePorts(1)[0];
    undertow = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
    undertow.start();
    baseUrl = "http://127.0.0.1:" + port;
    client = HttpClient.newHttpClient();
  }

  private HttpResponse<String> send(final String method, final String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).method(method, HttpRequest.BodyPublishers.noBody()).build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
