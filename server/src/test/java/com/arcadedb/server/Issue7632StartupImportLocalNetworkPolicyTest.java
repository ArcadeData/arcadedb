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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7632: the {@code import:} startup command of {@code arcadedb.server.defaultDatabases} ran the import as
 * <pre>database.command("sql", "import database " + commandParams)</pre>
 * and that overload builds a <b>fresh, empty</b> {@code ContextConfiguration} for the command, so when
 * {@code ImportDatabaseStatement} resolved the importer's local-network policy from
 * {@code context.getConfiguration()} it found nothing overlaid and read the process-wide static
 * {@link GlobalConfiguration#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS} instead of this server's own answer.
 * <p>
 * That is the divergence #6474 closed for the client-issued {@code import database} verb and #7468 for the startup
 * {@code restore:} command: an operator who set the flag on one server instance's {@code ContextConfiguration} (an
 * embedded or multi-instance deployment) got one answer from that server's {@code IMPORT DATABASE} verb and a
 * different one from the same server's boot-time import. A database opened by the server inherits no
 * {@code ContextConfiguration} at all - {@code DatabaseFactory} is handed a path and nothing else - so the
 * configuration has to travel with the command.
 * <p>
 * Each test here sets the flag on the server's {@code ContextConfiguration} and on the static global to
 * <b>opposite</b> values, so only the per-server answer can satisfy the assertion: a fix that kept reading the
 * global fails both directions. The third test pins the other half of the contract - with no per-server override
 * the global is still what decides, which is what keeps this change invisible to every deployment that never
 * overrode the setting.
 * <p>
 * The source is served over {@code http://127.0.0.1}, not handed over as a {@code file://} path, on purpose:
 * {@code SourceDiscovery} gates only its remote branch on the flag, so a {@code file://} URL would exercise no
 * policy at all and pass against the unfixed code. {@code WITH probeOnly = true} keeps the import to the fetch
 * this test is about - the policy decision happens while opening the source, before a single row is parsed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7632StartupImportLocalNetworkPolicyTest extends StaticBaseServerTest {
  private static final String DB_NAME  = "import7632db";
  private static final String CSV_NAME = "source-7632.csv";
  /**
   * The IPv4 loopback LITERAL. {@code localhost} resolves to {@code ::1} or {@code 127.0.0.1} depending on the
   * machine and the moment, and a fetch dialling a name that lands on the other stack reads as a connection
   * failure rather than as the policy decision this test is about.
   */
  private static final String LOOPBACK = "127.0.0.1";

  private ArcadeDBServer server;
  private HttpServer     sourceServer;
  private boolean        globalDefault;

  @AfterEach
  @Override
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    if (sourceServer != null) {
      sourceServer.stop(0);
      sourceServer = null;
    }
    try {
      super.endTest();
    } finally {
      GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(globalDefault);
    }
  }

  /**
   * Override <b>blocks</b>, global <b>allows</b>: the operator hardened this server instance, so its own boot-time
   * import must refuse a loopback source even though the process-wide default permits it. This is the direction
   * that matters for the SSRF control - against the unfixed code the import read the global, saw "allow" and
   * fetched from loopback anyway.
   */
  @Test
  @Timeout(180)
  void aStartupImportFollowsAPerServerOverrideThatBlocksLocalNetworks() {
    globalDefault = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);

    server = newServer(config -> config.setValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS, true));

    assertThatThrownBy(() -> server.start())
        .as("the per-server override blocks local networks, so this server's own startup import must refuse a "
            + "loopback source even though the static global allows it")
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("blocked request to a non-public or restricted address");

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);
  }

  /**
   * Override <b>allows</b>, global <b>blocks</b>: the operator deliberately permitted local sources for this
   * server, so its own boot-time import must fetch one. Against the unfixed code the import read the global, saw
   * "block" and refused a source the operator had allowed - a startup failure rather than a security hole, but the
   * same single root cause, and pinning only the direction above would leave a fix free to hard-code a refusal.
   */
  @Test
  @Timeout(180)
  void aStartupImportFollowsAPerServerOverrideThatAllowsLocalNetworks() {
    globalDefault = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(true);

    server = newServer(config -> config.setValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS, false));

    assertThatCode(() -> server.start())
        .as("the per-server override allows local networks, so the startup import must have fetched the source")
        .doesNotThrowAnyException();

    assertThat(server.isStarted()).isTrue();
    assertThat(server.existsDatabase(DB_NAME)).isTrue();
  }

  /**
   * No per-server override at all: the static global is still what decides, because
   * {@code ContextConfiguration.getValue(GlobalConfiguration)} falls through to it for every key the server did not
   * override. Without this the fix could have hard-coded an answer and the two tests above would not have noticed.
   */
  @Test
  @Timeout(180)
  void aStartupImportWithNoOverrideStillFollowsTheGlobal() {
    globalDefault = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(true);

    server = newServer(config -> {
      // deliberately no overlay for the setting
    });
    assertThat(server.getConfiguration().hasValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getKey()))
        .as("this test is only meaningful while the server carries no overlay for the setting").isFalse();

    assertThatThrownBy(() -> server.start()).isInstanceOf(SecurityException.class);
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);

    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);
    server = newServer(config -> {
    });
    assertThatCode(() -> server.start()).doesNotThrowAnyException();
    assertThat(server.existsDatabase(DB_NAME)).isTrue();
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  private ArcadeDBServer newServer(final java.util.function.Consumer<ContextConfiguration> customize) {
    if (server != null && server.isStarted())
      server.stop();
    if (sourceServer == null)
      sourceServer = serveSource();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
        DB_NAME + "[root]{import:" + sourceUrl() + " WITH probeOnly = true}");
    customize.accept(config);
    return new ArcadeDBServer(config);
  }

  private String sourceUrl() {
    return "http://" + LOOPBACK + ":" + sourceServer.getAddress().getPort() + "/" + CSV_NAME;
  }

  /** Serves a two-row CSV from the IPv4 loopback literal, on an OS-assigned port. */
  private static HttpServer serveSource() {
    try {
      final byte[] bytes = "id,name\n1,a\n2,b\n".getBytes(StandardCharsets.UTF_8);
      final HttpServer httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
      httpServer.createContext("/" + CSV_NAME, exchange -> {
        exchange.getResponseHeaders().add("Content-Type", "text/csv");
        exchange.sendResponseHeaders(200, bytes.length);
        try (final OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      });
      httpServer.start();
      return httpServer;
    } catch (final IOException e) {
      throw new RuntimeException("Cannot serve the test source", e);
    }
  }
}
