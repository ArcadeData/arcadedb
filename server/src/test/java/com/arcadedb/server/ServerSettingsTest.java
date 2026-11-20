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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.plugin.ServerSettingVetoTestPlugin;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Server and database settings changed while the server runs: the allow-list of {@code set server setting}, a plugin's veto,
 * and what {@code set database setting} re-derives.
 */
class ServerSettingsTest {
  /*
   * Regression test for issue #7296 at the writer the report names.
   * <p>
   * {@code SET SERVER SETTING arcadedb.server.mode prodction} returned 200 and stored {@code "prodction"}, so
   * production mode never engaged: {@code AbstractServerHttpHandler} decides error-detail concealment by comparing
   * the stored value with {@code "production"}, found it different, and served the deployment the DEVELOPMENT
   * behaviour. The allow-list that exists to refuse exactly that value was enforced on the configuration-file
   * writer and on none of the administrative ones.
   * <p>
   * Asserted through {@link ServerControlPlane#applySetting} rather than over HTTP because that IS the shared
   * writer: since the control-plane refactor the HTTP {@code set server setting} command and the gRPC
   * {@code SetServerSetting} RPC both land here, so one test covers both transports. The
   * {@code IllegalArgumentException} it now throws is what the HTTP layer already maps to 400 and gRPC to
   * {@code INVALID_ARGUMENT}, the same way a bad Boolean has been handled since #7124.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7296 set server setting honours its allow-list
  @Test
  void anUnlistedServerModeIsRefusedInsteadOfStoredVerbatim() {
    final ContextConfiguration configuration = new ContextConfiguration();

    assertThatThrownBy(() -> ServerControlPlane.applySetting(configuration, GlobalConfiguration.SERVER_MODE.getKey(),
        "prodction")).isInstanceOf(IllegalArgumentException.class);

    assertThat(configuration.hasValue(GlobalConfiguration.SERVER_MODE.getKey()))
        .as("a refused value must not reach the overlay at all")
        .isFalse();
  }

  // Regression: #7296 set server setting honours its allow-list
  @Test
  void aListedServerModeIsStillStoredAndNormalised() {
    final ContextConfiguration configuration = new ContextConfiguration();

    ServerControlPlane.applySetting(configuration, GlobalConfiguration.SERVER_MODE.getKey(), "PRODUCTION");

    assertThat(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE))
        .as("every reader compares the stored value with \"production\"")
        .isEqualTo("production");
  }

  /** The other allow-listed server settings the same writer reaches. */
  // Regression: #7296 set server setting honours its allow-list
  @Test
  void theOtherAllowListedServerSettingsAreRefusedToo() {
    final ContextConfiguration configuration = new ContextConfiguration();

    assertThatThrownBy(() -> ServerControlPlane.applySetting(configuration, GlobalConfiguration.HA_QUORUM.getKey(),
        "majorty")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ServerControlPlane.applySetting(configuration,
        GlobalConfiguration.HA_SERVER_ROLE.getKey(), "witness")).isInstanceOf(IllegalArgumentException.class);
  }

  /** A key that names no declared setting is still stored verbatim, as this endpoint has long allowed. */
  // Regression: #7296 set server setting honours its allow-list
  @Test
  void anUndeclaredKeyIsStillStoredVerbatim() {
    final ContextConfiguration configuration = new ContextConfiguration();

    ServerControlPlane.applySetting(configuration, "arcadedb.custom.notDeclared", "anything");

    assertThat(configuration.getValueAsString("arcadedb.custom.notDeclared", null)).isEqualTo("anything");
  }

  // Regression: #9749 a plugin can veto a server setting before it is stored
  /**
   * Issue #9749: a plugin can refuse a server setting an operator changes while the server runs, before it is stored. The
   * Gremlin Server plugin uses it to refuse a switch to production mode while its channelizer cannot conceal engine error
   * text; this drives the hook through the writers in the server module, HTTP {@code set server setting} and the control
   * plane gRPC's {@code SetServerSetting} calls, with a plugin that refuses the same switch.
   */
  @Nested
  class PluginVeto extends BaseGraphServerTest {
    private final HttpClient client = HttpClient.newHttpClient();

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, "veto9749:" + ServerSettingVetoTestPlugin.class.getName());
    }

    private ServerSettingVetoTestPlugin plugin() {
      for (final ServerPlugin p : getServer(0).getPlugins())
        if (p instanceof ServerSettingVetoTestPlugin veto)
          return veto;
      throw new AssertionError("ServerSettingVetoTestPlugin is not installed");
    }

    @BeforeEach
    void clearRecordedChanges() {
      plugin().getChanges().clear();
    }

    @Test
    void httpSetServerSettingRefusedByAPluginStoresNothing() throws Exception {
      final String before = getServer(0).getMode();

      final HttpResponse<String> response = executeServerCommand("set server setting arcadedb.server.mode production");

      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(response.body()).contains(ServerSettingVetoTestPlugin.REFUSAL);
      assertThat(getServer(0).getMode()).isEqualTo(before);
      assertThat(getServer(0).isProductionMode()).isFalse();
      assertThat(getServer(0).getOperatorSettingSource(GlobalConfiguration.SERVER_MODE.getKey()))
          .as("a refused change is not reported as an operator setting").isNull();
    }

    @Test
    void controlPlaneSetServerSettingRefusedByAPluginStoresNothing() {
      final String before = getServer(0).getMode();

      assertThatThrownBy(() -> new ServerControlPlane(getServer(0)).setServerSetting("arcadedb.server.mode", "production"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(ServerSettingVetoTestPlugin.REFUSAL);

      assertThat(getServer(0).getMode()).isEqualTo(before);
    }

    @Test
    void thePluginIsHandedTheParsedValueNotTheCommandText() {
      // Quoted and in another case: the allow-list accepts it and stores "production", so the plugin must see that
      assertThatThrownBy(() -> new ServerControlPlane(getServer(0)).setServerSetting("`arcadedb.server.mode`", "'Production'"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(ServerSettingVetoTestPlugin.REFUSAL);

      assertThat(plugin().getChanges()).containsExactly(
          new ServerSettingVetoTestPlugin.Change(GlobalConfiguration.SERVER_MODE, "production"));
      assertThat(getServer(0).isProductionMode()).isFalse();
    }

    @Test
    void aChangeThePluginAcceptsIsStored() throws Exception {
      final ContextConfiguration configuration = getServer(0).getConfiguration();
      final boolean hadValue = configuration.hasValue(GlobalConfiguration.SERVER_MODE.getKey());
      final Object previous = configuration.getValue(GlobalConfiguration.SERVER_MODE);
      try {
        assertThat(executeServerCommand("set server setting arcadedb.server.mode test").statusCode()).isEqualTo(200);

        assertThat(getServer(0).getMode()).isEqualTo("test");
        assertThat(plugin().getChanges()).containsExactly(new ServerSettingVetoTestPlugin.Change(GlobalConfiguration.SERVER_MODE, "test"));
      } finally {
        configuration.setValue(GlobalConfiguration.SERVER_MODE.getKey(), hadValue ? previous : null);
      }
    }

    @Test
    void anInvalidValueIsRefusedBeforeThePluginsAreAsked() {
      assertThatThrownBy(() -> new ServerControlPlane(getServer(0)).setServerSetting("arcadedb.server.mode", "prodction"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("prodction");

      assertThat(plugin().getChanges()).isEmpty();
    }

    /**
     * Issue #9836: the veto, the store and the operator mark are one operation, so a surface that stores a declared setting
     * through it owes none of them separately. The refusal is its own type so a surface that words refusals itself (the MCP
     * tool) can tell it from an {@link IllegalArgumentException} the store raised.
     */
    @Test
    void storeOperatorSettingRefusedByAPluginStoresAndMarksNothing() {
      final String before = getServer(0).getMode();

      assertThatThrownBy(() -> getServer(0).storeOperatorSetting(GlobalConfiguration.SERVER_MODE, "production"))
          .isInstanceOf(ServerSettingRefusedException.class)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(ServerSettingVetoTestPlugin.REFUSAL);

      assertThat(getServer(0).getMode()).isEqualTo(before);
      assertThat(getServer(0).getOperatorSettingSource(GlobalConfiguration.SERVER_MODE.getKey())).isNull();
    }

    @Test
    void storeOperatorSettingStoresAndMarksWhatThePluginAccepts() {
      final ContextConfiguration configuration = getServer(0).getConfiguration();
      final boolean hadValue = configuration.hasValue(GlobalConfiguration.SERVER_MODE.getKey());
      final Object previous = configuration.getValue(GlobalConfiguration.SERVER_MODE);
      try {
        getServer(0).storeOperatorSetting(GlobalConfiguration.SERVER_MODE, "test");

        assertThat(getServer(0).getMode()).isEqualTo("test");
        assertThat(getServer(0).getOperatorSettingSource(GlobalConfiguration.SERVER_MODE.getKey())).isEqualTo("runtime");
        assertThat(plugin().getChanges()).containsExactly(new ServerSettingVetoTestPlugin.Change(GlobalConfiguration.SERVER_MODE, "test"));
      } finally {
        configuration.setValue(GlobalConfiguration.SERVER_MODE.getKey(), hadValue ? previous : null);
      }
    }

    private HttpResponse<String> executeServerCommand(final String command) throws Exception {
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI(getServerHttpUrl("/api/v1/server")))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
          .setHeader("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .build();
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }

  // Regression: #9949 set database setting re-derives what the open database derives from it
  /**
   * Issue #9949: {@code set database setting} (HTTP and gRPC, both through {@link ServerControlPlane#setDatabaseSetting})
   * stored the value and saved {@code configuration.json}, but never re-derived what the open database derives from its
   * configuration: the serializer's date and datetime implementations and the statement caches. Outside Raft HA nothing
   * else did either, so the change was reported as applied while the open database kept reading DATE values as the
   * previous class until it was reopened. Nor did it run the check {@code ALTER DATABASE} runs before changing the
   * implementation (issue #9976), so a REGEXP on a DATE property could be left matching no value of the new class.
   */
  @Nested
  class DatabaseSettingRederives {
    @RegisterExtension
    final ServedDatabases served = new ServedDatabases();

    private static final String DB_NAME = "set9949db";

    @Test
    void dateImplementationReachesTheOpenDatabaseWithoutReopeningIt() throws Exception {
      final ServerDatabase database = served.open(DB_NAME);
      database.getSchema().createDocumentType("D").createProperty("d", Type.DATE);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("D").set("d", LocalDate.of(2024, 1, 15)).save().getIdentity());
      assertThat(database.getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);

      controlPlane(database).setDatabaseSetting(DB_NAME, "arcadedb.dateImplementation", "java.util.Date");

      assertThat(database.getSerializer().getDateImplementation()).isEqualTo(Date.class);
      try (final ResultSet rs = database.query("sql", "SELECT d FROM " + rid[0])) {
        assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(Date.class);
      }
    }

    @Test
    void dateTimeImplementationReachesTheOpenDatabaseWithoutReopeningIt() throws Exception {
      final ServerDatabase database = served.open(DB_NAME);
      assertThat(database.getSerializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);

      controlPlane(database).setDatabaseSetting(DB_NAME, "`arcadedb.dateTimeImplementation`", "'java.util.Date'");

      assertThat(database.getSerializer().getDateTimeImplementation()).isEqualTo(Date.class);
      assertThat(database.getSerializer().getSubMillisDateTimeImplementation()).isEqualTo(LocalDateTime.class);
    }

    /** The statement caches are re-derived too: a cached statement is never parsed again, so a lowered limit missed it. */
    @Test
    void aLoweredParseLimitReachesAStatementAlreadyCached() throws Exception {
      final ServerDatabase database = served.open(DB_NAME);
      database.getSchema().createDocumentType("T");
      final String query = "SELECT FROM T WHERE " + "(".repeat(10) + "1 = 1" + ")".repeat(10);
      try (final ResultSet rs = database.query("sql", query)) {
        assertThat(rs.hasNext()).isFalse();
      }

      controlPlane(database).setDatabaseSetting(DB_NAME, GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getKey(), "3");

      assertThatThrownBy(() -> database.query("sql", query)).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("maximum allowed depth of 3");
    }

    /** The check ALTER DATABASE runs before changing the implementation (issue #9976) is not bypassed by this path. */
    @Test
    void aChangeThatWouldBreakADateRegexpIsRefusedAndLeavesTheSettingAsItWas() throws Exception {
      final ServerDatabase database = served.open(DB_NAME);
      database.command("sql", "CREATE DOCUMENT TYPE R1");
      database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')");
      final ServerControlPlane controlPlane = controlPlane(database);

      assertThatThrownBy(() -> controlPlane.setDatabaseSetting(DB_NAME, "arcadedb.dateImplementation", "java.util.Date"))
          .hasMessageContaining("R1.d").hasMessageContaining("held as Date");

      assertThat(database.getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);
      assertThat(database.getConfiguration().hasValue(GlobalConfiguration.DATE_IMPLEMENTATION.getKey())).isFalse();
      final File configurationFile = new File(database.getDatabasePath(), "configuration.json");
      assertThat(!configurationFile.exists() || !FileUtils.readFileAsString(configurationFile).contains("dateImplementation"))
          .as("a refused change must not reach configuration.json").isTrue();
      database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')").close());

      // once the pattern is gone, the change goes through
      database.command("sql", "ALTER PROPERTY R1.d REGEXP null");
      controlPlane.setDatabaseSetting(DB_NAME, "arcadedb.dateImplementation", "java.util.Date");
      assertThat(database.getSerializer().getDateImplementation()).isEqualTo(Date.class);
    }

    /** The DATETIME twin of the check above, against the datetime implementation. */
    @Test
    void aChangeThatWouldBreakADateTimeRegexpIsRefusedAndLeavesTheSettingAsItWas() throws Exception {
      final ServerDatabase database = served.open(DB_NAME);
      database.command("sql", "CREATE DOCUMENT TYPE R2");
      database.command("sql", "CREATE PROPERTY R2.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
      final ServerControlPlane controlPlane = controlPlane(database);

      assertThatThrownBy(() -> controlPlane.setDatabaseSetting(DB_NAME, "arcadedb.dateTimeImplementation", "java.util.Date"))
          .hasMessageContaining("R2.t");

      assertThat(database.getSerializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);
      assertThat(database.getConfiguration().hasValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION.getKey())).isFalse();
    }

    private static ServerControlPlane controlPlane(final ServerDatabase database) {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? database : null);
      return new ServerControlPlane(server);
    }
  }
}
