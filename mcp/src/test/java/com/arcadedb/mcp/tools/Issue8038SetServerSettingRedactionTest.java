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
package com.arcadedb.mcp.tools;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8038. {@code set_server_setting} echoes back the value it replaced and the value it stored, and it
 * masked both on {@link GlobalConfiguration#isHidden()} alone while the settings READERS - {@code GET
 * /api/v1/server}, the MCP {@code get_server_settings} tool and {@code SELECT FROM schema:database} - had moved
 * onto {@link GlobalConfiguration#publishableValue(Object)}. {@code isHidden()} is {@code false} for
 * {@code arcadedb.server.defaultDatabases}, whose credentials live inside the value, so one call to the SETTER
 * handed back in clear what every GETTER refuses to show - and {@code previousValue} in particular carries the
 * PRE-EXISTING configured value, which the caller did not supply.
 * <p>
 * Both fields now go through {@code publishableValue}, so the setter cannot disclose what the getter conceals.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8038SetServerSettingRedactionTest extends BaseGraphServerTest {

  private static final String CONFIGURED = "Universe[albert:einstein:admin];Amiga[Jay:Miner,Jack:Tramiel]";
  private static final String REPLACEMENT = "Galaxy[carl:sagan:admin]";

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowAdmin(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void thePreviousValueOfDefaultDatabasesIsRedactedNotEchoed() {
    withConfiguredDefaultDatabases(() -> {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
              .put("value", REPLACEMENT), config);

      final String previous = result.getString("previousValue");
      assertThat(previous).as("the pre-existing credentials the caller never supplied").doesNotContain("einstein");
      assertThat(previous).doesNotContain("Miner").doesNotContain("Tramiel");
      assertThat(previous).as("and it is exactly what the readers would publish")
          .isEqualTo(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue(CONFIGURED));
    });
  }

  @Test
  void theNewValueOfDefaultDatabasesIsRedactedToo() {
    withConfiguredDefaultDatabases(() -> {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
              .put("value", REPLACEMENT), config);

      final String stored = result.getString("newValue");
      assertThat(stored).as("a response the caller may log, cache or hand on").doesNotContain("sagan");
      assertThat(stored).isEqualTo(GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue(REPLACEMENT));
    });
  }

  /**
   * The redaction replaces one span and copies everything else, so the response still tells an operator which
   * databases exist and who may reach them with which role. Blanking the whole value would make the setter's
   * answer useless for the case it is read for.
   */
  @Test
  void theResponseStillNamesTheDatabasesAndTheirUsers() {
    withConfiguredDefaultDatabases(() -> {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
              .put("value", REPLACEMENT), config);

      assertThat(result.getString("previousValue")).contains("Universe").contains("albert").contains("admin");
      assertThat(result.getString("newValue")).contains("Galaxy").contains("carl");
    });
  }

  /** A setting {@code isHidden()} covers is still masked whole: the new rule is a superset of the old one. */
  @Test
  void aWhollyHiddenSettingIsStillMaskedWhole() {
    final GlobalConfiguration setting = GlobalConfiguration.HA_CLUSTER_TOKEN;
    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      getServer(0).getConfiguration().setValue(setting.getKey(), "previous-cluster-token");

      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", setting.getKey()).put("value", "new-cluster-token"), config);

      assertThat(result.getString("previousValue")).isEqualTo("*****");
      assertThat(result.getString("newValue")).isEqualTo("*****");
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }

  /** And a setting that carries no secret is still reported as written, or the response explains nothing. */
  @Test
  void anOrdinarySettingIsStillReportedInFull() {
    final GlobalConfiguration setting = GlobalConfiguration.SQL_STATEMENT_CACHE;
    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", setting.getKey()).put("value", "500"), config);

      assertThat(result.getString("newValue")).isEqualTo("500");
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }

  /**
   * Installs a configured {@code defaultDatabases} the way an operator's startup configuration would, runs the
   * assertion against it and puts the server's configuration back. The setting is only read at boot
   * ({@code ArcadeDBServer.loadDefaultDatabases}), so writing it here creates no database.
   */
  private void withConfiguredDefaultDatabases(final Runnable assertion) {
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_DEFAULT_DATABASES;
    final boolean hadValue = getServer(0).getConfiguration().hasValue(setting.getKey());
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      getServer(0).getConfiguration().setValue(setting.getKey(), CONFIGURED);
      assertion.run();
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), hadValue ? previous : null);
    }
  }
}
