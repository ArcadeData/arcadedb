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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7784, MCP sibling: {@code get_server_settings} read each value off the process-wide
 * {@code GlobalConfiguration} enum while taking {@code overridden} from the server's own
 * {@link ContextConfiguration}, so an LLM asked to diagnose the configuration was handed the enum's numbers while
 * every handler ran on the overlay's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7784McpServerSettingsEffectiveValueTest extends BaseGraphServerTest {

  private static final int STARTUP_OVERRIDE = 4242;
  private static final int LIVE_OVERRIDE    = 777;

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, STARTUP_OVERRIDE);
  }

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowedUsers(List.of("root"));
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void theToolReportsTheValueTheServerActuallyRunsOn() {
    final String key = GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey();
    final int declaredDefault = ((Number) GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getDefValue())
        .intValue();
    assertThat(declaredDefault)
        .as("precondition: the override must differ from the declared default, or the test proves nothing")
        .isNotEqualTo(STARTUP_OVERRIDE);

    JSONObject setting = settingFromTool(key);
    assertThat(setting.getBoolean("overridden")).isTrue();
    assertThat(setting.getInt("value")).isEqualTo(STARTUP_OVERRIDE);
    assertThat(setting.getInt("default")).as("'default' keeps reporting the enum's declared default")
        .isEqualTo(declaredDefault);

    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, LIVE_OVERRIDE);

    setting = settingFromTool(key);
    assertThat(setting.getInt("value")).as("a live change must be visible to the tool that diagnoses it")
        .isEqualTo(LIVE_OVERRIDE);
  }

  private JSONObject settingFromTool(final String key) {
    final JSONArray settings = GetServerSettingsTool.execute(getServer(0), user, new JSONObject(), config)
        .getJSONArray("settings");
    for (int i = 0; i < settings.length(); i++) {
      final JSONObject setting = settings.getJSONObject(i);
      if (key.equals(setting.getString("key")))
        return setting;
    }
    throw new AssertionError("setting '" + key + "' is not reported by get_server_settings");
  }
}
