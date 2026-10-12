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
import com.arcadedb.mcp.MCPToolArgumentException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.plugin.ServerSettingVetoTestPlugin;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9749: the {@code set_server_setting} tool writes the server configuration itself rather than through the control
 * plane, so it has to ask the plugins too. A plugin that refuses a switch to production mode (the Gremlin Server plugin
 * does while its channelizer cannot conceal engine error text) must leave the mode where it was.
 */
class Issue9749SetServerSettingPluginVetoTest extends BaseGraphServerTest {
  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "veto9749:" + ServerSettingVetoTestPlugin.class.getName());
  }

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
    plugin().getChanges().clear();
  }

  private ServerSettingVetoTestPlugin plugin() {
    for (final ServerPlugin p : getServer(0).getPlugins())
      if (p instanceof ServerSettingVetoTestPlugin veto)
        return veto;
    throw new AssertionError("ServerSettingVetoTestPlugin is not installed");
  }

  @Test
  void aChangeAPluginRefusesIsNotStored() {
    final String before = getServer(0).getMode();

    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", GlobalConfiguration.SERVER_MODE.getKey()).put("value", "Production"), config))
        // answered in every mode: the refusal is the server's wording about the request, not engine text
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessage(ServerSettingVetoTestPlugin.REFUSAL);

    assertThat(getServer(0).getMode()).isEqualTo(before);
    assertThat(getServer(0).isProductionMode()).isFalse();
    assertThat(plugin().getChanges()).as("the plugin is handed the parsed value")
        .containsExactly(new ServerSettingVetoTestPlugin.Change(GlobalConfiguration.SERVER_MODE, "production"));
  }

  @Test
  void aChangeThePluginAcceptsIsStored() {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final boolean hadValue = configuration.hasValue(GlobalConfiguration.SERVER_MODE.getKey());
    final Object previous = configuration.getValue(GlobalConfiguration.SERVER_MODE);
    try {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", GlobalConfiguration.SERVER_MODE.getKey()).put("value", "test"), config);

      assertThat(result.getString("newValue")).isEqualTo("test");
      assertThat(getServer(0).getMode()).isEqualTo("test");
    } finally {
      configuration.setValue(GlobalConfiguration.SERVER_MODE.getKey(), hadValue ? previous : null);
    }
  }
}
