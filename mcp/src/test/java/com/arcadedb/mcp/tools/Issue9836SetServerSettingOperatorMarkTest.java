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
import com.arcadedb.server.plugin.ServerSettingVetoTestPlugin;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9836: a server setting changed through the {@code set_server_setting} tool is a value an operator set, exactly like
 * the same change over HTTP or gRPC, so the support diagnostics have to report it as one ({@code nonDefault}, source
 * {@code runtime}) and not as a value the server computed for itself. Storing a declared setting at runtime and recording
 * that an operator stored it are one operation, {@code ArcadeDBServer#storeOperatorSetting}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9836SetServerSettingOperatorMarkTest extends BaseGraphServerTest {
  private static final GlobalConfiguration SETTING = GlobalConfiguration.SERVER_NAME;

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "veto9836:" + ServerSettingVetoTestPlugin.class.getName());
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
  }

  @Test
  void aSettingChangedOverMcpIsRecordedAsSetByTheOperator() {
    final ContextConfiguration configuration = getServer(0).getConfiguration();
    final boolean hadValue = configuration.hasValue(SETTING.getKey());
    final Object previous = configuration.getValue(SETTING);
    try {
      assertThat(getServer(0).getOperatorSettingSource(SETTING.getKey())).as("not yet an operator setting")
          .isNotEqualTo("runtime");

      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", SETTING.getKey()).put("value", "node-9836"), config);

      assertThat(result.getString("newValue")).isEqualTo("node-9836");
      assertThat(getServer(0).getOperatorSettingSource(SETTING.getKey())).isEqualTo("runtime");
    } finally {
      configuration.setValue(SETTING.getKey(), hadValue ? previous : null);
    }
  }

  @Test
  void aChangeAPluginRefusesIsNotRecorded() {
    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", GlobalConfiguration.SERVER_MODE.getKey()).put("value", "production"), config))
        .isInstanceOf(MCPToolArgumentException.class)
        .hasMessage(ServerSettingVetoTestPlugin.REFUSAL);

    assertThat(getServer(0).getOperatorSettingSource(GlobalConfiguration.SERVER_MODE.getKey())).isNull();
  }

  @Test
  void aValueThatIsNotOfTheSettingsTypeIsNotRecorded() {
    final String key = GlobalConfiguration.ASYNC_WORKER_THREADS.getKey();

    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", key).put("value", "abc"), config))
        .isInstanceOf(MCPToolArgumentException.class);

    assertThat(getServer(0).getOperatorSettingSource(key)).isNull();
  }
}
