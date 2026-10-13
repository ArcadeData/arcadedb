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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.plugin.ServerSettingVetoTestPlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9749: a plugin can refuse a server setting an operator changes while the server runs, before it is stored. The
 * Gremlin Server plugin uses it to refuse a switch to production mode while its channelizer cannot conceal engine error
 * text; this drives the hook through the writers in the server module, HTTP {@code set server setting} and the control
 * plane gRPC's {@code SetServerSetting} calls, with a plugin that refuses the same switch.
 */
class Issue9749ServerSettingPluginVetoTest extends BaseGraphServerTest {
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
