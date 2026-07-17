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
package com.arcadedb.server.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.mcp.tools.GetServerSettingsTool;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-p9wc-4fhr-78wm: the MCP {@code get_server_settings} tool masked only settings whose key
 * literally contained "password", so the HA cluster token ({@code arcadedb.ha.clusterToken}, SCOPE.SERVER) leaked in
 * clear. Combined with cluster-forwarded-auth headers that token allowed root impersonation. This is the sibling of
 * GHSA-46hj-24h4-j8gf, which fixed the same leak in {@code GET /api/v1/server} (GetServerHandler). After the fix every
 * setting flagged secret by {@link GlobalConfiguration#isHidden()} (which includes clusterToken) is redacted to
 * {@code *****}.
 */
class GetServerSettingsToolRedactionIT extends BaseGraphServerTest {

  private static final String SECRET_TOKEN = "s3cr3t-cluster-token-should-never-leak";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, SECRET_TOKEN);
  }

  @Test
  void clusterTokenIsRedactedInMcpServerSettings() throws Exception {
    testEachServer((serverIndex) -> {
      final MCPConfiguration config = new MCPConfiguration("./target/test");
      config.setAllowReads(true);

      final JSONObject result = GetServerSettingsTool.execute(getServer(serverIndex), null, new JSONObject(), config);

      // The raw secret must not appear anywhere in the serialized response.
      assertThat(result.toString()).as("cluster token must not leak through the MCP get_server_settings tool")
          .doesNotContain(SECRET_TOKEN);

      // And the specific setting entry must be present but masked (both value and default).
      final JSONArray settings = result.getJSONArray("settings");
      boolean found = false;
      for (int i = 0; i < settings.length(); i++) {
        final JSONObject entry = settings.getJSONObject(i);
        if ("arcadedb.ha.clusterToken".equals(entry.getString("key"))) {
          found = true;
          assertThat(entry.getString("value")).as("clusterToken value must be masked").isEqualTo("*****");
          assertThat(entry.getString("default")).as("clusterToken default must be masked").isEqualTo("*****");
        }
      }
      assertThat(found).as("clusterToken setting must be present in the exported settings").isTrue();
    });
  }
}
