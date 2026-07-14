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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPResourcesTest extends BaseGraphServerTest {

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = getServer(0).getMCPConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void listExposesOneSchemaResourcePerDatabase() {
    final JSONArray resources = MCPResources.list(getServer(0), user, config).getJSONArray("resources");

    JSONObject graphResource = null;
    for (int i = 0; i < resources.length(); i++)
      if ("arcadedb://graph/schema".equals(resources.getJSONObject(i).getString("uri")))
        graphResource = resources.getJSONObject(i);

    assertThat(graphResource).isNotNull();
    assertThat(graphResource.getString("name")).isEqualTo("graph schema");
    assertThat(graphResource.getString("mimeType")).isEqualTo("application/json");
    assertThat(graphResource.getString("description")).contains("graph");
  }

  @Test
  void listIsEmptyWhenReadsDisabled() {
    config.setAllowReads(false);

    final JSONArray resources = MCPResources.list(getServer(0), user, config).getJSONArray("resources");

    assertThat(resources.length()).isZero();
  }

  @Test
  void readReturnsSchemaIdenticalToGetSchemaTool() {
    final JSONObject resource = MCPResources.read(getServer(0), user, config, "arcadedb://graph/schema");

    final JSONArray contents = resource.getJSONArray("contents");
    assertThat(contents.length()).isEqualTo(1);
    assertThat(contents.getJSONObject(0).getString("uri")).isEqualTo("arcadedb://graph/schema");
    assertThat(contents.getJSONObject(0).getString("mimeType")).isEqualTo("application/json");

    final JSONObject toolResult = GetSchemaTool.execute(getServer(0), user, new JSONObject().put("database", "graph"), config);

    assertThat(contents.getJSONObject(0).getString("text")).isEqualTo(toolResult.toString());
  }

  @Test
  void readRejectsUnknownDatabase() {
    assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config, "arcadedb://nosuchdb/schema"))
        .isInstanceOf(MCPResourceNotFoundException.class)
        .hasMessageContaining("Resource not found");
  }

  @Test
  void readRejectsMalformedURI() {
    // Wrong scheme, wrong suffix, empty database, embedded slash, and a URI too short to hold a database name.
    for (final String uri : new String[] { "http://graph/schema", "arcadedb://graph/tables", "arcadedb:///schema",
        "arcadedb://a/b/schema", "arcadedb://schema", "graph", "" })
      assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config, uri))
          .isInstanceOf(MCPResourceNotFoundException.class);
  }

  @Test
  void readDeniedWhenReadsDisabled() {
    config.setAllowReads(false);

    assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config, "arcadedb://graph/schema"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void parseSchemaURIPreservesCaseAndUnderscores() {
    // java.net.URI would lowercase the authority and reject the underscore, silently resolving this to nothing.
    assertThat(MCPResources.parseSchemaURI("arcadedb://My_DB/schema")).isEqualTo("My_DB");
    assertThat(MCPResources.parseSchemaURI("arcadedb://graph/schema")).isEqualTo("graph");
    assertThat(MCPResources.parseSchemaURI("arcadedb://schema")).isNull();
    assertThat(MCPResources.parseSchemaURI(null)).isNull();
  }

  @Test
  void schemaURIRoundTripsThroughParse() {
    assertThat(MCPResources.parseSchemaURI(MCPResources.schemaURI("My_DB"))).isEqualTo("My_DB");
  }
}
