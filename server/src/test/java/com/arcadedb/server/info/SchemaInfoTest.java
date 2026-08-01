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
package com.arcadedb.server.info;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the schema document shape. Both the MCP get_schema tool and the Studio AI assistant render from this
 * one producer, so a change here changes two consumer contracts at once.
 */
class SchemaInfoTest extends BaseGraphServerTest {

  @Test
  void schemaDocumentCarriesTypesWithCategoryAndProperties() {
    final JSONObject schema = SchemaInfo.toJSON(getServer(0).getDatabase(getDatabaseName()), getDatabaseName());

    assertThat(schema.getString("database")).isEqualTo(getDatabaseName());

    final JSONArray types = schema.getJSONArray("types");
    assertThat(types.length()).isGreaterThan(0);

    JSONObject vertexType = null;
    for (int i = 0; i < types.length(); i++) {
      final JSONObject type = types.getJSONObject(i);
      if (VERTEX1_TYPE_NAME.equals(type.getString("name", null)))
        vertexType = type;
    }

    assertThat(vertexType).isNotNull();
    assertThat(vertexType.getString("category")).isEqualTo("vertex");
    assertThat(vertexType.has("properties")).isTrue();
  }

  @Test
  void resolvingForAUserRejectsAnUnknownDatabase() {
    assertThatThrownBy(() -> SchemaInfo.forUser(getServer(0), rootUser(), "doesNotExist"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("doesNotExist");
  }

  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }
}
