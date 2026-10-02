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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the connect request tells the portal about the server: a flat map of short texts the portal interprets, never a secret
 * or a path. The cluster name and size only exist for a server that runs in HA.
 */
class SupportConnectorAttributesTest {
  @Test
  void aStandaloneServerSendsHostVersionAndNameOnly() {
    final JSONObject a = SupportConnector.attributesOf("db1.example.com", "arcadedb_0", null, 0);

    assertThat(a.getString("host")).isEqualTo("db1.example.com");
    assertThat(a.getString("version")).isNotBlank();
    assertThat(a.getString("serverName")).isEqualTo("arcadedb_0");
    assertThat(a.has("clusterName")).isFalse();
    assertThat(a.has("haNodes")).isFalse();
  }

  @Test
  void anHaServerAlsoSendsTheClusterNameAndSizeAsText() {
    final JSONObject a = SupportConnector.attributesOf("db1", "arcadedb-0", "prod-eu", 3);

    assertThat(a.getString("clusterName")).isEqualTo("prod-eu");
    assertThat(a.getString("haNodes")).isEqualTo("3");
  }

  @Test
  void anUnknownClusterSizeIsLeftOut() {
    final JSONObject a = SupportConnector.attributesOf("db1", "arcadedb-0", "prod-eu", 0);

    assertThat(a.getString("clusterName")).isEqualTo("prod-eu");
    assertThat(a.has("haNodes")).isFalse();
  }

  @Test
  void emptyValuesAreDroppedAndLongOnesTruncatedTo200Characters() {
    final JSONObject a = SupportConnector.attributesOf("", "x".repeat(500), "  ", 0);

    assertThat(a.has("host")).isFalse();
    assertThat(a.getString("serverName")).hasSize(200);
    assertThat(a.has("clusterName")).isFalse();
  }

  @Test
  void everyKeyAndValueFitsTheContractOfThePortal() {
    final JSONObject a = SupportConnector.attributesOf("h".repeat(300), "s".repeat(300), "c".repeat(300), 12);

    assertThat(a.length()).isLessThanOrEqualTo(20);
    for (final String key : a.keySet()) {
      assertThat(key).matches("[A-Za-z0-9._-]{1,40}");
      assertThat(a.get(key)).isInstanceOf(String.class);
      assertThat(a.getString(key).length()).isLessThanOrEqualTo(200);
    }
    assertThat(a.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length).isLessThanOrEqualTo(2048);
  }
}
