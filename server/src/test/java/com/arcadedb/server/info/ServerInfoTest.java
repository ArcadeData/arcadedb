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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The visibility predicate is the only thing that decides which databases appear, and the HA block is opt-in.
 * Both are what let one producer serve the MCP server_status tool and the Studio AI assistant, which apply
 * different policies over the same data.
 */
class ServerInfoTest extends BaseGraphServerTest {

  @Test
  void versionNameAndLanguagesAreAlwaysPresent() {
    final JSONObject info = ServerInfo.toJSON(getServer(0), db -> true, false);

    assertThat(info.getString("version")).isNotEmpty();
    assertThat(info.getString("serverName")).isNotEmpty();
    assertThat(info.has("languages")).isTrue();
  }

  @Test
  void theVisibilityPredicateFiltersTheDatabaseList() {
    final JSONObject visible = ServerInfo.toJSON(getServer(0), db -> true, false);
    final JSONObject hidden = ServerInfo.toJSON(getServer(0), db -> false, false);

    assertThat(visible.getJSONArray("databases").length()).isGreaterThan(0);
    assertThat(hidden.getJSONArray("databases").length()).isZero();
  }

  @Test
  void theHaBlockIsOmittedWhenNotRequested() {
    assertThat(ServerInfo.toJSON(getServer(0), db -> true, false).has("ha")).isFalse();
  }
}
