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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MCPConfigurationTest {
  private static final String TEST_ROOT = "./target/mcp-config-test";

  @BeforeEach
  void setUp() {
    new File(TEST_ROOT + "/config").mkdirs();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_ROOT));
  }

  @Test
  void defaultValues() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.isAllowReads()).isTrue();
    assertThat(config.isAllowInsert()).isFalse();
    assertThat(config.isAllowUpdate()).isFalse();
    assertThat(config.isAllowDelete()).isFalse();
    assertThat(config.isAllowSchemaChange()).isFalse();
    assertThat(config.getAllowedUsers()).containsExactly("root");
  }

  @Test
  void saveAndLoad() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setEnabled(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setAllowedUsers(List.of("root", "admin"));
    config.save();

    final MCPConfiguration loaded = new MCPConfiguration(TEST_ROOT);
    loaded.load();

    assertThat(loaded.isEnabled()).isTrue();
    assertThat(loaded.isAllowInsert()).isTrue();
    assertThat(loaded.isAllowUpdate()).isTrue();
    assertThat(loaded.isAllowDelete()).isFalse();
    assertThat(loaded.getAllowedUsers()).containsExactly("root", "admin");
  }

  @Test
  void userAllowed() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    assertThat(config.isUserAllowed("root")).isTrue();
    assertThat(config.isUserAllowed("unknown")).isFalse();
  }

  @Test
  void wildcardUser() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setAllowedUsers(List.of("*"));

    assertThat(config.isUserAllowed("root")).isTrue();
    assertThat(config.isUserAllowed("anyone")).isTrue();
  }

  @Test
  void toJSON() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    final JSONObject json = config.toJSON();
    assertThat(json.getBoolean("enabled")).isFalse();
    assertThat(json.getBoolean("allowReads")).isTrue();
    assertThat(json.getBoolean("allowInsert")).isFalse();
    assertThat(json.getJSONArray("allowedUsers").length()).isEqualTo(1);
    assertThat(json.getJSONArray("allowedUsers").getString(0)).isEqualTo("root");
  }

  @Test
  void updateFrom() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    final JSONObject update = new JSONObject()
        .put("allowInsert", true)
        .put("allowDelete", true)
        .put("allowedUsers", new JSONArray().put("root").put("editor"));

    config.updateFrom(update);

    assertThat(config.isAllowInsert()).isTrue();
    assertThat(config.isAllowDelete()).isTrue();
    assertThat(config.getAllowedUsers()).containsExactly("root", "editor");
    // Unchanged values should remain
    assertThat(config.isEnabled()).isFalse();
    assertThat(config.isAllowUpdate()).isFalse();
  }

  @Test
  void updateFromNullAllowedUsersResultsInEmptyList() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    // Sending "allowedUsers": null should be treated as clearing the list, not a no-op
    final JSONObject update = new JSONObject();
    update.put("allowedUsers", (Object) null);

    config.updateFrom(update);

    assertThat(config.getAllowedUsers()).isEmpty();
    assertThat(config.isUserAllowed("root")).isFalse();
  }

  @Test
  void createDefaultFileOnFirstLoad() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    final File configFile = new File(TEST_ROOT + "/config/mcp-config.json");
    assertThat(configFile.exists()).isTrue();
  }
}
