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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    assertThat(config.getToolProfile()).isEqualTo(MCPConfiguration.ToolProfile.ALL);
  }

  @Test
  void saveAndLoad() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setEnabled(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setToolProfile(MCPConfiguration.ToolProfile.RAG);
    config.setAllowedUsers(List.of("root", "admin"));
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject()
                .put("allowUpdate", false)
                .put("allowedUsers", new JSONArray().put("admin")))));
    config.save();

    final MCPConfiguration loaded = new MCPConfiguration(TEST_ROOT);
    loaded.load();

    assertThat(loaded.isEnabled()).isTrue();
    assertThat(loaded.isAllowInsert()).isTrue();
    assertThat(loaded.isAllowUpdate()).isTrue();
    assertThat(loaded.isAllowDelete()).isFalse();
    assertThat(loaded.getToolProfile()).isEqualTo(MCPConfiguration.ToolProfile.RAG);
    assertThat(loaded.getAllowedUsers()).containsExactly("root", "admin");
    assertThat(loaded.getPermissionsForDatabase("tenant").isAllowInsert()).isTrue();
    assertThat(loaded.getPermissionsForDatabase("tenant").isAllowUpdate()).isFalse();
    assertThat(loaded.getPermissionsForDatabase("tenant").isUserAllowed("admin")).isTrue();
    assertThat(loaded.getPermissionsForDatabase("tenant").isUserAllowed("root")).isFalse();
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
    assertThat(config.isUserAllowed("apitoken:mytoken")).isTrue();
  }

  @Test
  void apiTokenUserAllowedByBareTokenName() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setAllowedUsers(List.of("root", "mytoken"));

    // API token user with synthetic name "apitoken:mytoken" should match "mytoken" in allowedUsers
    assertThat(config.isUserAllowed("apitoken:mytoken")).isTrue();
    // Regular user "root" still works
    assertThat(config.isUserAllowed("root")).isTrue();
    // API token not in the list should be denied
    assertThat(config.isUserAllowed("apitoken:othertoken")).isFalse();
  }

  @Test
  void apiTokenUserAllowedByFullPrefixedName() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setAllowedUsers(List.of("apitoken:mytoken"));

    // Should also work when the full prefixed name is in the list
    assertThat(config.isUserAllowed("apitoken:mytoken")).isTrue();
  }

  @Test
  void toJSON() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    final JSONObject json = config.toJSON();
    assertThat(json.getBoolean("enabled")).isFalse();
    assertThat(json.getBoolean("allowReads")).isTrue();
    assertThat(json.getBoolean("allowInsert")).isFalse();
    assertThat(json.getString("profile")).isEqualTo("all");
    assertThat(json.getJSONArray("allowedUsers").length()).isEqualTo(1);
    assertThat(json.getJSONArray("allowedUsers").getString(0)).isEqualTo("root");
    assertThat(json.has("databases")).isFalse();
  }

  @Test
  void databaseOverrideInheritsUnspecifiedGlobalValues() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject().put("allowUpdate", false))));

    final MCPPermissions tenant = config.getPermissionsForDatabase("tenant");
    assertThat(tenant.isAllowReads()).isTrue();
    assertThat(tenant.isAllowInsert()).isTrue();
    assertThat(tenant.isAllowUpdate()).isFalse();

    final MCPPermissions inherited = config.getPermissionsForDatabase("unconfigured");
    assertThat(inherited).isSameAs(config);
    assertThat(inherited.isAllowReads()).isTrue();
    assertThat(inherited.isAllowInsert()).isTrue();
    assertThat(inherited.isAllowUpdate()).isTrue();
  }

  @Test
  void databaseOverrideCannotGrantPermissionsDeniedGlobally() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setAllowReads(true);
    config.setAllowInsert(false);
    config.setAllowUpdate(false);
    config.setAllowDelete(false);
    config.setAllowSchemaChange(false);
    config.setAllowAdmin(false);
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject()
                .put("allowReads", false)
                .put("allowInsert", true)
                .put("allowUpdate", true)
                .put("allowDelete", true)
                .put("allowSchemaChange", true)
                .put("allowAdmin", true))));

    final MCPPermissions tenant = config.getPermissionsForDatabase("tenant");
    assertThat(tenant.isAllowReads()).isFalse();
    assertThat(tenant.isAllowInsert()).isFalse();
    assertThat(tenant.isAllowUpdate()).isFalse();
    assertThat(tenant.isAllowDelete()).isFalse();
    assertThat(tenant.isAllowSchemaChange()).isFalse();
    assertThat(tenant.isAllowAdmin()).isFalse();
  }

  @Test
  void databaseAllowedUsersAreAnAdditionalRestriction() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.setAllowedUsers(List.of("root", "tenant-token"));
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject()
                .put("allowedUsers", new JSONArray().put("tenant-token")))));

    final MCPPermissions tenant = config.getPermissionsForDatabase("tenant");
    assertThat(tenant.isUserAllowed("root")).isFalse();
    assertThat(tenant.isUserAllowed("apitoken:tenant-token")).isTrue();

    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject()
                .put("allowedUsers", new JSONArray().put("*")))));
    assertThat(config.getPermissionsForDatabase("tenant").isUserAllowed("root")).isTrue();
    assertThat(config.getPermissionsForDatabase("tenant").isUserAllowed("unknown")).isFalse();
  }

  @Test
  void explicitNullClearsDatabaseOverrides() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject().put("allowReads", false))));
    assertThat(config.getPermissionsForDatabase("tenant").isAllowReads()).isFalse();

    final JSONObject update = new JSONObject();
    update.put("databases", (Object) null);
    config.updateFrom(update);

    assertThat(config.getPermissionsForDatabase("tenant").isAllowReads()).isTrue();
    assertThat(config.toJSON().has("databases")).isFalse();
  }

  @Test
  void databaseUpdatesMergeByNameAndNullRemovesOneOverride() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant_a", new JSONObject().put("allowReads", false))
            .put("tenant_b", new JSONObject().put("allowInsert", false))));

    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject()
            .put("tenant_a", new JSONObject().put("allowUpdate", false))));

    assertThat(config.toJSON().getJSONObject("databases").keySet())
        .containsExactlyInAnyOrder("tenant_a", "tenant_b");
    assertThat(config.getPermissionsForDatabase("tenant_a").isAllowUpdate()).isFalse();
    assertThat(config.toJSON().getJSONObject("databases")
        .getJSONObject("tenant_a").has("allowReads")).isFalse();

    final JSONObject removal = new JSONObject();
    removal.put("tenant_a", (Object) null);
    config.updateFrom(new JSONObject().put("databases", removal));

    assertThat(config.toJSON().getJSONObject("databases").keySet())
        .containsExactly("tenant_b");
  }

  @Test
  void unknownDatabaseOverrideSettingIsRejectedWithoutPartialUpdate() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);

    assertThatThrownBy(() -> config.updateFrom(new JSONObject()
        .put("allowInsert", true)
        .put("databases", new JSONObject()
            .put("tenant", new JSONObject().put("allowRead", false)))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("allowRead");

    assertThat(config.isAllowInsert()).isFalse();
    assertThat(config.getPermissionsForDatabase("tenant").isAllowReads()).isTrue();
  }

  @Test
  void updateFrom() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    final JSONObject update = new JSONObject()
        .put("allowInsert", true)
        .put("allowDelete", true)
        .put("profile", "admin")
        .put("allowedUsers", new JSONArray().put("root").put("editor"));

    config.updateFrom(update);

    assertThat(config.isAllowInsert()).isTrue();
    assertThat(config.isAllowDelete()).isTrue();
    assertThat(config.getToolProfile()).isEqualTo(MCPConfiguration.ToolProfile.ADMIN);
    assertThat(config.getAllowedUsers()).containsExactly("root", "editor");
    // Unchanged values should remain
    assertThat(config.isEnabled()).isFalse();
    assertThat(config.isAllowUpdate()).isFalse();
  }

  @Test
  void invalidProfileIsRejectedWithoutPartialUpdate() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    assertThatThrownBy(() -> config.updateFrom(new JSONObject()
        .put("allowInsert", true)
        .put("profile", "retrieval")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("all").hasMessageContaining("rag").hasMessageContaining("admin");

    assertThat(config.isAllowInsert()).isFalse();
    assertThat(config.getToolProfile()).isEqualTo(MCPConfiguration.ToolProfile.ALL);
  }

  @Test
  void invalidBooleanTypeIsRejected() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);
    config.load();

    assertThatThrownBy(() -> config.updateFrom(new JSONObject().put("enabled", "yes")))
        .isInstanceOf(JSONException.class)
        .hasMessageContaining("enabled").hasMessageContaining("boolean");

    assertThat(config.isEnabled()).isFalse();
  }

  @Test
  void profileNamesAreCaseInsensitive() {
    final MCPConfiguration config = new MCPConfiguration(TEST_ROOT);

    config.updateFrom(new JSONObject().put("profile", "RaG"));

    assertThat(config.getToolProfile()).isEqualTo(MCPConfiguration.ToolProfile.RAG);
    assertThat(config.toJSON().getString("profile")).isEqualTo("rag");
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
