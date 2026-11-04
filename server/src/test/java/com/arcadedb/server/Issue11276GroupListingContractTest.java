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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11276: {@code GET /server/groups} declares every group with {@code resultSetLimit}, {@code readTimeout}, {@code access}
 * and {@code types} ({@code GroupDefinition}), but the shipped {@code crud} group had no {@code readTimeout} and the listing
 * returned groups as stored, so a client enforcing the required members failed on every call.
 */
class Issue11276GroupListingContractTest extends StaticBaseServerTest {
  private static final List<String> REQUIRED = List.of("resultSetLimit", "readTimeout", "access", "types");

  private ArcadeDBServer server;

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    server = new ArcadeDBServer(config);
    server.start();
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      if (server != null && server.isStarted())
        server.stop();
    } finally {
      super.endTest();
    }
  }

  @Test
  void shippedGroupsCarryEveryRequiredMember() throws IOException {
    Path root = new File("").getAbsoluteFile().toPath();
    while (root != null && !Files.isRegularFile(root.resolve("package/src/main/config/server-groups.json")))
      root = root.getParent();
    assertThat(root).isNotNull();

    final JSONObject document = new JSONObject(Files.readString(root.resolve("package/src/main/config/server-groups.json")));
    final JSONObject databases = document.getJSONObject("databases");
    for (final String database : databases.keySet()) {
      final JSONObject groups = databases.getJSONObject(database).getJSONObject("groups");
      for (final String name : groups.keySet())
        for (final String member : REQUIRED)
          assertThat(groups.getJSONObject(name).has(member)).as("group '%s' of database '%s' lacks '%s'", name, database, member)
              .isTrue();
    }
  }

  @Test
  void listingNormalizesAGroupStoredWithoutTheDefaultedMembers() {
    // What the 26.11.1 snapshot seeded: no readTimeout
    server.getSecurity().saveGroup("*", "issue11276", new JSONObject().put("access", new JSONArray()).put("types",
        new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));

    final JSONObject listed = new ServerControlPlane(server).listGroups().getJSONObject("databases").getJSONObject("*")
        .getJSONObject("groups").getJSONObject("issue11276");

    for (final String member : REQUIRED)
      assertThat(listed.has(member)).as("listed group lacks '%s'", member).isTrue();
    assertThat(listed.getLong("readTimeout")).isEqualTo(-1L);
    assertThat(listed.getLong("resultSetLimit")).isEqualTo(-1L);

    // the normalization is a view: the stored document is untouched
    assertThat(server.getSecurity().groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
        .getJSONObject("issue11276").has("readTimeout")).isFalse();
  }
}
