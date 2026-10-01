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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema file must not depend on the history of the in-memory maps it is serialised from (issue #8206).
 * <p>
 * A Raft leader builds its type map through DDL - growing it, and never shrinking it when a type is dropped - while a
 * follower rebuilds a fresh map from the schema file on every replicated schema entry. Both are hash maps, whose
 * iteration order depends on their table capacity, so the two replicas serialised the same types in different orders
 * and {@code RaftReplicationChangeSchemaIT} saw "different" schema files. A reopen walks exactly the follower's path
 * (fresh maps filled from the file), so a schema written before and after a reopen must be the same string.
 */
class Issue8206SchemaSerializationOrderTest extends TestHelper {
  private static final int CREATED = 40;
  private static final int KEPT    = 5;

  @Test
  void typesSerializeInTheSameOrderAfterDropAndReload() {
    // Grow the type map well past its initial capacity, then shrink it back: a hash map keeps the large table.
    for (int i = 0; i < CREATED; i++)
      database.getSchema().createVertexType("Type" + i);
    for (int i = KEPT; i < CREATED; i++)
      database.getSchema().dropType("Type" + i);
    database.getSchema().createVertexType("TypeAfterDrop");

    assertSameSchemaAfterReopen();
    assertThat(keysOf(schemaJson().getJSONObject("types"))).isSorted();
  }

  @Test
  void propertiesAndCustomValuesSerializeInTheSameOrderAfterDropAndReload() {
    final DocumentType type = database.getSchema().createDocumentType("Wide");
    for (int i = 0; i < CREATED; i++) {
      type.createProperty("prop" + i, String.class);
      type.setCustomValue("custom" + i, i);
    }
    for (int i = KEPT; i < CREATED; i++) {
      type.dropProperty("prop" + i);
      type.setCustomValue("custom" + i, null);
    }
    type.createProperty("propAfterDrop", String.class);

    final Property kept = type.getProperty("prop0");
    for (int i = 0; i < CREATED; i++)
      kept.setCustomValue("propertyCustom" + i, i);
    for (int i = KEPT; i < CREATED; i++)
      kept.setCustomValue("propertyCustom" + i, null);

    assertSameSchemaAfterReopen();
    final JSONObject wide = schemaJson().getJSONObject("types").getJSONObject("Wide");
    assertThat(keysOf(wide.getJSONObject("properties"))).isSorted();
    assertThat(keysOf(wide.getJSONObject("custom"))).isSorted();
    assertThat(keysOf(wide.getJSONObject("properties").getJSONObject("prop0").getJSONObject("custom"))).isSorted();
  }

  @Test
  void indexesSerializeInTheSameOrderAfterDropAndReload() {
    final DocumentType type = database.getSchema().createDocumentType("Indexed");
    final String[] indexNames = new String[CREATED];
    for (int i = 0; i < CREATED; i++) {
      type.createProperty("prop" + i, Integer.class);
      indexNames[i] = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "prop" + i).getName();
    }
    for (int i = KEPT; i < CREATED; i++)
      database.getSchema().dropIndex(indexNames[i]);

    assertSameSchemaAfterReopen();
    assertThat(keysOf(schemaJson().getJSONObject("types").getJSONObject("Indexed").getJSONObject("indexes"))).isSorted();
  }

  @Test
  void triggersAndFunctionsSerializeInTheSameOrderAfterDropAndReload() {
    database.getSchema().createDocumentType("Triggered");
    for (int i = 0; i < CREATED; i++) {
      database.command("sql", "CREATE TRIGGER trigger" + i + " BEFORE CREATE ON TYPE Triggered EXECUTE SQL 'SELECT 1'");
      database.command("sql", "DEFINE FUNCTION lib" + i + ".f 'SELECT 1 AS result' LANGUAGE sql");
    }
    // Libraries first: unregisterFunctionLibrary() does not save the schema by itself (#8879), so the trigger drops
    // that follow are what write their removal to the file.
    for (int i = KEPT; i < CREATED; i++)
      database.getSchema().unregisterFunctionLibrary("lib" + i);
    for (int i = KEPT; i < CREATED; i++)
      database.getSchema().dropTrigger("trigger" + i);

    assertSameSchemaAfterReopen();
    assertThat(keysOf(schemaJson().getJSONObject("triggers"))).isSorted();
    assertThat(keysOf(schemaJson().getJSONObject("functions"))).isSorted();
  }

  private void assertSameSchemaAfterReopen() {
    final String before = normalized();
    reopenDatabase();
    assertThat(normalized()).isEqualTo(before);
  }

  private JSONObject schemaJson() {
    return database.getSchema().getEmbedded().toJSON();
  }

  // schemaVersion is a per-node save counter, not schema content; a reload legitimately changes it.
  private String normalized() {
    final JSONObject json = schemaJson();
    json.remove("schemaVersion");
    return json.toString();
  }

  private static List<String> keysOf(final JSONObject json) {
    return new ArrayList<>(json.keySet());
  }
}
