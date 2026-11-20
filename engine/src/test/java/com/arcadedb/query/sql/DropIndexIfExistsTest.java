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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11815: {@code DROP INDEX <name> IF EXISTS} on an index that does not exist changed the schema anyway: it saved
 * schema.json, moved the schema version and, on an HA cluster, shipped a schema entry every follower applied.
 */
class DropIndexIfExistsTest extends TestHelper {

  @Test
  void aMissingIndexIsNoSchemaChange() {
    database.getSchema().createDocumentType("T");
    final LocalSchema schema = database.getSchema().getEmbedded();
    final long before = schema.getVersion();

    try (final ResultSet rs = database.command("sql", "DROP INDEX `T[x]` IF EXISTS")) {
      assertThat(rs.next().<String>getProperty("indexName")).isEqualTo("T[x]");
    }

    assertThat(schema.getVersion()).as("no schema save for an index that is not there").isEqualTo(before);
  }

  @Test
  void anExistingIndexIsStillDroppedAndAMissingOneWithoutIfExistsStillRefused() {
    database.getSchema().createDocumentType("T").createProperty("x", String.class);
    database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "x");

    database.command("sql", "DROP INDEX `T[x]` IF EXISTS").close();
    assertThat(database.getSchema().existsIndex("T[x]")).isFalse();

    assertThatThrownBy(() -> database.command("sql", "DROP INDEX `T[x]`").close())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("Index not found");
  }
}
