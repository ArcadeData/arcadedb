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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12418: {@code UPSERT} was accepted on an edge type and wrote an edge record whose
 * endpoints pointed at records that did not exist, a record the engine cannot read back: the second run of the same
 * statement failed with a {@code SerializationException} and the damage survived a restart of the server.
 * <p>
 * The reference page states that {@code UPSERT} is not available on edge types, because an edge always requires
 * {@code FROM} and {@code TO} endpoints, so the statement is refused and nothing is written.
 */
class UpsertOnEdgeTypeIssue12418Test extends TestHelper {

  @Test
  void upsertOnAnEdgeTypeIsRefusedAndNothingIsWritten() {
    final EdgeType type = database.getSchema().createEdgeType("Knows");
    type.createProperty("w", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "w");

    // inside a transaction: outside one the statement fails with "Transaction not begun" before it reaches the UPSERT
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Knows SET w = 1 UPSERT WHERE w = 1")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Knows")
        .hasMessageContaining("UPSERT");

    // no edge with missing endpoints, and no record that cannot be decoded again
    assertThat(storedEdges()).isZero();
  }

  @Test
  void upsertOnAnEdgeTypeIsRefusedAlsoWhenTheEdgeAlreadyExists() {
    final EdgeType type = database.getSchema().createEdgeType("Knows");
    type.createProperty("w", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "w");

    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      final var alice = database.newVertex("Person").save();
      final var bob = database.newVertex("Person").save();
      alice.newEdge("Knows", bob).set("w", 1).save();
    });

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Knows SET w = 2 UPSERT WHERE w = 1")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Knows");

    assertThat(storedEdges()).isEqualTo(1L);
  }

  /** The records the type holds, counted from storage (count(@rid)), not from the bucket's metadata counter. */
  private long storedEdges() {
    return ((Number) database.query("sql", "SELECT count(@rid) AS n FROM Knows").next().getProperty("n")).longValue();
  }

  @Test
  void upsertOnAVertexTypeStillCreatesTheVertex() {
    final var type = database.getSchema().createVertexType("City");
    type.createProperty("name", Type.STRING);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");

    database.transaction(() -> database.command("sql", "UPDATE City SET name = 'Rome' UPSERT WHERE name = 'Rome'"));

    assertThat(database.countType("City", true)).isEqualTo(1L);
    final Object name = database.query("sql", "SELECT name FROM City").next().getProperty("name");
    assertThat(name).isEqualTo("Rome");
  }
}
