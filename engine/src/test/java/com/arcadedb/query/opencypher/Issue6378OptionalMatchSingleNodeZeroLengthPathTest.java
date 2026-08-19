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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #6378: {@code OPTIONAL MATCH p = ({k: 1}) RETURN p} raised
 * "ZeroLengthPathStep requires a previous step" instead of returning the matching node as a
 * zero-length path, even though the equivalent non-optional {@code MATCH p = ({k: 1}) RETURN p}
 * worked correctly.
 */
class Issue6378OptionalMatchSingleNodeZeroLengthPathTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue6378").create();
    database.getSchema().createVertexType("N");
    database.transaction(() -> database.newVertex("N").set("k", 1).save());
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void controlMatchWithSingleNodeZeroLengthPathReturnsOneRow() {
    // Sanity check: the non-optional form from the issue must keep working.
    final ResultSet rs = database.query("opencypher", "MATCH p = ({k: 1}) RETURN p");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    final Object path = row.getProperty("p");
    assertThat(path).isInstanceOf(TraversalPath.class);
    assertThat(((TraversalPath) path).getStartVertex()).isNotNull();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void standaloneOptionalMatchWithSingleNodeZeroLengthPathReturnsOneRow() {
    // Exact query from the issue report.
    final ResultSet rs = database.query("opencypher", "OPTIONAL MATCH p = ({k: 1}) RETURN p");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    final Object path = row.getProperty("p");
    assertThat(path).isInstanceOf(TraversalPath.class);

    final Vertex startNode = ((TraversalPath) path).getStartVertex();
    assertThat(startNode).isNotNull();
    assertThat((Integer) startNode.get("k")).isEqualTo(1);

    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void standaloneOptionalMatchWithNoMatchReturnsNullPath() {
    // No vertex has k: 999, so OPTIONAL MATCH must fall back to a single NULL row rather than
    // throwing or returning zero rows.
    final ResultSet rs = database.query("opencypher", "OPTIONAL MATCH p = ({k: 999}) RETURN p");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((Object) row.getProperty("p")).isNull();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void chainedOptionalMatchWithSingleNodeZeroLengthPathReturnsOneRow() {
    // Covers the branch where matchChainStart was already set by an earlier (non-optional)
    // MATCH clause before the OPTIONAL MATCH single-node pattern is processed.
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:N {k: 1}) OPTIONAL MATCH p = (n) RETURN p");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    final Object path = row.getProperty("p");
    assertThat(path).isInstanceOf(TraversalPath.class);
    assertThat((Integer) ((TraversalPath) path).getStartVertex().get("k")).isEqualTo(1);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }
}
