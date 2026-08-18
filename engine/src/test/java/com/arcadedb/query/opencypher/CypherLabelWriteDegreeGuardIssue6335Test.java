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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the degree guard of issue #6335. A Cypher label write maps to an ArcadeDB type change, and a record's type
 * comes from the bucket it lives in, so there is no in-place retype: the vertex is rewritten under the new type and
 * every incident edge is re-created, which makes {@code SET n:Label} a write proportional to degree that also
 * changes the RID of the vertex and of all of its edges.
 * <p>
 * That cannot be fixed while a label is a type, so it is made visible instead: it is counted and logged above
 * {@code arcadedb.opencypher.labelWriteDegreeWarning}, and refused above
 * {@code arcadedb.opencypher.labelWriteDegreeLimit} - which is off by default, because the rewrite is slow rather
 * than wrong and refusing it has to be the operator's choice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelWriteDegreeGuardIssue6335Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-write-degree-6335");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {k:'h'})");
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "CREATE (:Leaf {k:'l" + i + "'})");
      database.command("opencypher", "MATCH (h:Hub), (l:Leaf) CREATE (h)-[:LINK]->(l)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void theLimitIsOffByDefaultSoALabelWriteOnAHubStillRuns() {
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT))
        .isZero();
    database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
    assertThat(labelsOfHub()).contains("Hub", "Popular");
    assertThat(degreeOfHub()).isEqualTo(5);
  }

  @Test
  void aLabelWriteAboveTheLimitIsRefusedBeforeAnythingMoves() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 4);
    try {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (h:Hub) SET h:Popular"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("labelWriteDegreeLimit");

      // Refused before any record moved: the hub still has its original type and all of its edges.
      assertThat(labelsOfHub()).containsExactly("Hub");
      assertThat(degreeOfHub()).isEqualTo(5);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }
  }

  @Test
  void aRemoveOfALabelIsGuardedTheSameWay() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 4);
    try {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (h:Hub) REMOVE h:Hub"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("labelWriteDegreeLimit");
      assertThat(labelsOfHub()).containsExactly("Hub");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }
  }

  @Test
  void aLabelWriteAtOrBelowTheLimitIsAllowed() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 5);
    try {
      database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
      assertThat(labelsOfHub()).contains("Hub", "Popular");
      assertThat(degreeOfHub()).isEqualTo(5);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }
  }

  @Test
  void crossingTheWarningThresholdReportsWithoutRefusing() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING, 1);
    try {
      database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
      assertThat(labelsOfHub()).contains("Hub", "Popular");
      assertThat(degreeOfHub()).isEqualTo(5);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING, 10_000);
    }
  }

  private List<String> labelsOfHub() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (h {k:'h'}) RETURN labels(h) AS l")) {
      assertThat(rs.hasNext()).isTrue();
      final List<String> labels = new ArrayList<>(rs.next().getProperty("l"));
      Collections.sort(labels);
      return labels;
    }
  }

  private long degreeOfHub() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (h {k:'h'})-[r]-() RETURN count(r) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
