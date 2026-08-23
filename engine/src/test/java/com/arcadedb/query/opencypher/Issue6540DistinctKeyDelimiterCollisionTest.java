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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6540: {@code ProjectReturnStep}, {@code UnionStep} and {@code WithStep} built their
 * DISTINCT/UNION deduplication key by concatenating {@code name=value|} for every projected property
 * into one string. A projected value whose rendering itself contains {@code =} or {@code |} could make
 * two genuinely different rows concatenate to the same key string and get wrongly collapsed - e.g. a
 * single property {@code a = "1|b=2"} and two properties {@code a=1, b=2} both used to render as
 * {@code "a=1|b=2|"}.
 * <p>
 * {@code UNION} is the clearest way to put two differently-shaped rows (one column vs. two columns)
 * through the same dedup key, since a plain {@code RETURN}/{@code WITH} fixes its column set for every
 * row of a single branch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6540DistinctKeyDelimiterCollisionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-6540-distinct-key-collision");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * The reported case, verbatim: a single string property that happens to look like a rendered
   * {@code name=value|name=value|} key must not collide with an unrelated two-property row.
   */
  @Test
  void unionDoesNotCollapseDifferentlyShapedRowsThatRenderToTheSameDelimitedString() {
    // Both branches write "RETURN *", so the parse-time column-name check that UNION otherwise
    // enforces (all branches must share the same return column names) passes trivially - but the
    // *actual* per-row property sets still differ at runtime, exactly like the reported example.
    final List<Result> rows = new ArrayList<>();
    try (ResultSet rs = database.query("cypher", """
        WITH '1|b=2' AS a RETURN *
        UNION
        WITH 1 AS a, 2 AS b RETURN *""")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }

    assertThat(rows).hasSize(2);
    assertThat(rows).anySatisfy(row -> {
      assertThat(row.getPropertyNames()).containsExactly("a");
      assertThat(row.<String>getProperty("a")).isEqualTo("1|b=2");
    });
    assertThat(rows).anySatisfy(row -> {
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("a", "b");
      assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(1);
      assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(2);
    });
  }

  /**
   * Sanity check: two rows that really do carry equal names/values must still collapse under UNION's
   * implicit DISTINCT.
   */
  @Test
  void unionStillCollapsesTrueDuplicates() {
    final List<Result> rows = new ArrayList<>();
    try (ResultSet rs = database.query("cypher", """
        RETURN 1 AS a, 2 AS b
        UNION
        RETURN 1 AS a, 2 AS b""")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    assertThat(rows).hasSize(1);
  }
}
