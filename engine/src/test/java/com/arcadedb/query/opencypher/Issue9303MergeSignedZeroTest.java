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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9303: {@code MERGE (n:T {v: 0.0})} over a stored -0.0 created a second node (or died with a
 * duplicated key under a UNIQUE index) because MERGE's re-verification told the two zeros apart, while a MATCH does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9303MergeSignedZeroTest extends TestHelper {

  private void load(final String type, final String index) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".v DOUBLE");
    if (index != null)
      database.command("sql", index);
    database.transaction(() -> database.newVertex(type).set("v", -0.0d).set("tag", "minus").save());
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") RETURN count(n) AS c")) {
      return rs.next().<Long>getProperty("c");
    }
  }

  @Test
  void mergeMatchesAStoredNegativeZeroWithAnIndex() {
    load("I", "CREATE INDEX ON I (v) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "MERGE (n:I {v: 0.0}) RETURN n").close());
    assertThat(count("I")).isEqualTo(1L);
  }

  @Test
  void mergeMatchesAStoredNegativeZeroWithoutAnIndex() {
    load("N", null);
    database.transaction(() -> database.command("opencypher", "MERGE (n:N {v: 0.0}) ON MATCH SET n.tag = 'matched' RETURN n").close());
    assertThat(count("N")).isEqualTo(1L);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N) RETURN n.tag AS t")) {
      assertThat(rs.next().<String>getProperty("t")).isEqualTo("matched");
    }
  }

  @Test
  void mergeOnAUniqueKeyHoldingNegativeZeroDoesNotCollide() {
    load("U", "CREATE INDEX ON U (v) UNIQUE");
    database.transaction(() -> database.command("opencypher", "MERGE (n:U {v: 0.0}) RETURN n").close());
    assertThat(count("U")).isEqualTo(1L);
  }

  @Test
  void mergeOfNegativeZeroMatchesAStoredPositiveZero() {
    database.command("sql", "CREATE VERTEX TYPE Z");
    database.transaction(() -> database.newVertex("Z").set("v", 0.0d).save());
    database.transaction(() -> database.command("opencypher", "MERGE (n:Z {v: -0.0}) RETURN n").close());
    assertThat(count("Z")).isEqualTo(1L);
  }
}
