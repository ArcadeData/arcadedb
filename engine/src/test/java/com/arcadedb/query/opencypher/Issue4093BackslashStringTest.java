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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4093.
 * <p>
 * Backslash characters in Cypher string literals must be preserved when the
 * following character is not a recognized escape sequence, matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4093BackslashStringTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4093-backslash-string").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void unknownEscapePreservesBackslash() {
    final ResultSet rs = database.query("opencypher", "RETURN 'Alice\\Bob' AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice\\Bob");
  }

  @Test
  void backslashInPropertyRoundTrips() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'Alice\\Bob'})"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:Person) RETURN n.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice\\Bob");
  }

  @Test
  void backslashInPropertyDoesNotMatchUnescapedForm() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'Alice\\Bob'})"));

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name = 'AliceBob' RETURN count(*) AS c");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(0L);
  }

  @Test
  void recognizedEscapeStillProcessed() {
    final ResultSet rs = database.query("opencypher", "RETURN 'a\\nb' AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("a\nb");
  }
}
