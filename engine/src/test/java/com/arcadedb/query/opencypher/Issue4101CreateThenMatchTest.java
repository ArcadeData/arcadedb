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
 * Regression test for GitHub issue #4101.
 * <p>
 * A {@code MATCH} that follows {@code CREATE} in the same statement must see
 * the newly created labeled nodes immediately, matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4101CreateThenMatchTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4101-create-then-match").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createThenLabeledMatchSeesNewNode() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CREATE (:Person {name:'Charlie'}) "
              + "MATCH (n:Person) "
              + "RETURN count(*) AS c");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    });
  }

  @Test
  void createTwoThenLabeledMatchSeesBoth() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CREATE (:Person {name:'Charlie'}), (:Person {name:'Diana'}) "
              + "MATCH (n:Person) "
              + "RETURN count(*) AS c");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
    });
  }

  @Test
  void createThenMatchWithBarrierStillWorks() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CREATE (:Person {name:'Charlie'}) "
              + "WITH 1 AS x "
              + "MATCH (n:Person) "
              + "RETURN count(*) AS c");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    });
  }

  @Test
  void createThenMatchUnlabeledControl() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CREATE ({name:'Charlie'}) "
              + "MATCH (n) "
              + "RETURN count(*) AS c");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    });
  }

  @Test
  void createThenMatchSeesMixOfPreexistingAndNew() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'Alice', age:30})");
      database.command("opencypher", "CREATE (:Person {name:'Bob', age:25})");
    });
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CREATE ({name:'Charlie'}), ({name:'Diana'}) "
              + "MATCH (n) "
              + "RETURN DISTINCT n.name AS name "
              + "ORDER BY name DESC");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.<String>getProperty("name"));
      }
      assertThat(names).containsExactly("Diana", "Charlie", "Bob", "Alice");
    });
  }
}
