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
 * Regression test for GitHub issue #4097.
 * <p>
 * An EXISTS subquery whose body returns an expression derived from an outer
 * variable must evaluate to true (the body produces a row), matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4097ExistsOuterVarTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4097-exists-outer-var").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id:1, name:'test'}), (:Node {id:2, name:'other'})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void existsWithOuterVarReturnsTrue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN exists { RETURN n AS x } AS result ORDER BY result LIMIT 1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
  }

  @Test
  void existsWithOuterVarPropertyReturnsTrue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN exists { RETURN n.id AS x } AS result ORDER BY result LIMIT 1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
  }

  @Test
  void existsWithConstantReturnsTrue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN exists { RETURN 1 AS x } AS result ORDER BY result LIMIT 1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
  }

  @Test
  void existsWithInnerMatchReturnsTrue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN exists { MATCH (m:Node) RETURN m AS x } AS result ORDER BY result LIMIT 1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
  }

  @Test
  void existsWithCountStarReturnsTrue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN exists { RETURN count(*) AS c } AS result ORDER BY result LIMIT 1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
  }
}
