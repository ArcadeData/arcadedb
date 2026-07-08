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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5137: {@code CASE WHEN r IS NOT NULL} mis-evaluates a relationship
 * variable introduced by a preceding {@code OPTIONAL MATCH}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCaseOptionalMatchRelIssue5137Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphercase5137").create();

    database.transaction(() -> {
      database.command("opencypher", """
          CREATE
            (a:User {id: 1, active: true}),
            (b:User {id: 2, active: true}),
            (c:User {active: false}),
            (a)-[:FRIEND]->(c)
          """);
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void diagnosticRawPredVsCasePred() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", """
          MATCH (u:User {active: true})
          OPTIONAL MATCH (u)-[r:FRIEND]->(:User)
          RETURN
            u.id AS id,
            r IS NOT NULL AS raw_pred,
            CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END AS case_pred
          ORDER BY id
          """);

      final Map<Object, Result> byId = new HashMap<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        byId.put(r.getProperty("id"), r);
      }

      assertThat(byId).hasSize(2);

      // User 1 has a FRIEND relationship
      assertThat((Boolean) byId.get(1).getProperty("raw_pred")).isTrue();
      assertThat(((Number) byId.get(1).getProperty("case_pred")).intValue()).isEqualTo(1);

      // User 2 has no FRIEND relationship
      assertThat((Boolean) byId.get(2).getProperty("raw_pred")).isFalse();
      assertThat(((Number) byId.get(2).getProperty("case_pred")).intValue()).isEqualTo(0);
    });
  }

  @Test
  void withCaseThenReturn() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", """
          MATCH (u:User {active: true})
          OPTIONAL MATCH (u)-[r:FRIEND]->(:User)
          WITH u, CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END AS b
          RETURN u.id AS id, b
          ORDER BY id
          """);

      final Map<Object, Object> byId = new HashMap<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        byId.put(r.getProperty("id"), r.getProperty("b"));
      }
      assertThat(((Number) byId.get(1)).intValue()).isEqualTo(1);
      assertThat(((Number) byId.get(2)).intValue()).isEqualTo(0);
    });
  }

  @Test
  void directSetWithCase() {
    database.transaction(() -> {
      database.command("opencypher", """
          MATCH (u:User {active: true})
          OPTIONAL MATCH (u)-[r:FRIEND]->(:User)
          SET u.boundary = CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END
          """);
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", """
          MATCH (u:User)
          WHERE u.id IS NOT NULL
          RETURN u.id AS id, u.boundary AS boundary
          ORDER BY id
          """);
      final Map<Object, Object> byId = new HashMap<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        byId.put(r.getProperty("id"), r.getProperty("boundary"));
      }
      assertThat(((Number) byId.get(1)).intValue()).isEqualTo(1);
      assertThat(((Number) byId.get(2)).intValue()).isEqualTo(0);
    });
  }

  @Test
  void withCasePredicateThenSet() {
    database.transaction(() -> {
      database.command("opencypher", """
          MATCH (u:User {active: true})
          OPTIONAL MATCH (u)-[r:FRIEND]->(:User)
          WITH u, CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END AS b
          SET u.boundary = b
          """);
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", """
          MATCH (u:User)
          WHERE u.id IS NOT NULL
          RETURN u.id AS id, u.boundary AS boundary
          ORDER BY id
          """);

      final Map<Object, Object> boundaryById = new HashMap<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        boundaryById.put(r.getProperty("id"), r.getProperty("boundary"));
      }

      assertThat(((Number) boundaryById.get(1)).intValue()).isEqualTo(1);
      assertThat(((Number) boundaryById.get(2)).intValue()).isEqualTo(0);
    });
  }
}
