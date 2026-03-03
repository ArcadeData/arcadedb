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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3571: SQL: accessing internal properties in a map.
 * <p>
 * Accessing properties with an {@code @} prefix (internal properties) via dot notation on the result
 * of a function call (e.g. {@code inE().@rid}) was returning null instead of the expected values.
 * </p>
 */
class Issue3571InternalPropertyOnFunctionResultTest extends TestHelper {

  @Test
  void testInEAtRidUnquoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571a IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571a IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571a");
      database.command("sql", "INSERT INTO V3571a");
      database.command("sql", "CREATE EDGE E3571a FROM (SELECT FROM V3571a) TO (SELECT FROM V3571a)");

      final ResultSet rs = database.query("sql", "SELECT inE().@rid FROM V3571a");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ridList = row.getProperty("inE().@rid");
      assertThat(ridList).isNotNull();
      assertThat(ridList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) ridList;
      assertThat(rids).hasSize(1);
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEAtRidQuoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571b IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571b IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571b");
      database.command("sql", "INSERT INTO V3571b");
      database.command("sql", "CREATE EDGE E3571b FROM (SELECT FROM V3571b) TO (SELECT FROM V3571b)");

      // Backtick-quoted `@rid` is treated as a record attribute, so the projection key is "inE().@rid"
      final ResultSet rs = database.query("sql", "SELECT inE().`@rid` FROM V3571b");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ridList = row.getProperty("inE().@rid");
      assertThat(ridList).isNotNull();
      assertThat(ridList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) ridList;
      assertThat(rids).hasSize(1);
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEIndexedAtRidUnquoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571c IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571c IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571c");
      database.command("sql", "INSERT INTO V3571c");
      database.command("sql", "CREATE EDGE E3571c FROM (SELECT FROM V3571c) TO (SELECT FROM V3571c)");

      final ResultSet rs = database.query("sql", "SELECT inE()[0].@rid FROM V3571c");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object rid = row.getProperty("inE()[0].@rid");
      assertThat(rid).isNotNull();
      assertThat(rid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEIndexedAtRidQuoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571d IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571d IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571d");
      database.command("sql", "INSERT INTO V3571d");
      database.command("sql", "CREATE EDGE E3571d FROM (SELECT FROM V3571d) TO (SELECT FROM V3571d)");

      // Backtick-quoted `@rid` is treated as a record attribute, so the projection key is "inE()[0].@rid"
      final ResultSet rs = database.query("sql", "SELECT inE()[0].`@rid` FROM V3571d");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object rid = row.getProperty("inE()[0].@rid");
      assertThat(rid).isNotNull();
      assertThat(rid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEAtType() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571e IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571e IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571e");
      database.command("sql", "INSERT INTO V3571e");
      database.command("sql", "CREATE EDGE E3571e FROM (SELECT FROM V3571e) TO (SELECT FROM V3571e)");

      // @type on function result should return the edge type name
      final ResultSet rs = database.query("sql", "SELECT inE()[0].@type FROM V3571e");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object type = row.getProperty("inE()[0].@type");
      assertThat(type).isNotNull();
      assertThat(type).isEqualTo("E3571e");
      rs.close();
    });
  }

  @Test
  void testInEAtCat() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571f IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571f IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571f");
      database.command("sql", "INSERT INTO V3571f");
      database.command("sql", "CREATE EDGE E3571f FROM (SELECT FROM V3571f) TO (SELECT FROM V3571f)");

      // @cat on an edge should return "e" — uses isRecordAttributeName string-matching fallback
      // since @cat has no dedicated grammar token
      final ResultSet rs = database.query("sql", "SELECT inE()[0].`@cat` FROM V3571f");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object cat = row.getProperty("inE()[0].@cat");
      assertThat(cat).isNotNull();
      assertThat(cat).isEqualTo("e");
      rs.close();
    });
  }
}
