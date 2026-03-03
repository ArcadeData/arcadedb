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
      database.command("sql", "INSERT INTO V3571a");
      database.command("sql", "CREATE EDGE E3571a FROM (SELECT FROM V3571a) TO (SELECT FROM V3571a)");

      // SELECT inE().@rid FROM V — should return an array of RIDs
      final ResultSet rs = database.query("sql", "SELECT inE().@rid FROM V3571a");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ridList = row.getProperty("inE().@rid");
      assertThat(ridList).isNotNull();
      assertThat(ridList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) ridList;
      assertThat(rids).isNotEmpty();
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEAtRidQuoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571b IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571b IF NOT EXISTS");
      database.command("sql", "INSERT INTO V3571b");
      database.command("sql", "CREATE EDGE E3571b FROM (SELECT FROM V3571b) TO (SELECT FROM V3571b)");

      // SELECT inE().`@rid` FROM V — backtick-quoted variant, should also return an array of RIDs.
      // The backtick-quoted `@rid` is treated as a record attribute, so the projection key is "inE().@rid".
      final ResultSet rs = database.query("sql", "SELECT inE().`@rid` FROM V3571b");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ridList = row.getProperty("inE().@rid");
      assertThat(ridList).isNotNull();
      assertThat(ridList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) ridList;
      assertThat(rids).isNotEmpty();
      assertThat(rids.getFirst()).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEIndexedAtRidUnquoted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571c IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571c IF NOT EXISTS");
      database.command("sql", "INSERT INTO V3571c");
      database.command("sql", "CREATE EDGE E3571c FROM (SELECT FROM V3571c) TO (SELECT FROM V3571c)");

      // SELECT inE()[0].@rid FROM V — should return an RID (not a list)
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
      database.command("sql", "INSERT INTO V3571d");
      database.command("sql", "CREATE EDGE E3571d FROM (SELECT FROM V3571d) TO (SELECT FROM V3571d)");

      // SELECT inE()[0].`@rid` FROM V — backtick-quoted, should return an RID.
      // The backtick-quoted `@rid` is treated as a record attribute, so the projection key is "inE()[0].@rid".
      final ResultSet rs = database.query("sql", "SELECT inE()[0].`@rid` FROM V3571d");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object rid = row.getProperty("inE()[0].@rid");
      assertThat(rid).isNotNull();
      assertThat(rid).isInstanceOf(RID.class);
      rs.close();
    });
  }
}
