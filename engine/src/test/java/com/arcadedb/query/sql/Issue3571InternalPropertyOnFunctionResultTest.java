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
import java.util.Map;

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
      assertThat(rids.get(0)).isInstanceOf(RID.class);
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
      assertThat(rids.get(0)).isInstanceOf(RID.class);
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

  @Test
  void testInEIndexedAtIn() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571g IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571g IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571g");
      database.command("sql", "INSERT INTO V3571g");
      database.command("sql", "CREATE EDGE E3571g FROM (SELECT FROM V3571g) TO (SELECT FROM V3571g)");

      // @in on an edge should return the RID of the in-vertex
      final ResultSet rs = database.query("sql", "SELECT inE()[0].@in FROM V3571g");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object inRid = row.getProperty("inE()[0].@in");
      assertThat(inRid).isNotNull();
      assertThat(inRid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEIndexedAtOut() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571h IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571h IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571h");
      database.command("sql", "INSERT INTO V3571h");
      database.command("sql", "CREATE EDGE E3571h FROM (SELECT FROM V3571h) TO (SELECT FROM V3571h)");

      // @out on an edge should return the RID of the out-vertex
      final ResultSet rs = database.query("sql", "SELECT inE()[0].@out FROM V3571h");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object outRid = row.getProperty("inE()[0].@out");
      assertThat(outRid).isNotNull();
      assertThat(outRid).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testInEAtInList() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571i IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571i IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571i");
      database.command("sql", "INSERT INTO V3571i");
      database.command("sql", "CREATE EDGE E3571i FROM (SELECT FROM V3571i) TO (SELECT FROM V3571i)");

      // @in on a list of edges should return a list of in-vertex RIDs
      final ResultSet rs = database.query("sql", "SELECT inE().@in FROM V3571i");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object inList = row.getProperty("inE().@in");
      assertThat(inList).isNotNull();
      assertThat(inList).isInstanceOf(List.class);
      final List<?> rids = (List<?>) inList;
      assertThat(rids).hasSize(1);
      assertThat(rids.get(0)).isInstanceOf(RID.class);
      rs.close();
    });
  }

  @Test
  void testMapWithAtOut() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V3571j IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE E3571j IF NOT EXISTS");
      database.command("sql", "DELETE FROM V3571j");
      database.command("sql", "INSERT INTO V3571j");
      database.command("sql", "CREATE EDGE E3571j FROM (SELECT FROM V3571j) TO (SELECT FROM V3571j)");

      // map(ine.@rid.asString(), ine.@out) should return a map with the edge RID string as key
      // and the out-vertex RID as value
      final ResultSet rs = database.query("sql",
          "SELECT map(ine.@rid.asString(), ine.@out) FROM (SELECT inE()[0] AS ine FROM V3571j)");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // The result should have a map property
      final Object mapResult = row.getProperty("map(ine.@rid.asString(), ine.@out)");
      assertThat(mapResult).isNotNull();
      assertThat(mapResult).isInstanceOf(Map.class);
      final Map<?, ?> map = (Map<?, ?>) mapResult;
      assertThat(map).hasSize(1);
      // The value should be the out-vertex RID (not null)
      assertThat(map.values().iterator().next()).isNotNull();
      assertThat(map.values().iterator().next()).isInstanceOf(RID.class);
      rs.close();
    });
  }
}
