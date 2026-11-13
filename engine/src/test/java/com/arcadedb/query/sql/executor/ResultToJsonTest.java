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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Result#toJSON()} and {@link Result#valueToJSON(Object)}: what a projected row serializes, and how its values are
 * rendered.
 */
class ResultToJsonTest extends TestHelper {

  // ---- #7773: toJSON() of a projected row ----
  /*
   * Regression test for issue #7773: {@code Result.toJSON()} short-circuited to the backing record whenever the row
   * {@code isElement()}, but {@code Projection#calculateSingle()} builds exactly such a row for {@code SELECT *} plus
   * an extra projection item - it sets the element (for the {@code *}) AND writes the explicit items into the row's
   * own content, so the two disagree. The element short-circuit therefore answered the record instead of the row: a
   * computed alias was dropped, an alias colliding with a real column answered the stored value instead of the
   * computed one, and an excluded column ({@code !col}) leaked back in - even though {@code JsonSerializer
   * .serializeResult()} (which drives the HTTP/remote path) and {@code getPropertyNames()}/{@code getProperty()} were
   * already correct in all three cases.
   */

  /** Regression test for issue #7773. */
  @Test
  void starPlusComputedAliasIsNotDropped() throws Exception {
    TestHelper.executeInNewDatabase("issue7773Alias", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1, d = 500");

      try (final ResultSet rs = db.query("sql", "SELECT *, n+1 AS z FROM T")) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).contains("z");
        assertThat(row.<Integer>getProperty("z")).isEqualTo(2);

        final JSONObject json = row.toJSON();
        assertThat(json.has("z")).as("computed alias must not be dropped from toJSON()").isTrue();
        assertThat(json.getInt("z")).isEqualTo(2);
        assertThat(json.getInt("n")).isEqualTo(1);
        assertThat(json.getInt("d")).isEqualTo(500);
      }
    });
  }

  /** Regression test for issue #7773. */
  @Test
  void starPlusAliasCollidingWithARealColumnAnswersTheComputedValue() throws Exception {
    TestHelper.executeInNewDatabase("issue7773Collide", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1, d = 500");

      try (final ResultSet rs = db.query("sql", "SELECT *, n+1 AS d FROM T")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("d")).isEqualTo(2);

        final JSONObject json = row.toJSON();
        assertThat(json.getInt("d")).as("the computed alias must win over the stored column of the same name")
            .isEqualTo(2);
      }
    });
  }

  /** Regression test for issue #7773. */
  @Test
  void starMinusAnExcludedColumnIsNotLeakedBackIn() throws Exception {
    TestHelper.executeInNewDatabase("issue7773Exclude", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1, secret = 'hunter2'");

      try (final ResultSet rs = db.query("sql", "SELECT *, !secret FROM T")) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).doesNotContain("secret");

        final JSONObject json = row.toJSON();
        assertThat(json.has("secret")).as("excluded column must not leak back into toJSON()").isFalse();
        assertThat(json.getInt("n")).isEqualTo(1);
      }
    });
  }

  /** Regression test for issue #7773. */
  @Test
  void plainSelectStarIsUnchangedAndStillCarriesRidAndType() throws Exception {
    TestHelper.executeInNewDatabase("issue7773PlainStar", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1");

      try (final ResultSet rs = db.query("sql", "SELECT * FROM T")) {
        final Result row = rs.next();
        final JSONObject json = row.toJSON();
        assertThat(json.getInt("n")).isEqualTo(1);
        assertThat(json.has("@rid")).isTrue();
        assertThat(json.has("@type")).isTrue();
      }
    });
  }

  /** Regression test for issue #7773. */
  @Test
  void asJSONSqlMethodAgreesWithResultToJSON() throws Exception {
    TestHelper.executeInNewDatabase("issue7773AsJson", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1");

      try (final ResultSet rs = db.query("sql",
          "SELECT $a.asJSON() AS j FROM T LET $a = (SELECT *, n+1 AS z FROM T)")) {
        final Object j = rs.next().getProperty("j");
        // asJSON() on a Result dispatches through Result.toJSON() (SQLMethodAsJSON), so it must now include z too.
        assertThat(j.toString()).contains("\"z\"");
      }
    });
  }

  // ---- #9810: zoned temporals in toJSON() ----
  /*
   * Issue #9810: {@code Result.toJSON()} rendered a ZONED_DATETIME as epoch millis (the zone is gone) and an OFFSET_TIME as
   * {@code null} (the value is gone), while the server's own {@link JsonSerializer} rendered both as ISO text. Equality, the
   * indexes and ORDER BY now tell two zones at one instant apart, so a client that cannot read the zone back cannot write a
   * predicate that matches. The two renderings of one row must agree.
   */

  private static final String ZONED  = "2026-01-01T00:30+02:00";
  private static final String OFFSET = "00:30+02:00";

  private void load() {
    final var type = database.getSchema().createVertexType("Z");
    type.createProperty("z", Type.ZONED_DATETIME);
    type.createProperty("t", Type.OFFSET_TIME);
    type.createProperty("l", Type.LOCAL_TIME);
    database.transaction(() -> database.newVertex("Z")
        .set("z", ZonedDateTime.parse(ZONED))
        .set("t", OffsetTime.parse(OFFSET))
        .set("l", LocalTime.parse("07:15:30"))
        .save());
  }

  /** Regression test for issue #9810. */
  @Test
  void projectionKeepsTheZoneAndTheOffsetTime() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT z, t, l FROM Z")) {
      final Result row = rs.next();
      final JSONObject json = row.toJSON();
      assertThat(json.getString("z")).isEqualTo(ZONED);
      assertThat(json.getString("t")).isEqualTo(OFFSET);
      assertThat(json.getString("l")).isEqualTo("07:15:30");
    }
  }

  /** Regression test for issue #9810. */
  @Test
  void elementResultKeepsTheZoneAndTheOffsetTime() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Z")) {
      final JSONObject json = rs.next().toJSON();
      assertThat(json.getString("z")).isEqualTo(ZONED);
      assertThat(json.getString("t")).isEqualTo(OFFSET);
      assertThat(json.getString("l")).isEqualTo("07:15:30");
    }
  }

  /** Regression test for issue #9810. */
  @Test
  void resultAndServerSerializerAgree() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT z, t, l FROM Z")) {
      final Result row = rs.next();
      final JSONObject viaResult = row.toJSON();
      final JSONObject viaSerializer = JsonSerializer.createJsonSerializer().serializeResult(database, row);
      for (final String property : new String[] { "z", "t", "l" })
        assertThat(viaResult.get(property)).as(property).isEqualTo(viaSerializer.get(property));
    }
  }

  /** Regression test for issue #9810. */
  @Test
  void renderedZoneParsesBackToTheStoredValue() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT z, t FROM Z")) {
      final JSONObject json = rs.next().toJSON();
      assertThat(ZonedDateTime.parse(json.getString("z"))).isEqualTo(ZonedDateTime.parse(ZONED));
      assertThat(OffsetTime.parse(json.getString("t"))).isEqualTo(OffsetTime.parse(OFFSET));
    }
  }

  /** Regression test for issue #9810. */
  @Test
  void temporalsInsideAListAndAMapKeepTheirText() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT [z, t] AS list, {'k': z} AS map FROM Z")) {
      final JSONObject json = rs.next().toJSON();
      final JSONArray list = json.getJSONArray("list");
      assertThat(list.getString(0)).isEqualTo(ZONED);
      assertThat(list.getString(1)).isEqualTo(OFFSET);
      assertThat(json.getJSONObject("map").getString("k")).isEqualTo(ZONED);
    }
  }

  /** Regression test for issue #9810. */
  @Test
  void asJsonMethodOnARowKeepsTheZoneAndTheOffsetTime() {
    load();
    // asJSON() on a projected row goes through Result.toJSON(); the text round trips through a serialized JSON string too
    try (final ResultSet rs = database.query("sql", "SELECT $current.asJSON() AS j FROM (SELECT z, t FROM Z)")) {
      final JSONObject json = new JSONObject(rs.next().<Object>getProperty("j").toString());
      assertThat(json.getString("z")).isEqualTo(ZONED);
      assertThat(json.getString("t")).isEqualTo(OFFSET);
    }
  }

  /** Regression test for issue #9810. */
  @Test
  void aTimeOfDayWrittenToAJsonObjectKeepsItsTextInsteadOfNull() {
    final JSONObject json = new JSONObject().put("t", OffsetTime.parse(OFFSET)).put("l", LocalTime.parse("07:15:30"));
    assertThat(json.getString("t")).isEqualTo(OFFSET);
    assertThat(json.getString("l")).isEqualTo("07:15:30");
  }

  /**
   * Regression test for issue #9810.
   * <p>
   * The Map arm recurses now: what it did for a nested Map, a List, a null and plain scalars must not change.
   */
  @Test
  void aMapKeepsItsNestedMapListNullAndScalarValues() {
    final Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("x", "y");
    inner.put("t", OffsetTime.parse(OFFSET));
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("n", null);
    map.put("i", 1);
    map.put("b", true);
    map.put("s", "text");
    map.put("r", new RID(1, 0));
    map.put("m", inner);
    map.put("l", List.of(1, 2));

    final JSONObject json = (JSONObject) new ResultInternal(database).valueToJSON(map);

    assertThat(json.toString()).isEqualTo(
        "{\"n\":null,\"i\":1,\"b\":true,\"s\":\"text\",\"r\":\"#1:0\",\"m\":{\"x\":\"y\",\"t\":\"" + OFFSET + "\"},\"l\":[1,2]}");
  }

  /**
   * Regression test for issue #9810.
   * <p>
   * A null key is written as the text "null" rather than refusing the whole map: a deliberate contract, pinned here.
   */
  @Test
  void aMapWithANullKeyIsWrittenWithTheTextNullAsItsKey() {
    final Map<String, Object> map = new HashMap<>();
    map.put(null, "orphan");

    final JSONObject json = (JSONObject) new ResultInternal(database).valueToJSON(map);

    assertThat(json.getString("null")).isEqualTo("orphan");
  }

  /** Regression test for issue #9810. */
  @Test
  void valueToJsonRendersTheNativeTemporalsAsText() {
    final Result row = new ResultInternal(database);
    assertThat(row.valueToJSON(ZonedDateTime.parse(ZONED))).isEqualTo(ZONED);
    assertThat(row.valueToJSON(OffsetTime.parse(OFFSET))).isEqualTo(OFFSET);
    assertThat(row.valueToJSON(LocalTime.parse("07:15:30"))).isEqualTo("07:15:30");
    assertThat(row.valueToJSON(CypherDuration.parse("PT1H"))).isEqualTo(CypherDuration.parse("PT1H").toString());

    final JSONObject nested = (JSONObject) row.valueToJSON(Map.of("k", List.of(OffsetTime.parse(OFFSET))));
    assertThat(nested.getJSONArray("k").getString(0)).isEqualTo(OFFSET);
  }
}
