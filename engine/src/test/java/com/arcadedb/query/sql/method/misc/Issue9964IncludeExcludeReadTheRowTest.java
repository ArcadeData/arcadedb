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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9964: {@code include()} and {@code exclude()} read a projected row's BACKING RECORD instead of the
 * row, through the element-first {@link Result#toMap()}: an overridden alias answered the stored value, an excluded column came
 * back, and so did an excluded {@code @rid} (#7895). The row answers for a name, as {@code getProperty()},
 * {@code getPropertyNames()}, {@code keys()}, {@code values()} and {@code toJSON()} already have it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class Issue9964IncludeExcludeReadTheRowTest extends TestHelper {

  private RID rid;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("D1");
    database.getSchema().createVertexType("V1");
    database.getSchema().createEdgeType("E1");
    database.transaction(() -> {
      rid = database.newDocument("D1").set("s", "x", "n", 1, "m", 2).save().getIdentity();
      final var a = database.newVertex("V1").set("s", "a").save();
      final var b = database.newVertex("V1").set("s", "b").save();
      a.newEdge("E1", b, "w", 7);
    });
  }

  @Test
  void anOverriddenAliasAnswersTheRowValue() {
    assertThat(map("select (select *, 'OVERRIDE' as s from D1)[0].include('s') as c")).containsOnly(Map.entry("s", "OVERRIDE"));
    assertThat(map("select (select *, 'OVERRIDE' as s from D1)[0].exclude('n') as c")).containsEntry("s", "OVERRIDE")
        .containsEntry("m", 2).doesNotContainKey("n");
  }

  @Test
  void anExcludedColumnStaysExcluded() {
    final Map<String, Object> included = map("select (select *, !m from D1)[0].include('m') as c");
    assertThat(included).containsOnlyKeys("m").containsEntry("m", null);
    assertThat(map("select (select *, !m from D1)[0].exclude('n') as c")).containsEntry("s", "x").doesNotContainKeys("m", "n");
  }

  @Test
  void anExcludedRidStaysExcluded() {
    assertThat(map("select (select *, !@rid from D1)[0].include('@rid') as c")).containsOnlyKeys("@rid").containsEntry("@rid", null);
    assertThat(map("select (select *, !@rid from D1)[0].exclude('n') as c")).doesNotContainKeys("@rid", "n");
    // the attributes the projection did not exclude are still there when a metadata name is asked for
    assertThat(map("select (select *, !@rid from D1)[0].include('@type', '@cat') as c")).containsEntry("@type", "D1")
        .containsEntry("@cat", "d");
  }

  @Test
  void aWholeRecordRowStillAnswersItsMetadataWhenNamed() {
    // a bare SELECT keeps no content of its own: the row IS the record, so its attributes answer when named, and only then
    final Map<String, Object> withRid = map("select (select from D1)[0].include('@rid', 's') as c");
    assertThat(withRid).containsOnlyKeys("@rid", "s").containsEntry("s", "x");
    assertThat(withRid.get("@rid")).hasToString(rid.toString());
    assertThat(map("select (select from D1)[0].exclude('n') as c")).containsOnlyKeys("s", "m");

    final Map<String, Object> edge = map("select (select from E1)[0].include('@in', '@out', 'w') as c");
    assertThat(edge).containsOnlyKeys("@in", "@out", "w").containsEntry("w", 7);
    assertThat(edge.get("@in")).isNotNull();
    assertThat(edge.get("@out")).isNotNull();
  }

  @Test
  void aCollectionOfProjectedRowsIsReadRowByRow() {
    final Object value = single("select (select *, 'OVERRIDE' as s, !m from D1).include('s', 'm') as c");
    assertThat((List<Map<String, Object>>) value).singleElement()
        .satisfies(m -> assertThat(m).containsEntry("s", "OVERRIDE").containsEntry("m", null));
  }

  /**
   * The guarantee rather than the correction: on every row a query produces, every method that projects the row's properties
   * agrees with {@code getProperty(name)} for every name in {@code getPropertyNames()}.
   */
  @Test
  void everyProjectingMethodAgreesWithTheRow() {
    for (final String query : List.of(//
        "select from D1",//
        "select * from D1",//
        "select *, 'OVERRIDE' as s from D1",//
        "select *, !m from D1",//
        "select *, !@rid from D1",//
        "select *, n + 1 as z from D1",//
        "select s, n from D1",//
        "select 'OVERRIDE' as s, n from D1",//
        "select from E1",//
        "select *, !w from E1")) {
      final Result row = firstRow(query);
      final List<String> names = new ArrayList<>(row.getPropertyNames());
      final Map<String, Object> expected = new LinkedHashMap<>();
      for (final String name : names)
        expected.put(name, row.getProperty(name));

      final String inner = "(" + query + ")[0]";
      final String quotedNames = names.isEmpty() ? "'-'" : "'" + String.join("', '", names) + "'";

      assertThat(map("select " + inner + ".include(" + quotedNames + ") as c")).as(query + " include()")
          .containsExactlyInAnyOrderEntriesOf(expected);
      // excluding a name the row does not have leaves the row's own properties
      final Map<String, Object> excluded = map("select " + inner + ".exclude('-no-such-') as c");
      assertThat(excluded).as(query + " exclude()").containsExactlyInAnyOrderEntriesOf(expected);

      assertThat((Collection<String>) single("select " + inner + ".keys() as c")).as(query + " keys()").containsExactlyElementsOf(names);
      assertThat((Collection<Object>) single("select " + inner + ".values() as c")).as(query + " values()").hasSize(names.size());

      final JSONObject json = row.toJSON();
      for (final String name : names)
        assertThat(json.has(name)).as(query + " toJSON() has " + name).isTrue();
    }
  }

  /**
   * {@code ResultInternal.toPropertyMap} walks the row's store directly: it must answer exactly what
   * {@code getPropertyNames()} and {@code getProperty()} do, tombstones included, whichever store answers.
   */
  @Test
  void toPropertyMapAgreesWithTheRowGettersOnEveryStoreShape() {
    database.transaction(() -> {
      final Document record = rid.asDocument();

      final ResultInternal elementOnly = new ResultInternal(record);
      final ResultInternal elementWithTombstone = new ResultInternal(record);
      elementWithTombstone.removeProperty("m");
      final ResultInternal contentAndElement = new ResultInternal(database);
      contentAndElement.setElement(record);
      contentAndElement.setProperty("s", "OVERRIDE");
      contentAndElement.setProperty("z", rid);
      final ResultInternal contentWithTombstone = new ResultInternal(database);
      contentWithTombstone.setElement(record);
      contentWithTombstone.setProperty("s", "x2");
      contentWithTombstone.setProperty("n", 9);
      contentWithTombstone.removeProperty("n");
      final ResultInternal contentOnly = new ResultInternal(Map.of("a", 1));

      for (final ResultInternal row : List.of(elementOnly, elementWithTombstone, contentAndElement, contentWithTombstone, contentOnly)) {
        final Map<String, Object> expected = new LinkedHashMap<>();
        for (final String name : row.getPropertyNames())
          expected.put(name, row.getProperty(name));
        assertThat(row.toPropertyMap(false)).as(row.toString()).containsExactlyEntriesOf(expected);
      }
      assertThat(elementWithTombstone.toPropertyMap(false)).doesNotContainKey("m");
    });
  }

  private Result firstRow(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next();
    }
  }

  private Map<String, Object> map(final String sql) {
    final Object value = single(sql);
    assertThat(value).as(sql).isInstanceOf(Map.class);
    return (Map<String, Object>) value;
  }

  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).isTrue();
      final Object value = rs.next().getProperty("c");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }
}
