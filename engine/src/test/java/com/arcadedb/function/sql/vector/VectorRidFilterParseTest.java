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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins what the {@code filter} option of {@code vector.neighbors} makes of the list it is given (the allow-list of the filtered
 * k-NN search that modelhub issues on every hybrid search): the RID strings it accepts, the way it refuses the others, what it
 * does with duplicates, RIDs of another bucket, nulls and an empty list, and that the strings and the RIDs they spell give the
 * same rows in the same order.
 */
class VectorRidFilterParseTest extends TestHelper {
  private static final int DOCS = 40;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }""");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.newVertex("Doc").set("name", "doc" + i)
            .set("embedding", new float[] { 1.0f, 0.02f * i, 0.5f - 0.01f * i }).save();
    });
  }

  private List<RID> allRids() {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc ORDER BY name")) {
      while (rs.hasNext())
        rids.add((RID) rs.next().getProperty("rid"));
    }
    return rids;
  }

  private Set<RID> parse(final List<?> items) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return SQLFunctionVectorAbstract.parseRidFilter(items, "vector.neighbors", context);
  }

  private static List<String> strings(final List<RID> rids) {
    final List<String> out = new ArrayList<>(rids.size());
    for (final RID rid : rids)
      out.add(rid.toString());
    return out;
  }

  @Test
  void stringsGiveTheSetOfTheRidsTheySpell() {
    final List<RID> rids = allRids();
    final Set<RID> parsed = parse(strings(rids));
    assertThat(parsed).hasSize(DOCS).containsExactlyInAnyOrderElementsOf(rids);
    for (final RID rid : parsed)
      assertThat(rid).isInstanceOf(DatabaseRID.class);
    for (final RID rid : parsed)
      assertThat(((DatabaseRID) rid).getBoundDatabase()).isSameAs(database);
  }

  @Test
  void absentOptionIsNoFilterAndAnEmptyListIsAnEmptySet() {
    assertThat(parse(null)).isNull();
    final Set<RID> empty = parse(List.of());
    assertThat(empty).isNotNull().isEmpty();
  }

  @Test
  void nullsAreSkipped() {
    final List<Object> items = new ArrayList<>();
    items.add(null);
    items.add("#1:2");
    items.add(null);
    items.add(new RID(3, 4));
    assertThat(parse(items)).containsExactlyInAnyOrder(new RID(1, 2), new RID(3, 4));
    final List<Object> onlyNulls = new ArrayList<>();
    onlyNulls.add(null);
    assertThat(parse(onlyNulls)).isEmpty();
  }

  @Test
  void duplicatesCollapseWhateverTheirSpelling() {
    final Set<RID> parsed = parse(Arrays.asList("#1:2", "#1:2", new RID(1, 2), "#01:002", "#+1:+2", "#1:2:", "#3:4", "#3:4"));
    assertThat(parsed).containsExactlyInAnyOrder(new RID(1, 2), new RID(3, 4));
  }

  @Test
  void ridsOfAnotherBucketAndOfNoRecordAreKept() {
    final Set<RID> parsed = parse(Arrays.asList("#9999:0", "#0:123456789", "#2147483647:9223372036854775807"));
    assertThat(parsed).containsExactlyInAnyOrder(new RID(9999, 0), new RID(0, 123456789), new RID(Integer.MAX_VALUE, Long.MAX_VALUE));
  }

  @Test
  void oneLevelOfNestingIsFlattenedForStringsToo() {
    final Set<RID> parsed = parse(Arrays.asList("#1:2", List.of("#3:4", "#5:6"), List.of()));
    assertThat(parsed).containsExactlyInAnyOrder(new RID(1, 2), new RID(3, 4), new RID(5, 6));
    assertThatThrownBy(() -> parse(List.of(List.of(List.of("#1:2")))))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must contain RIDs");
  }

  @Test
  void aStringIsRefusesIsAParsingErrorNamingTheToken() {
    for (final String token : new String[] { "docA", "#1", "#", "#1:", "#a:b", "#1:2:3", "#1::2", " #1:2", "#1:2 ", "1:2", "", "#:",
        "#1:2\n", "##1:2" })
      assertThatThrownBy(() -> parse(Arrays.asList("#7:7", token)))
          .as(token)
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessage("Option 'filter' for function 'vector.neighbors' must contain RIDs, got: '" + token + "'");
  }

  @Test
  void aStringIsAcceptsButCannotBeConvertedIsANumberFormatError() {
    for (final String token : new String[] { "#:12", "#--1:2", "#1+2:3", "#1:2-3", "#2147483648:1", "#99999999999:1",
        "#1:9223372036854775808" })
      assertThatThrownBy(() -> parse(Arrays.asList("#7:7", token))).as(token).isInstanceOf(NumberFormatException.class);
  }

  @Test
  void aValueThatIsNotARidIsRefusedWithItsClass() {
    assertThatThrownBy(() -> parse(List.of(12))).isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("Option 'filter' for function 'vector.neighbors' must contain RIDs, got: Integer");
  }

  @Test
  void stringsAndRidsFilterToTheSameRowsInTheSameOrder() {
    final List<RID> rids = allRids();
    final List<RID> picked = new ArrayList<>();
    for (int i = 0; i < rids.size(); i += 3)
      picked.add(rids.get(i));
    final List<Object> asStrings = new ArrayList<>(strings(picked));
    asStrings.add(1, "#9999:5");
    asStrings.add(picked.size() / 2, strings(picked).getFirst());

    final List<Map<String, Object>> viaStrings = search(asStrings, 10);
    final List<Map<String, Object>> viaRids = search(new ArrayList<>(picked), 10);
    assertThat(viaStrings).isNotEmpty();
    assertThat(viaStrings).isEqualTo(viaRids);
    for (final Map<String, Object> row : viaStrings)
      assertThat(picked).contains((RID) row.get("rid"));

    // THE SCORING SHAPE OF THE HYBRID SEARCH: k = the length of the list
    assertThat(search(asStrings, picked.size())).isEqualTo(search(new ArrayList<>(picked), picked.size()))
        .hasSize(picked.size());
  }

  @Test
  void anEmptyOrUnmatchedListAnswersNoRows() {
    assertThat(search(List.of(), 5)).isEmpty();
    assertThat(search(List.of("#9999:1", "#9999:2"), 5)).isEmpty();
  }

  @Test
  void aMalformedStringFailsTheQueryAndAValidPrefixAnswersNothing() {
    assertThatThrownBy(() -> search(Arrays.asList(allRids().getFirst().toString(), "#nope"), 5))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must contain RIDs");
  }

  private List<Map<String, Object>> search(final List<?> filter, final int k) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name, @rid AS rid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', [1.0, 0.1, 0.4], :k, "
            + "{ filter: :f })))", Map.of("k", k, "f", filter))) {
      while (rs.hasNext()) {
        final var row = rs.next();
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", row.getProperty("name"));
        m.put("rid", row.getProperty("rid"));
        m.put("distance", row.getProperty("distance"));
        rows.add(m);
      }
    }
    return rows;
  }

  @Test
  void theSetOfALargeStringListEqualsTheSetOfItsRids() {
    final List<RID> rids = allRids();
    final Set<RID> expected = new HashSet<>(rids);
    final List<String> many = new ArrayList<>();
    for (int round = 0; round < 50; round++)
      many.addAll(strings(rids));
    assertThat(parse(many)).isEqualTo(expected);
  }
}
