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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LegacyKeyIndexes;
import com.arcadedb.query.sql.executor.QueryOperatorEquals;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #12711: a value a protocol read as a list from text whose type the client left unspecified (a Postgres
 * parameter of OID 0 that is a well-formed array literal) is the text it was sent as wherever it becomes a STRING: stored into a
 * STRING property, or compared with a string. Where a list is wanted it stays the list.
 */
class SourceTextConversionIssue12711Test extends TestHelper {

  private static final class ListFromText extends ArrayList<Object> implements SourceText {
    private final String text;

    private ListFromText(final String text, final List<Object> elements) {
      super(elements);
      this.text = text;
    }

    @Override
    public String sourceText() {
      return text;
    }
  }

  @Test
  void convertedToAStringItIsTheTextItWasReadFrom() {
    final DocumentType type = database.getSchema().createDocumentType("T12711");
    final Property property = type.createProperty("s", Type.STRING);

    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    assertThat(Type.convert(database, value, String.class, property)).isEqualTo("{a,b}");
    // without a property: the conversion a comparison with a string makes
    assertThat(Type.convert(database, value, String.class)).isEqualTo("{a,b}");
    assertThat(Type.convert(database, new ListFromText("{}", List.of()), String.class, property)).isEqualTo("{}");
    // a plain list keeps its own rendering
    assertThat(Type.convert(database, List.of("a", "b"), String.class, property)).isEqualTo("[a, b]");
  }

  @Test
  void comparedWithAStringItIsTheTextItWasReadFrom() {
    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    assertThat(QueryOperatorEquals.equals("{a,b}", value)).isTrue();
    assertThat(QueryOperatorEquals.equals("[a, b]", value)).isFalse();
    // against a list it is still the list it was read as
    assertThat(QueryOperatorEquals.equals(List.of("a", "b"), value)).isTrue();
  }

  @Test
  void aStringPropertyStoresTheTextAndAnUndeclaredOneTheList() {
    database.getSchema().createDocumentType("T12711").createProperty("s", Type.STRING);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T12711 SET k = 1, s = ?", new ListFromText("{name}", List.of("name")));
      database.command("sql", "INSERT INTO T12711 SET k = 2, u = ?", new ListFromText("{a,b}", List.of("a", "b")));
    });

    try (final ResultSet rs = database.query("sql", "SELECT k FROM T12711 WHERE s = ?", new ListFromText("{name}", List.of("name")))) {
      assertThat(rs.next().<Integer>getProperty("k")).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT s, u, u.type() AS t FROM T12711 ORDER BY k")) {
      assertThat(rs.next().<Object>getProperty("s")).isEqualTo("{name}");
      final Result second = rs.next();
      assertThat(second.<String>getProperty("t")).isEqualTo("LIST");
      assertThat(second.<List<Object>>getProperty("u")).containsExactly("a", "b");
    }
  }

  /** The comparison answers the same whichever side the parameter stands on. */
  @Test
  void comparedWithAStringOnEitherSide() {
    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    assertThat(QueryOperatorEquals.equals(value, "{a,b}")).isTrue();
    assertThat(QueryOperatorEquals.equals(value, "[a, b]")).isFalse();
    assertThat(QueryOperatorEquals.equals(value, List.of("a", "b"))).isTrue();

    database.getSchema().createDocumentType("T12711").createProperty("s", Type.STRING);
    database.transaction(() -> database.command("sql", "INSERT INTO T12711 SET k = 1, s = '{a,b}'"));
    assertThat(keys("sql", "SELECT k FROM T12711 WHERE ? = s", value)).containsExactly(1);
    assertThat(keys("sql", "SELECT k FROM T12711 WHERE s <> ?", value)).isEmpty();
    assertThat(keys("sql", "SELECT k FROM T12711 WHERE s IN [?]", value)).containsExactly(1);
  }

  /** A declared STRING property is looked up under the text, by every index kind that keys it. */
  @Test
  void aDeclaredStringIndexIsLookedUpUnderTheText() {
    final DocumentType type = database.getSchema().createDocumentType("T12711");
    type.createProperty("l", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    type.createProperty("h", Type.STRING).createIndex(Schema.INDEX_TYPE.HASH, false);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T12711 SET k = 1, l = ?, h = ?", new ListFromText("{a,b}", List.of("a", "b")),
          new ListFromText("{a,b}", List.of("a", "b")));
      database.command("sql", "INSERT INTO T12711 SET k = 2, l = '[a, b]', h = '[a, b]'");
    });

    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    assertThat(keys("sql", "SELECT k FROM T12711 WHERE l = ?", value)).containsExactly(1);
    assertThat(keys("sql", "SELECT k FROM T12711 WHERE h = ?", value)).containsExactly(1);

    // the entry is the one the stored text has: gone with the record
    database.transaction(() -> database.command("sql", "DELETE FROM T12711 WHERE k = 1"));
    assertThat(database.getSchema().getIndexByName("T12711[l]").countEntries()).isEqualTo(1L);
    assertThat(database.getSchema().getIndexByName("T12711[h]").countEntries()).isEqualTo(1L);
  }

  /**
   * An index on a property nothing declares is STRING-keyed and keys a list by the text of its elements (issue #11578). A list read
   * from text is that list there: keyed as the list it is stored as, so the entry written from the parameter is the one a delete of
   * the reloaded record removes, and a lookup with the parameter finds the stored list as the scan does.
   */
  @Test
  void anIndexOnAnUndeclaredPropertyKeysItAsTheList() {
    database.transaction(() -> database.command("opencypher", "CREATE (:K12711 {id: 0, u: ['x']})"));
    database.command("opencypher", "CREATE INDEX FOR (n:K12711) ON (n.u)");
    assertThat(database.getSchema().getType("K12711").getPropertyIfExists("u")).as("the property stays undeclared").isNull();

    database.transaction(
        () -> database.command("sql", "CREATE VERTEX K12711 SET id = 1, u = ?", new ListFromText("{a,b}", List.of("a", "b"))));

    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    // the index seek and the scan agree
    assertThat(cypherKeys("MATCH (n:K12711) WHERE n.u = $p RETURN n.id AS k", value)).containsExactly(1);
    assertThat(keys("sql", "SELECT id AS k FROM K12711 WHERE u = ?", value)).containsExactly(1);
    assertThat(keys("sql", "SELECT id AS k FROM K12711 WHERE u.size() > 0 AND u = ?", value)).containsExactly(1);

    final Index index = database.getSchema().getIndexByName("K12711[u]");
    assertThat(index.countEntries()).isEqualTo(2L);
    database.transaction(() -> database.command("sql", "DELETE FROM K12711 WHERE id = 1"));
    assertThat(index.countEntries()).as("no entry is left behind for the deleted record").isEqualTo(1L);
    assertThat(cypherKeys("MATCH (n:K12711) WHERE n.u = $p RETURN n.id AS k", value)).isEmpty();
  }

  /**
   * An index an older build wrote is looked up under the text that build gave a list too (issue #12160): the text of its elements,
   * which for a list of strings is the key it has today, so there is no second key to look under.
   */
  @Test
  void anIndexOfAnOlderBuildHasNoOtherKeyForIt() throws IOException {
    database.transaction(() -> database.command("opencypher", "CREATE (:K12711 {id: 1, u: ['a', 'b']})"));
    database.command("opencypher", "CREATE INDEX FOR (n:K12711) ON (n.u)");
    database.close();
    LegacyKeyIndexes.forgetCanonicalKeys(getDatabasePath());
    database = factory.open();

    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    int checked = 0;
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("K12711[u]")).getIndexesOnBuckets()) {
      final LSMTreeIndex index = (LSMTreeIndex) bucketIndex;
      assertThat(index.mayHoldLegacyKeys()).isTrue();
      final Object[] keys = { value };
      assertThat(index.convertLookupKeys(keys)).containsExactly("[a, b]");
      assertThat(index.legacyKeys(keys, index.convertLookupKeys(keys))).isNull();
      ++checked;
    }
    assertThat(checked).isPositive();
    assertThat(cypherKeys("MATCH (n:K12711) WHERE n.u = $p RETURN n.id AS k", value)).containsExactly(1);
  }

  private List<Integer> keys(final String language, final String query, final Object... parameters) {
    try (final ResultSet rs = database.query(language, query, parameters)) {
      return keys(rs);
    }
  }

  private List<Integer> cypherKeys(final String query, final Object parameter) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("p", parameter))) {
      return keys(rs);
    }
  }

  private static List<Integer> keys(final ResultSet rs) {
    final List<Integer> keys = new ArrayList<>();
    while (rs.hasNext())
      keys.add(rs.next().<Number>getProperty("k").intValue());
    return keys;
  }
}
