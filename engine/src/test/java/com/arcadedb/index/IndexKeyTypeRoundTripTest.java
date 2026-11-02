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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests on how index keys of every type are stored, compared and round-tripped: collection items, DECIMAL,
 * signed-zero FLOAT, zoned temporals, accented strings, array-valued tuple elements and keys changed away and back.
 * Each nested class keeps the fixture of the issue it was written for.
 */
class IndexKeyTypeRoundTripTest {

  /**
   * A BY ITEM list index was keyed as STRING whatever the declared item type, so {@code CONTAINS 7} missed a stored
   * {@code 7.0} that the scan finds, and {@code CONTAINSVALUE} on a map compared with {@code Map.containsValue}, missing
   * the other way (issue #8890). The indexed and the not indexed property must answer alike.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#8890 BY ITEM and BY VALUE collection index key type")
  class CollectionIndexKeyType extends TestHelper {
    private int typeCounter = 0;

    private long count(final String sql, final Object... args) {
      return database.query("sql", sql, args).stream().count();
    }

    private void assertIndexedAndScanAgree(final String listType, final Object stored, final long expected, final Object... operands) {
      final String type = "L" + typeCounter++;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a " + listType);
      database.command("sql", "CREATE PROPERTY " + type + ".b " + listType);
      database.command("sql", "CREATE INDEX ON " + type + " (a BY ITEM) NOTUNIQUE");
      database.transaction(() -> database.newDocument(type).set("a", new ArrayList<>(List.of(stored)), "b", new ArrayList<>(List.of(stored))).save());

      for (final Object operand : operands) {
        assertThat(count("SELECT FROM " + type + " WHERE a CONTAINS ?", operand)).as("%s indexed, operand %s (%s)", listType, operand,
            operand.getClass().getSimpleName()).isEqualTo(expected);
        assertThat(count("SELECT FROM " + type + " WHERE b CONTAINS ?", operand)).as("%s scan, operand %s (%s)", listType, operand,
            operand.getClass().getSimpleName()).isEqualTo(expected);
      }
    }

    @Test
    void listOfDoubleFoundByAnyNumber() {
      assertIndexedAndScanAgree("LIST OF DOUBLE", 7.0, 1, 7, 7L, 7.0, new BigDecimal("7"), "7");
    }

    @Test
    void listOfIntegerAndLongFoundByADouble() {
      assertIndexedAndScanAgree("LIST OF INTEGER", 7, 1, 7, 7L, 7.0, new BigDecimal("7.0"));
      assertIndexedAndScanAgree("LIST OF LONG", 7L, 1, 7, 7L, 7.0, new BigDecimal("7.0"));
    }

    @Test
    void listOfDecimalFoundByADouble() {
      assertIndexedAndScanAgree("LIST OF DECIMAL", new BigDecimal("19.90"), 1, new BigDecimal("19.9"), 19.9d);
    }

    @Test
    void listOfDatetimeFoundByTheSameMoment() {
      final Date moment = new Date(1_790_858_096_789L);
      assertIndexedAndScanAgree("LIST OF DATETIME", moment, 1, moment, new Date(moment.getTime()));
    }

    @Test
    void aValueNotInTheListIsStillNotFound() {
      assertIndexedAndScanAgree("LIST OF DOUBLE", 7.0, 0, 8, 8.5, "abc");
    }

    @Test
    void listOfStringAndUntypedListKeepWorking() {
      assertIndexedAndScanAgree("LIST OF STRING", "x", 1, "x");
      assertIndexedAndScanAgree("LIST", "x", 1, "x");
    }

    @Test
    void indexedKeyTypeIsTheDeclaredItemType() {
      database.command("sql", "CREATE DOCUMENT TYPE K");
      database.command("sql", "CREATE PROPERTY K.a LIST OF DOUBLE");
      database.command("sql", "CREATE PROPERTY K.u LIST");
      database.command("sql", "CREATE INDEX ON K (a BY ITEM) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON K (u BY ITEM) NOTUNIQUE");
      assertThat(database.getSchema().getType("K").getPolymorphicIndexByProperties("a by item").getKeyTypes())
          .containsExactly(Type.DOUBLE);
      assertThat(database.getSchema().getType("K").getPolymorphicIndexByProperties("u by item").getKeyTypes())
          .containsExactly(Type.STRING);
    }

    @Test
    void mapContainsValueAnswersTheSameWithAndWithoutIndex() {
      database.command("sql", "CREATE DOCUMENT TYPE M");
      database.command("sql", "CREATE PROPERTY M.m MAP OF DOUBLE");
      database.command("sql", "CREATE PROPERTY M.n MAP OF DOUBLE");
      database.command("sql", "CREATE INDEX ON M (m BY VALUE) NOTUNIQUE");
      database.transaction(() -> database.newDocument("M").set("m", new HashMap<>(Map.of("x", 7.0)), "n", new HashMap<>(Map.of("x", 7.0))).save());

      for (final Object operand : new Object[] { 7.0, 7, 7L, "7" }) {
        assertThat(count("SELECT FROM M WHERE m CONTAINSVALUE ?", operand)).as("indexed %s", operand).isEqualTo(1);
        assertThat(count("SELECT FROM M WHERE n CONTAINSVALUE ?", operand)).as("scan %s", operand).isEqualTo(1);
      }
      for (final Object operand : new Object[] { 8, 8.5, "abc" })
        assertThat(count("SELECT FROM M WHERE n CONTAINSVALUE ?", operand)).as("scan %s", operand).isEqualTo(0);
    }

    @Test
    void rangeLookupsByAnUnreadableKeyAnswerAsTheScanDoes() {
      database.command("sql", "CREATE DOCUMENT TYPE W");
      database.command("sql", "CREATE PROPERTY W.i INTEGER");
      database.command("sql", "CREATE PROPERTY W.j INTEGER");
      database.command("sql", "CREATE INDEX ON W (i) NOTUNIQUE");
      database.transaction(() -> database.newDocument("W").set("i", 7, "j", 7).save());
      for (final String op : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertThat(count("SELECT FROM W WHERE i " + op + " ?", "7.0")).as(op).isEqualTo(count("SELECT FROM W WHERE j " + op + " ?", "7.0"));
      assertThat(count("SELECT FROM W WHERE i BETWEEN ? AND ?", "6.0", "8.0"))
          .isEqualTo(count("SELECT FROM W WHERE j BETWEEN ? AND ?", "6.0", "8.0"));
      assertThat(count("SELECT FROM W WHERE i IN ?", List.of("7.0", "x"))).isEqualTo(count("SELECT FROM W WHERE j IN ?", List.of("7.0", "x")));
    }

    @Test
    void theIndexApiAnswersNoRowForAnUnreadableKey() {
      database.command("sql", "CREATE DOCUMENT TYPE Z");
      database.command("sql", "CREATE PROPERTY Z.i INTEGER");
      database.command("sql", "CREATE INDEX ON Z (i) NOTUNIQUE");
      database.transaction(() -> database.newDocument("Z").set("i", 7).save());
      final Index index = database.getSchema().getType("Z").getPolymorphicIndexByProperties("i");
      assertThat(index.get(new Object[] { "7.0" }).hasNext()).isFalse();
      assertThat(index.get(new Object[] { "7" }).hasNext()).isTrue();
    }

    @Test
    void listOfBooleanIsLookedUpWithAndWithoutTheIndex() {
      assertIndexedAndScanAgree("LIST OF BOOLEAN", true, 1, true, "true");
    }

    @Test
    void aListItemOfAnotherTypeIsStillIndexed() {
      database.command("sql", "CREATE DOCUMENT TYPE X");
      database.command("sql", "CREATE PROPERTY X.a LIST OF INTEGER");
      database.command("sql", "CREATE INDEX ON X (a BY ITEM) NOTUNIQUE");
      database.transaction(() -> database.newDocument("X").set("a", new ArrayList<>(List.of(7, 8L, "9"))).save());
      assertThat(count("SELECT FROM X WHERE a CONTAINS 7")).isEqualTo(1);
      assertThat(count("SELECT FROM X WHERE a CONTAINS 9")).isEqualTo(1);
    }

    @Test
    void aListItemThatCannotBeReadAsTheDeclaredTypeIsRefusedWithOrWithoutTheIndex() {
      // the declared OF type is enforced when the record is saved, before any index sees the item: typing the BY ITEM key
      // narrows nothing that could be written before
      database.command("sql", "CREATE DOCUMENT TYPE Y");
      database.command("sql", "CREATE PROPERTY Y.a LIST OF INTEGER");
      database.command("sql", "CREATE PROPERTY Y.b LIST OF INTEGER");
      database.command("sql", "CREATE INDEX ON Y (a BY ITEM) NOTUNIQUE");
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Y").set("a", new ArrayList<>(List.of(7, "abc"))).save()))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Y").set("b", new ArrayList<>(List.of(7, "abc"))).save()))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rebuiltIndexKeepsTheDeclaredKeyType() {
      database.command("sql", "CREATE DOCUMENT TYPE R");
      database.command("sql", "CREATE PROPERTY R.a LIST OF DOUBLE");
      database.command("sql", "CREATE INDEX ON R (a BY ITEM) NOTUNIQUE");
      database.transaction(() -> database.newDocument("R").set("a", new ArrayList<>(List.of(7.0, 8.5))).save());
      database.command("sql", "REBUILD INDEX *");
      assertThat(count("SELECT FROM R WHERE a CONTAINS 7")).isEqualTo(1);
      assertThat(count("SELECT FROM R WHERE a CONTAINS 8.5")).isEqualTo(1);
    }
  }

  /**
   * Regression guard for #8972: keys that compare equal but serialize to different lengths (DECIMAL 5.00 and 5) made the LSM retrieve
   * read the values of the run of equal keys from inside the key bytes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#8972 DECIMAL keys of different serialized length in a NOTUNIQUE index")
  class DecimalNotUniqueIndex extends TestHelper {

    private void create(final String type) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".amount DECIMAL");
      database.command("sql", "CREATE INDEX ON " + type + " (amount) NOTUNIQUE");
    }

    private List<String> names(final String type) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE amount = 5")) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      return names;
    }

    @Test
    void wideScaleThenNarrow() {
      create("A");
      database.transaction(() -> {
        database.newDocument("A").set("name", "apple", "amount", new BigDecimal("5.00")).save();
        database.newDocument("A").set("name", "pear", "amount", new BigDecimal("4.00")).save();
      });
      database.transaction(() -> {
        database.newDocument("A").set("name", "plum", "amount", new BigDecimal("5")).save();
        database.newDocument("A").set("name", "fig", "amount", new BigDecimal("6")).save();
      });

      assertThat(names("A")).containsExactlyInAnyOrder("apple", "plum");
      final IndexCursor cursor = database.getSchema().getIndexByName("A[amount]").get(new Object[] { new BigDecimal("5") });
      final List<RID> rids = new ArrayList<>();
      while (cursor.hasNext())
        rids.add(cursor.next().getIdentity());
      assertThat(rids).hasSize(2);
      for (final RID rid : rids)
        assertThat(new BigDecimal(database.lookupByRID(rid, true).asDocument().get("amount").toString())).isEqualByComparingTo("5");
    }

    @Test
    void narrowScaleThenWideAndDelete() {
      create("B");
      database.transaction(() -> database.newDocument("B").set("name", "kiwi", "amount", new BigDecimal("5")).save());
      database.transaction(() -> database.newDocument("B").set("name", "lime", "amount", new BigDecimal("5.00")).save());
      database.transaction(() -> database.newDocument("B").set("name", "nut", "amount", new BigDecimal("5.0")).save());

      assertThat(names("B")).containsExactlyInAnyOrder("kiwi", "lime", "nut");

      database.transaction(() -> database.command("sql", "DELETE FROM B WHERE name = 'lime'"));
      assertThat(names("B")).containsExactlyInAnyOrder("kiwi", "nut");
      assertThat(database.countType("B", false)).isEqualTo(2);
    }

    @Test
    void manyWidthsAcrossTransactionsThenCompacted() throws Exception {
      create("C");
      final String[] widths = { "5", "5.00", "5.0", "5.000", "5.0000000000" };
      for (int i = 0; i < 12; i++) {
        final String amount = widths[i % widths.length];
        final int n = i;
        database.transaction(() -> database.newDocument("C").set("name", "n" + n, "amount", new BigDecimal(amount)).save());
      }
      assertThat(names("C")).hasSize(12);

      final Index index = database.getSchema().getIndexByName("C[amount]");
      ((IndexInternal) index).compact();
      assertThat(names("C")).hasSize(12);

      final IndexCursor cursor = index.get(new Object[] { new BigDecimal("5") });
      final Set<RID> found = new HashSet<>();
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        assertThat(database.lookupByRID(rid, true)).isNotNull();
        found.add(rid);
      }
      // a wrong value offset could return the same record twice
      assertThat(found).hasSize(12);
    }

    @Test
    void compositeIndexWithADecimalPart() {
      database.command("sql", "CREATE DOCUMENT TYPE Comp");
      database.command("sql", "CREATE PROPERTY Comp.name STRING");
      database.command("sql", "CREATE PROPERTY Comp.amount DECIMAL");
      database.command("sql", "CREATE PROPERTY Comp.tag STRING");
      database.command("sql", "CREATE INDEX ON Comp (name, amount) NOTUNIQUE");
      final String[] widths = { "5", "5.00", "5.0", "5.000" };
      for (int i = 0; i < 8; i++) {
        final String amount = widths[i % widths.length];
        final int n = i;
        database.transaction(() -> database.newDocument("Comp").set("name", "x", "tag", "t" + n, "amount", new BigDecimal(amount)).save());
      }

      final List<String> tags = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT tag FROM Comp WHERE name = 'x' AND amount = 5")) {
        while (rs.hasNext())
          tags.add(rs.next().getProperty("tag"));
      }
      assertThat(tags).containsExactlyInAnyOrder("t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7");
    }
  }

  /**
   * Issue #9033: a FLOAT property holding 0f and -0f under a NOTUNIQUE LSM index made DELETE FROM fail at commit with a
   * NegativeArraySizeException from LSMTreeIndexAbstract.readEntryValues, because the page held the two spellings of one key.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9033 FLOAT 0 and -0 under a NOTUNIQUE index")
  class FloatSignedZeroDelete extends TestHelper {

    @Test
    void deleteFloatZeroAndNegativeZero() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE FI").close();
        database.command("sql", "CREATE PROPERTY FI.x FLOAT").close();
        database.command("sql", "CREATE INDEX ON FI (x) NOTUNIQUE").close();
      });
      database.transaction(() -> {
        database.newDocument("FI").set("x", 0f).save();
        database.newDocument("FI").set("x", -0f).save();
      });
      database.transaction(() -> database.command("sql", "DELETE FROM FI").close());
      assertThat(count("FI")).isZero();
    }

    @Test
    void lookupAndDeleteBothSpellings() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE FJ").close();
        database.command("sql", "CREATE PROPERTY FJ.x FLOAT").close();
        database.command("sql", "CREATE INDEX ON FJ (x) NOTUNIQUE").close();
      });
      database.transaction(() -> {
        database.newDocument("FJ").set("x", 0f).save();
        database.newDocument("FJ").set("x", -0f).save();
        database.newDocument("FJ").set("x", 1.5f).save();
      });
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM FJ WHERE x = 0")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(2L);
      }
      database.transaction(() -> database.command("sql", "DELETE FROM FJ WHERE x = -0.0").close());
      assertThat(count("FJ")).isEqualTo(1L);
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM " + type)) {
        return rs.next().<Number>getProperty("n").longValue();
      }
    }
  }

  /**
   * Issue #9648: the index compared a ZONED_DATETIME (and an OFFSET_TIME) by its instant while the row filter compared it by
   * {@code equals()}, which also requires the zone, so creating or dropping an index changed what a query returned. Both now
   * follow openCypher, as {@code CypherDateTime} already did (#8300): the same instant in two zones is two values, ordered by
   * instant, then offset, then zone name.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9648 zoned temporal key equality with and without an index")
  class ZonedTemporalIndexEquality extends TestHelper {

    private static final ZonedDateTime STORED  = ZonedDateTime.parse("2026-01-01T00:30+02:00");
    private static final ZonedDateTime SAME_UTC = ZonedDateTime.parse("2025-12-31T22:30Z");

    private void load(final String type, final Type propertyType, final String algorithm, final Object... values) {
      database.getSchema().createDocumentType(type).createProperty("z", propertyType);
      if (algorithm != null)
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), false, type, "z");
      database.transaction(() -> {
        for (final Object value : values)
          database.newDocument(type).set("z", value).save();
      });
    }

    private long count(final String type, final Object value) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE z = ?", value)) {
        return rs.next().<Long>getProperty("c");
      }
    }

    private List<Object> sorted(final String type, final String where, final Object value) {
      final List<Object> result = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT z FROM " + type + " WHERE " + where + " ORDER BY z", value)) {
        rs.stream().forEach(r -> result.add(r.getProperty("z")));
      }
      return result;
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        LSM_TREE
        HASH
        """)
    void zonedDateTimeEqualityIsTheSameWithAndWithoutIndex(final String algorithm) {
      assertThat(BinaryComparator.equals(STORED, SAME_UTC)).isFalse();

      load("Indexed", Type.ZONED_DATETIME, algorithm, STORED);
      load("Plain", Type.ZONED_DATETIME, null, STORED);

      for (final Object probe : new Object[] { STORED, SAME_UTC, STORED.toOffsetDateTime() }) {
        final long expected = count("Plain", probe);
        assertThat(count("Indexed", probe)).as("probe " + probe).isEqualTo(expected);
      }
      assertThat(count("Plain", STORED)).isEqualTo(1L);
      assertThat(count("Plain", SAME_UTC)).isZero();
      // the OffsetDateTime of the same zone is the stored value, to the filter as to the index
      assertThat(count("Plain", STORED.toOffsetDateTime())).isEqualTo(1L);

      assertThat(database.getSchema().getIndexByName("Indexed[z]").get(new Object[] { SAME_UTC }).hasNext()).isFalse();
      assertThat(database.getSchema().getIndexByName("Indexed[z]").get(new Object[] { STORED }).hasNext()).isTrue();
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        LSM_TREE
        HASH
        """)
    void offsetTimeEqualityIsTheSameWithAndWithoutIndex(final String algorithm) {
      // 01:30+01:00 and 00:30Z are the same time of day in two offsets
      final OffsetTime stored = OffsetTime.of(1, 30, 0, 0, ZoneOffset.ofHours(1));
      final OffsetTime sameUtc = OffsetTime.of(0, 30, 0, 0, ZoneOffset.UTC);
      load("Indexed", Type.OFFSET_TIME, algorithm, stored);
      load("Plain", Type.OFFSET_TIME, null, stored);

      for (final Object probe : new Object[] { stored, sameUtc })
        assertThat(count("Indexed", probe)).as("probe " + probe).isEqualTo(count("Plain", probe));
      assertThat(count("Indexed", stored)).isEqualTo(1L);
      assertThat(count("Indexed", sameUtc)).isZero();
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        LSM_TREE
        HASH
        """)
    void sameInstantInTwoZonesIsTwoKeys(final String algorithm) {
      load("Indexed", Type.ZONED_DATETIME, algorithm, STORED, SAME_UTC, ZonedDateTime.parse("2025-12-31T23:30+01:00[Europe/Paris]"));
      load("Plain", Type.ZONED_DATETIME, null, STORED, SAME_UTC, ZonedDateTime.parse("2025-12-31T23:30+01:00[Europe/Paris]"));

      assertThat(count("Indexed", STORED)).isEqualTo(1L);
      assertThat(count("Indexed", SAME_UTC)).isEqualTo(1L);
      assertThat(count("Indexed", ZonedDateTime.parse("2025-12-31T23:30+01:00[Europe/Paris]"))).isEqualTo(1L);
      // one instant, three zones: none is the fixed +01:00 offset without the region
      assertThat(count("Indexed", ZonedDateTime.parse("2025-12-31T23:30+01:00"))).isZero();
    }

    @Test
    void rangeOrderIsInstantThenOffsetThenZone() {
      final ZonedDateTime west = ZonedDateTime.parse("2025-12-31T17:30-05:00");
      final ZonedDateTime later = ZonedDateTime.parse("2026-01-01T10:00Z");
      load("Indexed", Type.ZONED_DATETIME, "LSM_TREE", later, STORED, SAME_UTC, west);
      load("Plain", Type.ZONED_DATETIME, null, later, STORED, SAME_UTC, west);

      // the same instant, west to east, then the later instant
      final List<Object> expected = List.of(west, SAME_UTC, STORED, later);
      assertThat(sorted("Plain", "z >= ?", west)).isEqualTo(expected);
      assertThat(sorted("Indexed", "z >= ?", west)).isEqualTo(expected);
      // a bound in one zone of the shared instant keeps the zones east of it and drops those west of it, with or without index
      assertThat(sorted("Indexed", "z > ?", SAME_UTC)).isEqualTo(sorted("Plain", "z > ?", SAME_UTC)).isEqualTo(List.of(STORED, later));
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        LSM_TREE
        HASH
        """)
    void inTransactionAndPersistedKeysAgree(final String algorithm) {
      // the pending entries of a transaction are keyed by the untyped compareTo, the persisted ones by the typed comparator:
      // both must tell the zones of one instant apart
      database.getSchema().createDocumentType("Tx").createProperty("z", Type.ZONED_DATETIME);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), false, "Tx", "z");
      final ZonedDateTime west = ZonedDateTime.parse("2025-12-31T17:30-05:00");

      database.begin();
      for (final ZonedDateTime value : List.of(STORED, SAME_UTC, west))
        database.newDocument("Tx").set("z", value).save();
      final long[] pending = { count("Tx", STORED), count("Tx", SAME_UTC), count("Tx", west) };
      database.commit();

      assertThat(pending).containsExactly(1L, 1L, 1L);
      assertThat(new long[] { count("Tx", STORED), count("Tx", SAME_UTC), count("Tx", west) }).containsExactly(1L, 1L, 1L);
      if ("LSM_TREE".equals(algorithm))
        assertThat(sorted("Tx", "z >= ?", west)).isEqualTo(List.of(west, SAME_UTC, STORED));
    }

    @Test
    void offsetDateTimeComparesAsItsZonedForm() {
      final OffsetDateTime offset = STORED.toOffsetDateTime();
      assertThat(BinaryComparator.equals(offset, STORED)).isTrue();
      assertThat(BinaryComparator.equals(STORED, offset)).isTrue();
      assertThat(BinaryComparator.equals(offset, SAME_UTC)).isFalse();
    }
  }

  /**
   * Regression test for issue #5321: partial-prefix lookup on a composite NOTUNIQUE index returns rows
   * of OTHER keys when the first key component contains some accented (multi-byte UTF-8) characters.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5321 accented composite key prefix lookup")
  class AccentedPrefix extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE ReproN7");
        database.command("sql", "CREATE PROPERTY ReproN7.w STRING");
        database.command("sql", "CREATE PROPERTY ReproN7.l STRING");
        database.command("sql", "CREATE PROPERTY ReproN7.t STRING");
        database.command("sql", "CREATE INDEX ON ReproN7 (w, l, t) NOTUNIQUE");

        final String[] words = { "cioccolato", "cinema", "cioè", "ciondolo", "citare", "così", "è", "perché", "casa", "cane" };
        for (final String w : words) {
          database.command("sql", "INSERT INTO ReproN7 SET w = ?, l = 'it', t = 'A'", w);
          database.command("sql", "INSERT INTO ReproN7 SET w = ?, l = 'it', t = 'B'", w);
        }
      });
    }

    private long countPartial(final String w) {
      return database.query("sql", "SELECT w FROM ReproN7 WHERE w = ? AND l = 'it'", w).stream().count();
    }

    @Test
    void fullKeyEqualityAlwaysCorrect() {
      database.transaction(() -> {
        assertThat(database.query("sql", "SELECT w FROM ReproN7 WHERE w = 'cioè' AND l = 'it' AND t = 'A'").stream().count())
            .isEqualTo(1);
        assertThat(database.query("sql", "SELECT w FROM ReproN7 WHERE w = 'è' AND l = 'it' AND t = 'A'").stream().count())
            .isEqualTo(1);
      });
    }

    @Test
    void partialPrefixEqualityAsciiCorrect() {
      database.transaction(() -> {
        assertThat(countPartial("casa")).isEqualTo(2);
        assertThat(countPartial("cinema")).isEqualTo(2);
      });
    }

    @Test
    void partialPrefixEqualityAccentedCorrect() {
      database.transaction(() -> {
        assertThat(countPartial("così")).as("così").isEqualTo(2);
        assertThat(countPartial("perché")).as("perché").isEqualTo(2);
        assertThat(countPartial("cioè")).as("cioè").isEqualTo(2);
        assertThat(countPartial("è")).as("è").isEqualTo(2);
      });
    }
  }

  /**
   * #10269: on a {@code NOTUNIQUE_HASH} index a record whose key changed away and back in one transaction ({@code 1 -> 4 -> 1})
   * was indexed twice under the key it came back to, because the replay put the pair again on top of the committed one and a
   * hash bucket appends a RID without asking whether it is already there. A later delete or key change removed one of the two
   * entries and left the record listed under a key it no longer held.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#10269 key changed away and back in one transaction")
  class HashIndexKeyRoundTrip extends TestHelper {

    private String create(final String type, final String kind) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER").close();
      database.command("sql", "CREATE INDEX ON " + type + " (k) " + kind).close();
      return type;
    }

    private RID insert(final String type, final int k) {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(type).set("k", k).save().getIdentity());
      return rid[0];
    }

    private void roundTrip(final RID rid) {
      database.transaction(() -> {
        rid.asDocument().modify().set("k", 4).save();
        rid.asDocument().modify().set("k", 1).save();
      });
    }

    private Index index(final String type) {
      return database.getSchema().getType(type).getPolymorphicIndexByProperties("k");
    }

    private List<RID> indexed(final String type, final int k) {
      final List<RID> rids = new ArrayList<>();
      final IndexCursor cursor = index(type).get(new Object[] { k });
      while (cursor.hasNext())
        rids.add(cursor.next().getIdentity());
      return rids;
    }

    private long count(final String type, final int k) {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type + " WHERE k = ?", k)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
    void keyChangedAwayAndBackIsIndexedOnce(final String kind) {
      final String t = create("A", kind);
      final RID x = insert(t, 1);

      roundTrip(x);

      assertThat(indexed(t, 1)).containsExactly(x);
      assertThat(indexed(t, 4)).isEmpty();
      assertThat(index(t).countEntries()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
    void deleteAfterTheRoundTripLeavesNoEntry(final String kind) {
      final String t = create("B", kind);
      final RID x = insert(t, 1);
      roundTrip(x);

      database.transaction(() -> x.asDocument().delete());

      assertThat(indexed(t, 1)).isEmpty();
      assertThat(count(t, 1)).isZero();
      assertThat(index(t).countEntries()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
    void keyChangeAfterTheRoundTripLeavesNoEntryUnderTheOldKey(final String kind) {
      final String t = create("C", kind);
      final RID x = insert(t, 1);
      roundTrip(x);

      database.transaction(() -> x.asDocument().modify().set("k", 2).save());

      assertThat(indexed(t, 1)).isEmpty();
      assertThat(indexed(t, 2)).containsExactly(x);
      assertThat(count(t, 1)).isZero();
      assertThat(count(t, 2)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
    void roundTripInsideTheTransactionThatDeletesTheRecord(final String kind) {
      final String t = create("D", kind);
      final RID x = insert(t, 1);

      database.transaction(() -> {
        x.asDocument().modify().set("k", 4).save();
        x.asDocument().modify().set("k", 1).save();
        x.asDocument().delete();
      });

      assertThat(indexed(t, 1)).isEmpty();
      assertThat(indexed(t, 4)).isEmpty();
      assertThat(index(t).countEntries()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
    void aSiblingUnderTheSameKeyIsLeftAlone(final String kind) {
      final String t = create("E", kind);
      final RID x = insert(t, 1);
      final RID y = insert(t, 1);

      roundTrip(x);
      database.transaction(() -> x.asDocument().delete());

      assertThat(indexed(t, 1)).containsExactly(y);
      assertThat(count(t, 1)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
    void keyChangedAwayAndBackTwiceInOneTransactionIsIndexedOnce(final String kind) {
      final String t = create("F", kind);
      final RID x = insert(t, 1);

      database.transaction(() -> {
        for (int i = 0; i < 2; i++) {
          x.asDocument().modify().set("k", 4).save();
          x.asDocument().modify().set("k", 1).save();
        }
      });
      assertThat(indexed(t, 1)).containsExactly(x);

      database.transaction(() -> x.asDocument().delete());

      assertThat(indexed(t, 1)).isEmpty();
      assertThat(index(t).countEntries()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
    void aRecordCreatedAndRoundTrippedInOneTransactionIsIndexedOnce(final String kind) {
      final String t = create("G", kind);
      final RID[] x = new RID[1];

      database.transaction(() -> {
        x[0] = database.newDocument(t).set("k", 1).save().getIdentity();
        x[0].asDocument().modify().set("k", 4).save();
        x[0].asDocument().modify().set("k", 1).save();
      });
      assertThat(indexed(t, 1)).containsExactly(x[0]);
      assertThat(indexed(t, 4)).isEmpty();

      database.transaction(() -> x[0].asDocument().delete());

      assertThat(indexed(t, 1)).isEmpty();
      assertThat(index(t).countEntries()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
    void theRoundTripIsConsistentInsideTheTransactionToo(final String kind) {
      final String t = create("H", kind);
      final RID x = insert(t, 1);

      database.transaction(() -> {
        x.asDocument().modify().set("k", 4).save();
        x.asDocument().modify().set("k", 1).save();

        assertThat(indexed(t, 1)).containsExactly(x);
        assertThat(indexed(t, 4)).isEmpty();
        assertThat(count(t, 1)).isEqualTo(1);
      });
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
    void theRoundTripAndTheDeleteSurviveAReopen(final String kind) {
      final String t = create("I", kind);
      final RID x = insert(t, 1);
      roundTrip(x);

      reopenDatabase();

      final RID[] reloaded = new RID[1];
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + t + " WHERE k = 1")) {
        reloaded[0] = rs.next().getIdentity().orElseThrow();
        assertThat(rs.hasNext()).isFalse();
      }
      assertThat(indexed(t, 1)).containsExactly(reloaded[0]);

      database.transaction(() -> reloaded[0].asDocument().delete());

      assertThat(indexed(t, 1)).isEmpty();
      assertThat(index(t).countEntries()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
    void compositeKeyRoundTrip(final String kind) {
      database.command("sql", "CREATE DOCUMENT TYPE Comp").close();
      database.command("sql", "CREATE PROPERTY Comp.a INTEGER").close();
      database.command("sql", "CREATE PROPERTY Comp.b INTEGER").close();
      database.command("sql", "CREATE INDEX ON Comp (a, b) " + kind).close();
      final RID[] x = new RID[1];
      database.transaction(() -> x[0] = database.newDocument("Comp").set("a", 1).set("b", 7).save().getIdentity());

      database.transaction(() -> {
        x[0].asDocument().modify().set("a", 4).save();
        x[0].asDocument().modify().set("a", 1).save();
      });
      database.transaction(() -> x[0].asDocument().modify().set("a", 2).save());

      final IndexCursor cursor = database.getSchema().getType("Comp").getPolymorphicIndexByProperties("a", "b").get(new Object[] { 1, 7 });
      assertThat(cursor.hasNext()).isFalse();
      try (final ResultSet rs = database.query("sql", "SELECT a FROM Comp WHERE a = 1 AND b = 7")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  /**
   * Issue #7109: the tuple delta introduced for composite indexes with one collection modifier (issue #6934) compared tuples
   * with the shallow {@code Arrays.equals}, so a tuple element that is itself an array (a {@code BINARY} key here, the
   * {@code float[]} of a vector elsewhere) never matched its own deserialized copy. Every update of such a record - even one
   * touching no indexed property - removed and re-put every tuple, which is the very churn issue #5318 had already removed from
   * the scalar-only path of the same method.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#7109 array key tuple delta churn")
  class ArrayKeyTupleDeltaChurn extends TestHelper {

    @Override
    public void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.hash BINARY");
        database.command("sql", "CREATE PROPERTY Doc.tags LIST");
        database.command("sql", "CREATE PROPERTY Doc.counter INTEGER");
        database.command("sql", "CREATE INDEX ON Doc (hash, tags BY ITEM) NOTUNIQUE");
      });
    }

    @Test
    void unchangedArrayKeyDoesNotChurnTheIndex() {
      final RID rid = insert(0);

      database.begin();
      try {
        // The record is re-read from the committed buffer, so its BINARY key is a fresh byte[] that only a content-aware
        // comparison can recognise as unchanged.
        final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
        doc.set("counter", 1);
        doc.save();

        assertThat(((DatabaseInternal) database).getTransaction().getIndexChanges().getTotalEntries())
            .as("an update leaving both index keys untouched must not enqueue any index remove/put")
            .isZero();
      } finally {
        database.commit();
      }

      assertThat(countWhere("tags CONTAINS 'x'")).isEqualTo(1);
      assertThat(countWhere("tags CONTAINS 'y'")).isEqualTo(1);
      assertThat(indexEntries()).isEqualTo(2);
    }

    @Test
    void changedListItemStillProducesTheMinimalDelta() {
      final RID rid = insert(0);

      database.begin();
      try {
        final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
        doc.set("tags", List.of("x", "z"));
        doc.save();

        // One remove (hash,y) and one put (hash,z): the retained (hash,x) tuple must be left alone.
        assertThat(((DatabaseInternal) database).getTransaction().getIndexChanges().getTotalEntries()).isEqualTo(2);
      } finally {
        database.commit();
      }

      assertThat(countWhere("tags CONTAINS 'x'")).isEqualTo(1);
      assertThat(countWhere("tags CONTAINS 'z'")).isEqualTo(1);
      assertThat(countWhere("tags CONTAINS 'y'")).isZero();
      assertThat(indexEntries()).isEqualTo(2);
    }

    private RID insert(final int counter) {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("hash", new byte[] { 1, 2, 3 });
        doc.set("tags", List.of("x", "y"));
        doc.set("counter", counter);
        rid[0] = doc.save().getIdentity();
      });
      return rid[0];
    }

    private long countWhere(final String condition) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Doc WHERE " + condition)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private long indexEntries() {
      long total = 0;
      for (final Index index : database.getSchema().getType("Doc").getAllIndexes(false))
        total += index.countEntries();
      return total;
    }
  }

  /**
   * Issue #6934: a composite index mixing a scalar property with exactly one {@code BY ITEM}/{@code BY KEY}/{@code BY VALUE}
   * property was never updated when only the scalar changed, because the single-expansion update path diffed the collection's
   * element membership instead of the complete key tuples. The record stayed visible under the stale key and invisible under the
   * real one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#6934 composite expanded key scalar update")
  class CompositeExpandedKeyScalarUpdate extends TestHelper {

    @Override
    public void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.status STRING");
        database.command("sql", "CREATE PROPERTY Doc.tags LIST");
        database.command("sql", "CREATE PROPERTY Doc.attrs MAP");
        database.command("sql", "CREATE PROPERTY Doc.items LIST");
      });
    }

    @Test
    void scalarChangeIsReflectedInListByItemCompositeIndex() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) NOTUNIQUE"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x','y']"));
      database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

      assertThat(countWhere("B", "tags CONTAINS 'x'")).isEqualTo(1);
      assertThat(countWhere("B", "tags CONTAINS 'y'")).isEqualTo(1);
      assertThat(countWhere("A", "tags CONTAINS 'x'")).isZero();
      assertThat(countWhere("A", "tags CONTAINS 'y'")).isZero();
      assertThat(indexEntries()).isEqualTo(2);
    }

    @Test
    void scalarAndListChangedTogetherKeepsNoStaleEntry() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) NOTUNIQUE"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x','y']"));
      // Partial list change together with the scalar: before the fix the retained element 'x' kept its (A,x) entry while the new
      // element 'z' was keyed with (B,z), so the index held a mix of old and new scalar values for the same record
      database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B', tags = ['x','z']"));

      assertThat(countWhere("B", "tags CONTAINS 'x'")).isEqualTo(1);
      assertThat(countWhere("B", "tags CONTAINS 'z'")).isEqualTo(1);
      assertThat(countWhere("A", "tags CONTAINS 'x'")).isZero();
      assertThat(countWhere("A", "tags CONTAINS 'y'")).isZero();
      assertThat(countWhere("B", "tags CONTAINS 'y'")).isZero();
      assertThat(indexEntries()).isEqualTo(2);
    }

    @Test
    void uniqueIndexReleasesTheStaleKey() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) UNIQUE"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x']"));
      database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

      // The key (A,x) is no longer held by any record: a new record can take it
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x']"));
      assertThat(countWhere("A", "tags CONTAINS 'x'")).isEqualTo(1);
      assertThat(countWhere("B", "tags CONTAINS 'x'")).isEqualTo(1);

      // And the key (B,x) is now really taken
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'B', tags = ['x']")))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    @Test
    void scalarChangeIsReflectedInMapByKeyCompositeIndex() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, attrs BY KEY) NOTUNIQUE"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', attrs = {'k1': 'v1', 'k2': 'v2'}"));
      database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

      assertThat(countWhere("B", "attrs CONTAINSKEY 'k1'")).isEqualTo(1);
      assertThat(countWhere("A", "attrs CONTAINSKEY 'k1'")).isZero();
      assertThat(indexEntries()).isEqualTo(2);
    }

    @Test
    void scalarChangeIsReflectedInMapByValueCompositeIndex() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, attrs BY VALUE) NOTUNIQUE"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', attrs = {'k1': 'v1', 'k2': 'v2'}"));
      database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

      assertThat(countWhere("B", "attrs CONTAINSVALUE 'v1'")).isEqualTo(1);
      assertThat(countWhere("A", "attrs CONTAINSVALUE 'v1'")).isZero();
      assertThat(indexEntries()).isEqualTo(2);
    }

    @Test
    void scalarChangeIsReflectedInNestedListByItemCompositeIndex() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, `items.id` BY ITEM) NOTUNIQUE"));
      database.transaction(() -> database.command("sql",
          "INSERT INTO Doc SET status = 'A', items = [{'id': 1}, {'id': 2}]"));
      database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

      assertThat(lookup("B", 1)).isEqualTo(1);
      assertThat(lookup("B", 2)).isEqualTo(1);
      assertThat(lookup("A", 1)).isZero();
      assertThat(lookup("A", 2)).isZero();
    }

    @Test
    void unchangedRecordTouchesNothing() {
      database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) NOTUNIQUE"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x','y']"));
      // An update that changes neither the scalar nor the list must leave the index as it is
      database.transaction(() -> database.command("sql", "UPDATE Doc SET other = 1"));

      assertThat(countWhere("A", "tags CONTAINS 'x'")).isEqualTo(1);
      assertThat(indexEntries()).isEqualTo(2);
    }

    /**
     * Counts the rows matching {@code status = '<status>' AND <predicate>}, checking every returned row really carries that
     * status: before the fix the index answered a record under a key it no longer held, returning a row with status 'B' for a
     * query asking for 'A'.
     */
    private int countWhere(final String status, final String predicate) {
      final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE status = '" + status + "' AND " + predicate);
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("status")).isEqualTo(status);
        ++count;
      }
      return count;
    }

    /** The only index defined on Doc by the test. */
    private TypeIndex docIndex() {
      final TypeIndex[] indexes = database.getSchema().getType("Doc").getAllIndexes(false).toArray(new TypeIndex[0]);
      assertThat(indexes).hasSize(1);
      return indexes[0];
    }

    private long indexEntries() {
      return docIndex().countEntries();
    }

    private int lookup(final Object... keys) {
      final IndexCursor cursor = docIndex().get(keys);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++count;
      }
      return count;
    }
  }
}
