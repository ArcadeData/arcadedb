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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract of an index, checked against an in-memory model: random transactions of inserts, key changes and deletes
 * (committed or rolled back) on a type with one LSM_TREE or HASH index, interleaved with compactions, reopens and rebuilds, and
 * after every step what the index answers must be exactly what the model holds: point lookups (with and without a limit),
 * entry counts, ordered and ranged iteration in both directions, the same questions asked through SQL, and what a transaction
 * reads of its own pending changes. A key that is a duplicate on a unique index must be refused, at the statement that brings
 * the second holder or at the commit, and the transaction with it. The index is created either before the records (it is
 * maintained by every write) or after them (the record-at-a-time and the sorted builds), and both must agree with the model.
 * <p>
 * {@link TestHelper} runs CHECK DATABASE at the end of every test, which compares the index entries with the records as well.
 */
class IndexModelContractTest extends TestHelper {
  enum KeyKind {
    INT, STRING, COMPOSITE, LONG, DOUBLE, BYTE, SHORT, FLOAT, DECIMAL, DATETIME, UNICODE
  }

  private static final String[] UNICODE_PARTS = { "a", "b", "Z", "", "\u00e9", "\uE000", "\uFFFD", "\uD83D\uDE00", "\u0000", "~", "\u4e2d" };

  private static final class Scenario {
    final Schema.INDEX_TYPE kind;
    final boolean                unique;
    final KeyKind                keyKind;
    final int                    pageSize;
    final int                    buckets;
    final int                    domain;
    final int                    txMax;
    final boolean                allowNull;
    final int                    buildMode;
    final Random                 random;

    Scenario(final Schema.INDEX_TYPE kind, final boolean unique, final KeyKind keyKind, final int pageSize, final int buckets, final int domain, final int txMax,
        final boolean allowNull, final int buildMode, final long seed) {
      this.kind = kind;
      this.unique = unique;
      this.keyKind = keyKind;
      this.pageSize = pageSize;
      this.buckets = buckets;
      this.domain = domain;
      this.txMax = txMax;
      this.allowNull = allowNull;
      this.buildMode = buildMode;
      this.random = new Random(seed);
    }

    boolean ordered() {
      return kind == Schema.INDEX_TYPE.LSM_TREE;
    }

    boolean composite() {
      return keyKind == KeyKind.COMPOSITE;
    }

    String[] properties() {
      return composite() ? new String[] { "a", "b" } : new String[] { "k" };
    }

    @Override
    public String toString() {
      return kind + (unique ? " unique " : " non-unique ") + keyKind + " page=" + pageSize + " buckets=" + buckets + " domain=" + domain + " nulls="
          + allowNull + " buildMode=" + buildMode;
    }
  }

  static Stream<Scenario> scenarios() {
    final List<Scenario> all = new ArrayList<>();
    long seed = 1;
    for (final KeyKind keyKind : KeyKind.values()) {
      for (final boolean unique : new boolean[] { true, false }) {
        final int domain = keyKind == KeyKind.COMPOSITE ? 12 : 30;
        all.add(new Scenario(Schema.INDEX_TYPE.LSM_TREE, unique, keyKind, 1024, 1, domain, 6, !unique && keyKind != KeyKind.BYTE, 0, seed++));
        all.add(new Scenario(Schema.INDEX_TYPE.LSM_TREE, unique, keyKind, 4096, 3, 6, 8, false, 1 + (int) (seed % 2), seed++));
      }
    }
    for (final KeyKind keyKind : new KeyKind[] { KeyKind.INT, KeyKind.STRING, KeyKind.COMPOSITE, KeyKind.LONG }) {
      for (final boolean unique : new boolean[] { true, false }) {
        all.add(new Scenario(Schema.INDEX_TYPE.HASH, unique, keyKind, 4096, 1, 25, 6, false, 0, seed++));
        all.add(new Scenario(Schema.INDEX_TYPE.HASH, unique, keyKind, 16384, 3, 4, 8, !unique, 1 + (int) (seed % 2), seed++));
      }
    }
    return all.stream();
  }

  // the model: record id -> key (null = no key at all)
  private Map<String, List<Object>> model;
  private int                       nextValue;
  private boolean                   inCommit;
  private List<Object>              currentKey;
  private String                    currentRid;

  @ParameterizedTest(name = "{0}")
  @MethodSource("scenarios")
  void theIndexAnswersWhatTheModelHolds(final Scenario s) {
    final String typeName = "T";
    model = new LinkedHashMap<>();
    nextValue = 0;

    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(s.buckets).create();
    if (s.composite()) {
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.STRING);
    } else
      type.createProperty("k", propertyType(s.keyKind));
    type.createProperty("v", Type.INTEGER);

    // compaction is explicit here: a background one would steal the scheduled status and turn the explicit calls into no-ops
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    if (s.buildMode != 0) {
      prefill(s, 20 + s.random.nextInt(s.buildMode == 2 ? 400 : 150));
      // 1 = record-at-a-time build, 2 = sorted build
      final long threshold = s.buildMode == 2 ? 1L : 0L;
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, threshold);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, threshold);
    }
    database.getSchema().buildTypeIndex(typeName, s.properties()).withType(s.kind).withUnique(s.unique).withPageSize(s.pageSize)
        .withNullStrategy(s.allowNull ? LSMTreeIndexAbstract.NULL_STRATEGY.INDEX : LSMTreeIndexAbstract.NULL_STRATEGY.SKIP).create();
    verify(s, "after the index was created");

    for (int step = 0; step < 30; step++) {
      final int what = s.random.nextInt(100);
      if (what < 82)
        transaction(s, step);
      else if (what < 90 && s.ordered()) {
        for (final Index index : database.getSchema().getIndexes())
          if (index instanceof TypeIndex typeIndex)
            try {
              ((IndexInternal) typeIndex).scheduleCompaction();
              ((IndexInternal) typeIndex).compact();
            } catch (final Exception e) {
              throw new AssertionError("compaction failed at step " + step, e);
            }
        verify(s, "after a compaction at step " + step);
      } else if (what < 96) {
        reopenDatabase();
        database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
        verify(s, "after a reopen at step " + step);
      } else {
        database.command("sql", "REBUILD INDEX *").close();
        verify(s, "after a rebuild at step " + step);
      }
    }
    verify(s, "at the end");
  }

  private void prefill(final Scenario s, final int total) {
    final Set<List<Object>> used = new HashSet<>();
    for (int done = 0; done < total; ) {
      database.begin();
      for (int i = 0; i < 50 && done < total; i++, done++) {
        List<Object> key = generateKey(s);
        if (s.unique && key != null) {
          int tries = 0;
          while (!used.add(key) && tries++ < 50)
            key = generateKey(s);
          if (tries >= 50)
            key = null;
        }
        final MutableDocument document = database.newDocument("T");
        setKey(s, document, key);
        document.set("v", nextValue++);
        document.save();
        model.put(document.getIdentity().toString(), key);
      }
      database.commit();
    }
  }

  private void transaction(final Scenario s, final int step) {
    final Map<String, List<Object>> committed = new LinkedHashMap<>(model);
    final Map<String, List<Object>> work = new LinkedHashMap<>(model);
    final boolean commit = s.random.nextInt(6) != 0;
    final int operations = 1 + s.random.nextInt(s.txMax);
    inCommit = false;
    currentKey = null;
    currentRid = null;
    database.begin();
    try {
      for (int i = 0; i < operations; i++) {
        final int op = s.random.nextInt(10);
        if (op < 5 || work.isEmpty()) {
          final List<Object> key = generateKey(s);
          final MutableDocument document = database.newDocument("T");
          setKey(s, document, key);
          document.set("v", nextValue++);
          currentKey = key;
          currentRid = null;
          document.save();
          work.put(document.getIdentity().toString(), key);
        } else if (op < 8) {
          final String rid = pick(work, s.random);
          final List<Object> key = generateKey(s);
          final MutableDocument document = database.lookupByRID(database.newRID(rid), true).asDocument().modify();
          setKey(s, document, key);
          currentKey = key;
          currentRid = rid;
          document.save();
          work.put(rid, key);
        } else if (op == 8) {
          final String rid = pick(work, s.random);
          database.lookupByRID(database.newRID(rid), true).asDocument().delete();
          work.remove(rid);
        } else {
          final String rid = pick(work, s.random);
          final MutableDocument document = database.lookupByRID(database.newRID(rid), true).asDocument().modify();
          document.set("v", nextValue++);
          document.save();
        }
        if (s.random.nextInt(8) == 0)
          verifyInTransaction(s, work);
      }
      inCommit = true;
      if (commit) {
        database.commit();
        assertThat(s.unique && hasDuplicate(work)).as("a duplicate on a unique index must be refused at commit").isFalse();
        model = work;
      } else {
        database.rollback();
        model = committed;
      }
    } catch (final DuplicatedKeyException e) {
      assertThat(s.unique).as("a duplicate key on a non-unique index: %s", e.getMessage()).isTrue();
      if (database.isTransactionActive())
        database.rollback();
      model = committed;
      boolean justified = false;
      if (inCommit)
        justified = hasDuplicate(work);
      else
        for (final Map.Entry<String, List<Object>> entry : work.entrySet())
          if (currentKey != null && currentKey.equals(entry.getValue()) && !entry.getKey().equals(currentRid))
            justified = true;
      assertThat(justified).as("step %d: the duplicate key %s was reported but no other record holds it", step, currentKey).isTrue();
    } catch (final RuntimeException e) {
      if (database.isTransactionActive())
        database.rollback();
      throw e;
    }
    verify(s, "after the transaction of step " + step + (commit ? " (commit)" : " (rollback)"));
  }

  // ------------------------------------------------------------------------------------------------------------------
  // keys
  // ------------------------------------------------------------------------------------------------------------------

  private static Type propertyType(final KeyKind kind) {
    return switch (kind) {
      case STRING, UNICODE -> Type.STRING;
      case LONG -> Type.LONG;
      case DOUBLE -> Type.DOUBLE;
      case BYTE -> Type.BYTE;
      case SHORT -> Type.SHORT;
      case FLOAT -> Type.FLOAT;
      case DECIMAL -> Type.DECIMAL;
      case DATETIME -> Type.DATETIME;
      default -> Type.INTEGER;
    };
  }

  private static List<Object> generateKey(final Scenario s) {
    if (s.allowNull && s.random.nextInt(10) == 0)
      return null;
    return generateNonNullKey(s);
  }

  private static List<Object> generateNonNullKey(final Scenario s) {
    if (s.composite()) {
      final int d = Math.max(2, (int) Math.sqrt(s.domain));
      return Arrays.asList(s.random.nextInt(d), String.format("s%03d", s.random.nextInt(d)));
    }
    final int v = s.random.nextInt(s.domain);
    final Object key = switch (s.keyKind) {
      case INT -> v;
      case STRING -> String.format("k%04d", v);
      case LONG -> (long) v * 1_000_000_007L - 5_000_000_000L;
      case BYTE -> (byte) (v % 250 - 120);
      case SHORT -> (short) (v * 331 % 60000 - 30000);
      case FLOAT -> v * 0.37f - 5f;
      case DECIMAL -> BigDecimal.valueOf(v * 7 - 50, 2);
      case DATETIME -> LocalDateTime.ofEpochSecond(1_600_000_000L + (long) (v - 20) * 86_400L, ((v * 7919) % 1000) * 1_000_000, ZoneOffset.UTC);
      case UNICODE -> {
        final StringBuilder builder = new StringBuilder();
        int x = v;
        for (int i = 0; i < 1 + v % 3; i++) {
          builder.append(UNICODE_PARTS[x % UNICODE_PARTS.length]);
          x = x * 7 + 3;
        }
        yield builder.toString();
      }
      default -> v * 0.5 - 3.25;
    };
    return Collections.singletonList(key);
  }

  private static void setKey(final Scenario s, final MutableDocument document, final List<Object> key) {
    if (s.composite()) {
      document.set("a", key == null ? null : key.get(0));
      document.set("b", key == null ? null : key.get(1));
    } else
      document.set("k", key == null ? null : key.get(0));
  }

  private static List<Object> readKey(final Scenario s, final Document document) {
    if (s.composite()) {
      final Object a = document.get("a");
      final Object b = document.get("b");
      return a == null && b == null ? null : Arrays.asList(a, b);
    }
    final Object k = document.get("k");
    return k == null ? null : Collections.singletonList(k);
  }

  /** Strings compare by code point (the UTF-8 byte order the index keeps and a scan uses), everything else by its natural order. */
  @SuppressWarnings("unchecked")
  private static int compareKeys(final List<Object> a, final List<Object> b) {
    for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
      final Object x = a.get(i);
      final Object y = b.get(i);
      final int c = x instanceof String sx && y instanceof String sy ?
          Arrays.compare(sx.codePoints().toArray(), sy.codePoints().toArray()) :
          ((Comparable<Object>) x).compareTo(y);
      if (c != 0)
        return c;
    }
    return Integer.compare(a.size(), b.size());
  }

  private static boolean hasDuplicate(final Map<String, List<Object>> records) {
    final Set<List<Object>> seen = new HashSet<>();
    for (final List<Object> key : records.values())
      if (key != null && !seen.add(key))
        return true;
    return false;
  }

  private static String pick(final Map<String, List<Object>> records, final Random random) {
    final int target = random.nextInt(records.size());
    int i = 0;
    for (final String rid : records.keySet())
      if (i++ == target)
        return rid;
    throw new IllegalStateException();
  }

  private static Map<List<Object>, Set<String>> invert(final Map<String, List<Object>> records) {
    final Map<List<Object>, Set<String>> inverted = new HashMap<>();
    for (final Map.Entry<String, List<Object>> entry : records.entrySet())
      if (entry.getValue() != null)
        inverted.computeIfAbsent(entry.getValue(), x -> new TreeSet<>()).add(entry.getKey());
    return inverted;
  }

  // ------------------------------------------------------------------------------------------------------------------
  // reads
  // ------------------------------------------------------------------------------------------------------------------

  private TypeIndex index() {
    return (TypeIndex) database.getSchema().getType("T").getAllIndexes(false).iterator().next();
  }

  private static Set<String> lookup(final TypeIndex index, final List<Object> key) {
    final Set<String> rids = new TreeSet<>();
    final IndexCursor cursor = index.get(key.toArray());
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity().toString());
    return rids;
  }

  private void assertLimitedLookup(final TypeIndex index, final List<Object> key, final Set<String> expected, final Random random, final String when) {
    final int limit = 1 + random.nextInt(expected.size() + 2);
    final Set<String> rids = new TreeSet<>();
    int count = 0;
    final IndexCursor cursor = index.get(key.toArray(), limit);
    while (cursor.hasNext()) {
      rids.add(cursor.next().getIdentity().toString());
      count++;
    }
    assertThat(count).as("%s: get(%s, limit=%d) returned %s of %s", when, key, limit, rids, expected).isEqualTo(Math.min(limit, expected.size()));
    assertThat(expected).as("%s: get(%s, limit=%d)", when, key, limit).containsAll(rids);
  }

  private List<String> sql(final String where, final Object... params) {
    final List<String> rids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM T" + (where.isEmpty() ? "" : " WHERE " + where), params)) {
      while (resultSet.hasNext())
        rids.add(resultSet.next().getIdentity().get().toString());
    }
    return rids;
  }

  /** What a transaction reads of its own pending changes (the index is asked while the transaction is open). */
  private void verifyInTransaction(final Scenario s, final Map<String, List<Object>> work) {
    if (s.unique && hasDuplicate(work))
      return;
    final TypeIndex index = index();
    final Map<List<Object>, Set<String>> expected = invert(work);
    for (int i = 0; i < 5; i++) {
      final List<Object> key = generateNonNullKey(s);
      final Set<String> exp = expected.getOrDefault(key, Collections.emptySet());
      assertThat(lookup(index, key)).as("in transaction: get(%s)", key).isEqualTo(exp);
      assertLimitedLookup(index, key, exp, s.random, "in transaction");
    }
    if (s.composite() || !s.ordered())
      return;
    for (int i = 0; i < 3; i++) {
      final List<Object> from = generateNonNullKey(s);
      final List<Object> to = generateNonNullKey(s);
      final boolean inclusive = s.random.nextBoolean();
      final boolean descending = s.random.nextBoolean();
      final List<Object> lo = compareKeys(from, to) <= 0 ? from : to;
      final List<Object> hi = compareKeys(from, to) <= 0 ? to : from;
      final Set<String> greater = new TreeSet<>();
      final Set<String> between = new TreeSet<>();
      for (final Map.Entry<String, List<Object>> entry : work.entrySet()) {
        if (entry.getValue() == null)
          continue;
        final int c = compareKeys(entry.getValue(), from);
        if (c > 0 || (inclusive && c == 0))
          greater.add(entry.getKey());
        if (compareKeys(entry.getValue(), lo) >= 0 && compareKeys(entry.getValue(), hi) <= 0)
          between.add(entry.getKey());
      }
      final List<String> got = sql("k " + (inclusive ? ">=" : ">") + " ? ORDER BY k " + (descending ? "DESC" : "ASC"), from.get(0));
      assertThat(got).as("in transaction: k %s %s", inclusive ? ">=" : ">", from).hasSameSizeAs(greater).containsExactlyInAnyOrderElementsOf(greater);
      for (int q = 1; q < got.size(); q++) {
        final int c = compareKeys(work.get(got.get(q - 1)), work.get(got.get(q)));
        assertThat(descending ? c >= 0 : c <= 0).as("in transaction: ORDER BY k %s out of order: %s", descending ? "DESC" : "ASC", got).isTrue();
      }
      assertThat(sql("k BETWEEN ? AND ?", lo.get(0), hi.get(0))).as("in transaction: k BETWEEN %s AND %s", lo, hi).hasSameSizeAs(between)
          .containsExactlyInAnyOrderElementsOf(between);
    }
  }

  private void verify(final Scenario s, final String when) {
    final TypeIndex index = index();
    final Map<List<Object>, Set<String>> expected = invert(model);

    final Map<String, List<Object>> table = new HashMap<>();
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM T")) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        table.put(result.getIdentity().get().toString(), readKey(s, result.toElement()));
      }
    }
    assertThat(table).as("%s: the records", when).isEqualTo(model);

    final Set<List<Object>> keys = new HashSet<>(expected.keySet());
    for (int i = 0; i < Math.min(40, s.domain + 5); i++)
      keys.add(generateNonNullKey(s));
    for (final List<Object> key : keys) {
      final Set<String> exp = expected.getOrDefault(key, Collections.emptySet());
      assertThat(lookup(index, key)).as("%s: get(%s)", when, key).isEqualTo(exp);
      assertLimitedLookup(index, key, exp, s.random, when);
      final List<String> viaSql = s.composite() ? sql("a = ? AND b = ?", key.get(0), key.get(1)) : sql("k = ?", key.get(0));
      assertThat(viaSql).as("%s: SQL lookup of %s", when, key).hasSameSizeAs(exp).containsExactlyInAnyOrderElementsOf(exp);
    }

    long expectedEntries = 0;
    for (final List<Object> key : model.values())
      if (key != null || s.allowNull)
        expectedEntries++;
    assertThat(index.countEntries()).as("%s: countEntries", when).isEqualTo(expectedEntries);

    if (!s.ordered())
      return;

    final List<Map.Entry<List<Object>, String>> sorted = new ArrayList<>();
    for (final Map.Entry<String, List<Object>> entry : model.entrySet())
      if (entry.getValue() != null)
        sorted.add(Map.entry(entry.getValue(), entry.getKey()));
    sorted.sort((x, y) -> compareKeys(x.getKey(), y.getKey()));

    for (final boolean ascending : new boolean[] { true, false }) {
      final List<List<Object>> seenKeys = new ArrayList<>();
      final Map<List<Object>, Set<String>> seen = new HashMap<>();
      final IndexCursor cursor = index.iterator(ascending);
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        final List<Object> key = Arrays.asList(cursor.getKeys());
        if (s.allowNull && key.stream().allMatch(Objects::isNull))
          continue;
        seenKeys.add(key);
        seen.computeIfAbsent(key, x -> new TreeSet<>()).add(rid.toString());
      }
      assertThat(seen).as("%s: iterator(%s) content", when, ascending).isEqualTo(expected);
      assertThat(seenKeys).as("%s: iterator(%s) entries", when, ascending).hasSize(sorted.size());
      for (int i = 1; i < seenKeys.size(); i++) {
        final int c = compareKeys(seenKeys.get(i - 1), seenKeys.get(i));
        assertThat(ascending ? c <= 0 : c >= 0).as("%s: iterator(%s) out of order at %d: %s then %s", when, ascending, i, seenKeys.get(i - 1),
            seenKeys.get(i)).isTrue();
      }
    }

    for (int i = 0; i < 6; i++) {
      final List<Object> from = generateNonNullKey(s);
      final List<Object> to = generateNonNullKey(s);
      final boolean includeFrom = s.random.nextBoolean();
      final boolean includeTo = s.random.nextBoolean();
      final boolean ascending = s.random.nextBoolean();
      final boolean fromIsLow = compareKeys(from, to) <= 0;
      final List<Object> lo = fromIsLow ? from : to;
      final List<Object> hi = fromIsLow ? to : from;
      final boolean includeLo = fromIsLow ? includeFrom : includeTo;
      final boolean includeHi = fromIsLow ? includeTo : includeFrom;

      final Map<List<Object>, Set<String>> inRange = new HashMap<>();
      for (final Map.Entry<List<Object>, String> entry : sorted) {
        final int a = compareKeys(entry.getKey(), lo);
        final int b = compareKeys(entry.getKey(), hi);
        if ((a > 0 || (includeLo && a == 0)) && (b < 0 || (includeHi && b == 0)))
          inRange.computeIfAbsent(entry.getKey(), x -> new TreeSet<>()).add(entry.getValue());
      }
      // the begin key of a descending range is its high end
      final IndexCursor range = ascending ?
          index.range(true, lo.toArray(), includeLo, hi.toArray(), includeHi) :
          index.range(false, hi.toArray(), includeHi, lo.toArray(), includeLo);
      final Map<List<Object>, Set<String>> rangeSeen = new HashMap<>();
      List<Object> previous = null;
      while (range.hasNext()) {
        final RID rid = range.next().getIdentity();
        final List<Object> key = Arrays.asList(range.getKeys());
        rangeSeen.computeIfAbsent(key, x -> new TreeSet<>()).add(rid.toString());
        if (previous != null) {
          final int c = compareKeys(previous, key);
          assertThat(ascending ? c <= 0 : c >= 0).as("%s: range out of order: %s then %s", when, previous, key).isTrue();
        }
        previous = key;
      }
      assertThat(rangeSeen).as("%s: range(%s, %s%s, %s%s)", when, ascending ? "asc" : "desc", includeLo ? "[" : "(", lo, hi, includeHi ? "]" : ")")
          .isEqualTo(inRange);

      // from a key to the end of the index, in the direction asked
      final Map<List<Object>, Set<String>> toTheEnd = new HashMap<>();
      for (final Map.Entry<List<Object>, String> entry : sorted) {
        final int a = compareKeys(entry.getKey(), from);
        if (ascending ? (a > 0 || (includeFrom && a == 0)) : (a < 0 || (includeFrom && a == 0)))
          toTheEnd.computeIfAbsent(entry.getKey(), x -> new TreeSet<>()).add(entry.getValue());
      }
      final IndexCursor tail = index.iterator(ascending, from.toArray(), includeFrom);
      final Map<List<Object>, Set<String>> tailSeen = new HashMap<>();
      while (tail.hasNext()) {
        final RID rid = tail.next().getIdentity();
        final List<Object> key = Arrays.asList(tail.getKeys());
        if (s.allowNull && key.stream().allMatch(Objects::isNull))
          continue;
        tailSeen.computeIfAbsent(key, x -> new TreeSet<>()).add(rid.toString());
      }
      assertThat(tailSeen).as("%s: iterator(%s, from %s, inclusive=%s)", when, ascending, from, includeFrom).isEqualTo(toTheEnd);

      if (!s.composite()) {
        final Set<String> greater = new TreeSet<>();
        final Set<String> between = new TreeSet<>();
        for (final Map.Entry<List<Object>, String> entry : sorted) {
          final int a = compareKeys(entry.getKey(), from);
          if (a > 0 || (includeFrom && a == 0))
            greater.add(entry.getValue());
          if (compareKeys(entry.getKey(), lo) >= 0 && compareKeys(entry.getKey(), hi) <= 0)
            between.add(entry.getValue());
        }
        assertThat(sql("k " + (includeFrom ? ">=" : ">") + " ?", from.get(0))).as("%s: SQL k %s %s", when, includeFrom ? ">=" : ">", from)
            .hasSameSizeAs(greater).containsExactlyInAnyOrderElementsOf(greater);
        assertThat(sql("k BETWEEN ? AND ?", lo.get(0), hi.get(0))).as("%s: SQL k BETWEEN %s AND %s", when, lo, hi).hasSameSizeAs(between)
            .containsExactlyInAnyOrderElementsOf(between);
      }
    }
  }
}
