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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12701: a native select answered from an index declared on a supertype read the sub-indexes of the whole hierarchy, so
 * {@code fromType(subtype)} and {@code polymorphic(false)} returned the records of the other types. The rows must not depend on
 * whether an index exists.
 */
class SelectSubtypeIndexScopeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T").createProperty("a", Type.INTEGER);
      database.getSchema().getType("T").createProperty("id", Type.INTEGER);
      database.getSchema().createDocumentType("U").addSuperType("T");
      database.getSchema().createDocumentType("W").addSuperType("T");
      database.newDocument("T").set("a", 1, "id", 1).save();
      database.newDocument("U").set("a", 1, "id", 2).save();
      database.newDocument("T").set("a", 2, "id", 3).save();
      database.newDocument("U").set("a", 2, "id", 4).save();
      database.newDocument("W").set("a", 1, "id", 5).save();
    });
  }

  private List<String> rows(final String type, final boolean polymorphic, final String operator) {
    final SelectWhereOperatorBlock where = database.select().fromType(type).polymorphic(polymorphic).where().property("a");
    final SelectIterator<Document> iterator = switch (operator) {
      case "eq" -> where.eq().value(1).documents();
      case "gt" -> where.gt().value(0).documents();
      case "in" -> where.in().value(List.of(1, 2)).documents();
      default -> where.between().values(0, 5).documents();
    };
    final List<String> result = new ArrayList<>();
    while (iterator.hasNext()) {
      final Document document = iterator.next();
      result.add(document.getTypeName() + ":" + document.getInteger("id"));
    }
    Collections.sort(result);
    return result;
  }

  @Test
  void anIndexOnTheSupertypeDoesNotChangeTheRowsOfASelect() {
    for (final String operator : new String[] { "eq", "gt", "in", "between" })
      for (final String type : new String[] { "T", "U", "W" })
        for (final boolean polymorphic : new boolean[] { true, false }) {
          final List<String> scanned = rows(type, polymorphic, operator);
          database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
          final List<String> indexed = rows(type, polymorphic, operator);
          database.getSchema().dropIndex(database.getSchema().getType("T").getIndexByProperties("a").getName());
          assertThat(indexed).as(operator + " on " + type + " polymorphic " + polymorphic).isEqualTo(scanned);
        }
  }

  @Test
  void aSubtypeSelectOverTheInheritedIndexReturnsOnlyItsOwnRecords() {
    database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    assertThat(rows("U", true, "eq")).containsExactly("U:2");
    assertThat(rows("W", true, "gt")).containsExactly("W:5");
    assertThat(rows("T", false, "in")).containsExactly("T:1", "T:3");
    assertThat(database.select().fromType("U").where().property("a").eq().value(1).compile().count()).isEqualTo(1L);
    assertThat(database.select().fromType("W").where().property("a").eq().value(2).compile().exists()).isFalse();
  }

  @Test
  void theSupertypeSelectStillUsesItsIndex() {
    database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    final SelectIterator<Document> iterator = database.select().fromType("T").where().property("a").eq().value(1).documents();
    assertThat(iterator.toList()).hasSize(3);
    assertThat(iterator.getMetrics().get("usedIndexes")).isEqualTo(1);
  }

  // ---- a deeper hierarchy: P <- Q <- R, and P <- S. 40 rows per type, a = i % 4 (10 rows per type for each key), ids unique ----

  private record Row(String type, int a, int id) {
  }

  private static final String[] HIERARCHY_TYPES = { "P", "Q", "R", "S" };

  private List<Row> createHierarchy() {
    final List<Row> model = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().createDocumentType("P").createProperty("a", Type.INTEGER);
      database.getSchema().getType("P").createProperty("id", Type.INTEGER);
      database.getSchema().createDocumentType("Q").addSuperType("P");
      database.getSchema().createDocumentType("R").addSuperType("Q");
      database.getSchema().createDocumentType("S").addSuperType("P");
      int id = 0;
      // INTERLEAVED, SO NO TYPE'S ROWS ARE CONTIGUOUS
      for (int i = 0; i < 40; i++)
        for (final String type : HIERARCHY_TYPES) {
          final Row row = new Row(type, i % 4, id++);
          database.newDocument(type).set("a", row.a, "id", row.id).save();
          model.add(row);
        }
    });
    database.getSchema().getType("P").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    return model;
  }

  private static Set<String> scope(final String type, final boolean polymorphic) {
    return switch (type) {
      case "P" -> polymorphic ? Set.of("P", "Q", "R", "S") : Set.of("P");
      case "Q" -> polymorphic ? Set.of("Q", "R") : Set.of("Q");
      default -> Set.of(type);
    };
  }

  /** The ids of the rows with a = 1 the select must return, ascending. */
  private static List<Integer> expectedIds(final List<Row> model, final String type, final boolean polymorphic) {
    final Set<String> types = scope(type, polymorphic);
    return model.stream().filter(r -> r.a == 1 && types.contains(r.type)).map(Row::id).sorted().toList();
  }

  private static List<Integer> ids(final SelectIterator<Document> iterator) {
    final List<Integer> result = new ArrayList<>();
    while (iterator.hasNext())
      result.add(iterator.next().getInteger("id"));
    return result;
  }

  @Test
  void everyTypeOfADeeperHierarchyKeepsTheIndexAndReturnsItsOwnScope() {
    final List<Row> model = createHierarchy();
    for (final String type : HIERARCHY_TYPES)
      for (final boolean polymorphic : new boolean[] { true, false }) {
        final String label = type + " polymorphic " + polymorphic;
        final List<Integer> expected = expectedIds(model, type, polymorphic);

        final SelectIterator<Document> iterator = database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq()
            .value(1).documents();
        final List<Integer> actual = ids(iterator);
        Collections.sort(actual);
        assertThat(actual).as(label).isEqualTo(expected);
        // THE INDEX ANSWERED, AND THE RECORDS OF THE OTHER TYPES WERE DROPPED BEFORE THEY WERE LOADED AND EVALUATED
        assertThat(iterator.getMetrics().get("usedIndexes")).as(label).isEqualTo(1);
        assertThat(iterator.getMetrics().get("evaluatedRecords")).as(label).isEqualTo((long) expected.size());

        assertThat(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1).count()).as(label)
            .isEqualTo((long) expected.size());
        assertThat(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1).exists()).as(label)
            .isEqualTo(!expected.isEmpty());
        assertThat(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(9).exists()).as(label)
            .isFalse();
      }
  }

  @Test
  void skipLimitAndOrderByApplyAfterTheScope() {
    final List<Row> model = createHierarchy();
    for (final String type : HIERARCHY_TYPES)
      for (final boolean polymorphic : new boolean[] { true, false }) {
        final String label = type + " polymorphic " + polymorphic;
        final List<Integer> expected = expectedIds(model, type, polymorphic);
        final int to = Math.min(expected.size(), 3 + 4);

        for (final boolean ascending : new boolean[] { true, false }) {
          final List<Integer> ordered = new ArrayList<>(expected);
          ordered.sort(ascending ? Comparator.naturalOrder() : Comparator.reverseOrder());
          assertThat(ids(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1)
              .orderBy("id", ascending).skip(3).limit(4).documents())).as(label + " ascending " + ascending)
              .isEqualTo(ordered.subList(Math.min(3, ordered.size()), to));
        }

        // WITHOUT AN ORDER BY THE ROWS ARE THE FIRST ONES THE INDEX DELIVERS: ONLY THE COUNT AND THE SCOPE ARE DETERMINED
        final List<Integer> limited = ids(
            database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1).limit(2).documents());
        assertThat(limited).as(label + " limit").hasSize(Math.min(2, expected.size())).isSubsetOf(expected);
        final List<Integer> skipped = ids(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1)
            .skip(expected.size() - 1).documents());
        assertThat(skipped).as(label + " skip").hasSize(1).isSubsetOf(expected);

        assertThat(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1).skip(3).limit(4)
            .count()).as(label + " count").isEqualTo((long) Math.max(0, to - Math.min(3, expected.size())));
        assertThat(database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1)
            .skip(expected.size()).count()).as(label + " count past the end").isZero();
      }
  }

  @Test
  void aLimitCountsTheRecordsOfTheScopeNotTheCandidatesOfTheIndex() {
    // THE SUB-INDEXES ARE READ IN BUCKET ORDER AND THE ROWS OF P AND Q COME BEFORE THOSE OF S: A CAP APPLIED TO THE CANDIDATES BEFORE THE
    // SCOPE FILTER WOULD HAND S's SELECT NOTHING (equality, in-list and range leaves each build their cursors differently)
    final List<Row> model = createHierarchy();
    final List<Integer> expectedS = expectedIds(model, "S", true);
    assertThat(expectedS).hasSize(10);

    assertThat(ids(database.select().fromType("S").where().property("a").eq().value(1).limit(3).documents())).hasSize(3)
        .isSubsetOf(expectedS);
    assertThat(ids(database.select().fromType("S").where().property("a").in().value(List.of(1)).limit(3).documents())).hasSize(3)
        .isSubsetOf(expectedS);
    assertThat(ids(database.select().fromType("S").where().property("a").between().values(1, 1).limit(3).documents())).hasSize(3)
        .isSubsetOf(expectedS);
    assertThat(database.select().fromType("S").where().property("a").eq().value(1).limit(3).count()).isEqualTo(3L);
    assertThat(database.select().fromType("S").where().property("a").eq().value(1).skip(9).limit(3).count()).isEqualTo(1L);
  }

  @Test
  void inListAndRangeLeavesOverTheInheritedIndexStayInTheScope() {
    final List<Row> model = createHierarchy();
    final List<Integer> expectedQ = model.stream().filter(r -> r.a >= 1 && r.a <= 2 && scope("Q", false).contains(r.type))
        .map(Row::id).sorted().toList();

    for (final SelectIterator<Document> iterator : List.of(
        database.select().fromType("Q").polymorphic(false).where().property("a").in().value(List.of(1, 2)).documents(),
        database.select().fromType("Q").polymorphic(false).where().property("a").between().values(1, 2).documents())) {
      final List<Integer> actual = ids(iterator);
      Collections.sort(actual);
      assertThat(actual).isEqualTo(expectedQ);
      assertThat(iterator.getMetrics().get("usedIndexes")).isEqualTo(1);
    }
  }

  @Test
  void aParallelSelectOverTheInheritedIndexStaysInTheScope() {
    final List<Row> model = createHierarchy();
    for (final String type : HIERARCHY_TYPES)
      for (final boolean polymorphic : new boolean[] { true, false }) {
        final SelectIterator<Document> iterator = eqSelectIterator(type, polymorphic);
        final List<Integer> actual = ids(iterator);
        Collections.sort(actual);
        assertThat(actual).as(type + " polymorphic " + polymorphic).isEqualTo(expectedIds(model, type, polymorphic));
        assertThat(iterator.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
  }

  private SelectIterator<Document> eqSelectIterator(final String type, final boolean polymorphic) {
    return database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1).compile().parallel()
        .documents();
  }

  // ---- the same hierarchy under a composite index P(a, b): a = i % 4, b = (i / 4) % 2, so 10 rows per type match a = 1 and b = 1 ----

  private record CompositeRow(String type, int a, int b, int id) {
  }

  private List<CompositeRow> createCompositeHierarchy() {
    final List<CompositeRow> model = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().createDocumentType("P").createProperty("a", Type.INTEGER);
      database.getSchema().getType("P").createProperty("b", Type.INTEGER);
      database.getSchema().getType("P").createProperty("id", Type.INTEGER);
      database.getSchema().createDocumentType("Q").addSuperType("P");
      database.getSchema().createDocumentType("R").addSuperType("Q");
      database.getSchema().createDocumentType("S").addSuperType("P");
      int id = 0;
      for (int i = 0; i < 40; i++)
        for (final String type : HIERARCHY_TYPES) {
          final CompositeRow row = new CompositeRow(type, i % 4, (i / 4) % 2, id++);
          database.newDocument(type).set("a", row.a, "b", row.b, "id", row.id).save();
          model.add(row);
        }
    });
    database.getSchema().getType("P").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a", "b");
    return model;
  }

  private static List<Integer> expectedCompositeIds(final List<CompositeRow> model, final String type, final boolean polymorphic,
      final boolean withB) {
    final Set<String> types = scope(type, polymorphic);
    return model.stream().filter(r -> r.a == 1 && (!withB || r.b == 1) && types.contains(r.type)).map(CompositeRow::id).sorted()
        .toList();
  }

  @Test
  void aCompositeIndexOnTheSupertypeKeepsTheIndexAndTheScopeOfEverySubtype() {
    final List<CompositeRow> model = createCompositeHierarchy();
    for (final boolean withB : new boolean[] { true, false })
      for (final String type : HIERARCHY_TYPES)
        for (final boolean polymorphic : new boolean[] { true, false }) {
          final String label = type + " polymorphic " + polymorphic + (withB ? " a and b" : " prefix a");
          final List<Integer> expected = expectedCompositeIds(model, type, polymorphic, withB);

          final SelectIterator<Document> iterator = compositeIterator(type, polymorphic, withB, -1, 0);
          final List<Integer> actual = ids(iterator);
          Collections.sort(actual);
          assertThat(actual).as(label).isEqualTo(expected);
          assertThat(iterator.getMetrics().get("usedIndexes")).as(label).isEqualTo(1);
          assertThat(iterator.getMetrics().get("evaluatedRecords")).as(label).isEqualTo((long) expected.size());

          assertThat(compositeCount(type, polymorphic, withB, -1, 0)).as(label + " count").isEqualTo((long) expected.size());
          assertThat(compositeCount(type, polymorphic, withB, 3, 0)).as(label + " count limit").isEqualTo(3L);
          assertThat(compositeCount(type, polymorphic, withB, -1, expected.size() - 2)).as(label + " count skip").isEqualTo(2L);
          assertThat(compositeCount(type, polymorphic, withB, 4, expected.size() - 2)).as(label + " count skip limit").isEqualTo(2L);
          assertThat(compositeCount(type, polymorphic, withB, -1, expected.size())).as(label + " count past the end").isZero();

          // THE SUB-INDEXES ARE READ IN BUCKET ORDER, SO THE ROWS OF THE SUPERTYPE AND OF THE OTHER BRANCH COME FIRST: A CAP COUNTING
          // CANDIDATES BEFORE THE SCOPE FILTER WOULD COME BACK SHORT
          assertThat(ids(compositeIterator(type, polymorphic, withB, 3, 0))).as(label + " limit").hasSize(3).isSubsetOf(expected);
          assertThat(ids(compositeIterator(type, polymorphic, withB, 4, expected.size() - 2))).as(label + " skip limit").hasSize(2)
              .isSubsetOf(expected);
          assertThat(ids(compositeIterator(type, polymorphic, withB, -1, expected.size() - 1))).as(label + " skip").hasSize(1)
              .isSubsetOf(expected);
        }
  }

  private SelectIterator<Document> compositeIterator(final String type, final boolean polymorphic, final boolean withB,
      final int limit, final int skip) {
    return compositeSelect(type, polymorphic, withB, limit, skip).documents();
  }

  private long compositeCount(final String type, final boolean polymorphic, final boolean withB, final int limit, final int skip) {
    return compositeSelect(type, polymorphic, withB, limit, skip).count();
  }

  private Select compositeSelect(final String type, final boolean polymorphic, final boolean withB, final int limit, final int skip) {
    final SelectWhereAfterBlock where = withB ?
        database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1).and().property("b").eq()
            .value(1) :
        database.select().fromType(type).polymorphic(polymorphic).where().property("a").eq().value(1);
    final Select select = where.skip(skip);
    return limit > -1 ? select.limit(limit) : select;
  }

  // ---- the deadline while out-of-scope entries are skipped, and the release of the source cursor ----

  /** Virtual milliseconds: never compared against a real clock, {@link ExpiredAfterArming} decides when they are exceeded. */
  private static final long BUDGET_MS = 1_000;

  /**
   * Reports "no time has passed" when the select arms its deadline, then a time past it on every later reading. The outcome is
   * a function of the call count alone, the same on a fast, a slow or a stalling machine.
   */
  private static final class ExpiredAfterArming implements LongSupplier {
    private int readings;

    @Override
    public long getAsLong() {
      return ++readings <= 1 ? 0 : BUDGET_MS + 1;
    }
  }

  /**
   * {@code W} has no record with {@code a = 2}, so every entry the inherited index answers for that key belongs to another type
   * and is discarded by the scope filter. The consumers check the deadline only when the filter hands them a record, which it
   * never does here: without a check inside the filter's skip loop, an expired deadline let it skip the whole sibling population
   * and answer "no match" instead of the timeout the select asked for.
   */
  @Test
  void everyConsumerHonoursTheDeadlineWhileOutOfScopeEntriesAreSkipped() {
    database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    for (final String consumer : new String[] { "exists", "count", "iterator" }) {
      final Select select = database.select().fromType("W").timeout(BUDGET_MS, TimeUnit.MILLISECONDS, true);
      select.where().property("a").eq().value(2);
      select.compile();
      final SelectExecutor executor = new SelectExecutor(select);
      executor.clock = new ExpiredAfterArming();
      assertThatThrownBy(() -> {
        switch (consumer) {
        case "exists" -> executor.executeExists();
        case "count" -> executor.executeCount();
        default -> {
          try (final SelectIterator<Document> iterator = executor.execute()) {
            iterator.hasNext();
          }
        }
        }
      }).as(consumer).isInstanceOf(TimeoutException.class);
    }
  }

  @Test
  void aSelectThatAskedNotToBeThrownAtStopsSkippingOnceTheDeadlineExpired() {
    database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    final Select select = database.select().fromType("W").timeout(BUDGET_MS, TimeUnit.MILLISECONDS, false);
    select.where().property("a").eq().value(2);
    select.compile();
    final SelectExecutor executor = new SelectExecutor(select);
    final ExpiredAfterArming clock = new ExpiredAfterArming();
    executor.clock = clock;

    assertThat(executor.executeExists()).isFalse();
    // ONE READING ARMS THE DEADLINE, THE NEXT ONE (THE FIRST DISCARDED ENTRY) FINDS IT EXPIRED AND ENDS THE SKIP LOOP: THE REST OF
    // THE SIBLING ENTRIES ARE NEVER WALKED
    assertThat(clock.readings).isEqualTo(2);
  }

  /** A source cursor that remembers whether it was released. */
  private static final class ClosingCursor extends TempIndexCursor {
    private boolean closed;

    private ClosingCursor(final List<IndexCursorEntry> entries) {
      super(entries);
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void closingTheScopedCursorEarlyReleasesItsSource() {
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (int i = 0; i < 4; i++)
      entries.add(new IndexCursorEntry(new Object[] { i }, new RID(i % 2 == 0 ? 7 : 8, i), 1));
    final ClosingCursor child = new ClosingCursor(entries);
    final List<IndexCursor> children = new ArrayList<>();
    children.add(child);

    final IntHashSet scope = new IntHashSet();
    scope.add(8);
    final Select select = database.select().fromType("T");
    select.compile();
    final SelectExecutor executor = new SelectExecutor(select);
    final BucketScopedCursor cursor = new BucketScopedCursor(new MultiIndexCursor(children, -1, true, false), scope, executor);

    // ONLY THE ENTRIES OF BUCKET 8 COME OUT, AND THE CONSUMER STOPS AFTER THE FIRST ONE, LIKE A limit(1) OR AN exists()
    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next().getIdentity()).isEqualTo(new RID(8, 1));
    assertThat(child.closed).isFalse();

    cursor.close();
    assertThat(child.closed).isTrue();
  }
}
