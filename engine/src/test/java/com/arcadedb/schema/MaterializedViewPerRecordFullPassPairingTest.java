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
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11551: the full pass of a per-record materialized view pairs each query row with its view row through one scan of the
 * backing type, sorted by source ({@link SourceLinks}), instead of one {@code _sourceRID} index lookup per row plus a second
 * scan for the orphans. These pin that the pairing writes exactly what the lookups did: updates in place, inserts, orphan
 * deletes, rows with no link, and two pairs of one source.
 */
class MaterializedViewPerRecordFullPassPairingTest extends TestHelper {
  private static final String QUERY = "SELECT name, age FROM Person WHERE age >= 10";

  @Test
  void sortedPairsAreFoundByTheirSource() {
    final Random random = new Random(11551);
    final Map<RID, RID> rowBySource = new HashMap<>();
    final SourceLinks links = new SourceLinks(4);
    // enough pairs to go through the quicksort, not only the insertion sort, and buckets that interleave
    while (rowBySource.size() < 5000) {
      final RID source = new RID(random.nextInt(12), random.nextInt(100_000));
      if (rowBySource.containsKey(source))
        continue;
      final RID row = new RID(100 + random.nextInt(4), rowBySource.size());
      rowBySource.put(source, row);
      links.add(source, row);
    }
    links.sort();

    assertThat(links.size()).isEqualTo(5000);
    for (final Map.Entry<RID, RID> entry : rowBySource.entrySet()) {
      final int index = links.match(entry.getKey());
      assertThat(index).isNotNegative();
      assertThat(links.row(index)).isEqualTo(entry.getValue());
      assertThat(links.isMatched(index)).isTrue();
    }
    for (int i = 0; i < links.size(); i++)
      assertThat(links.isMatched(i)).as("every pair was claimed").isTrue();

    assertThat(links.match(new RID(13, 0))).as("a source past every bucket").isEqualTo(-1);
    assertThat(links.match(new RID(-1, 0))).as("a source before every bucket").isEqualTo(-1);
    int absent = 0;
    for (int position = 0; position < 100_000 && absent < 100; position++) {
      final RID source = new RID(5, position);
      if (!rowBySource.containsKey(source)) {
        assertThat(links.match(source)).isEqualTo(-1);
        ++absent;
      }
    }
    assertThat(absent).isEqualTo(100);
  }

  /**
   * Two view rows linking one source (which the UNIQUE index forbids, so only a view damaged some other way holds them): each
   * query row of the source claims a pair not claimed yet, and a further one writes over a claimed one, as the index lookup did.
   * The pair left unclaimed is an orphan.
   */
  @Test
  void twoPairsOfOneSourceAreClaimedOneAtATime() {
    final SourceLinks links = new SourceLinks(1);
    final RID source = new RID(3, 7);
    links.add(new RID(3, 9), new RID(20, 0));
    links.add(source, new RID(20, 2));
    links.add(new RID(1, 1), new RID(20, 3));
    links.add(source, new RID(20, 1));
    links.sort();

    final int first = links.match(source);
    assertThat(links.row(first)).as("ties are ordered by row").isEqualTo(new RID(20, 1));
    final int second = links.match(source);
    assertThat(links.row(second)).isEqualTo(new RID(20, 2));
    assertThat(links.match(source)).as("a third row of the source writes over a row it already has").isEqualTo(first);

    assertThat(links.isMatched(links.match(new RID(3, 9)))).isTrue();
    int unclaimed = 0;
    for (int i = 0; i < links.size(); i++)
      if (!links.isMatched(i)) {
        assertThat(links.row(i)).isEqualTo(new RID(20, 3));
        ++unclaimed;
      }
    assertThat(unclaimed).as("the pair of (1:1) is the orphan").isEqualTo(1);
  }

  @Test
  void emptyLinksMatchNothing() {
    final SourceLinks links = new SourceLinks(0);
    links.sort();
    assertThat(links.size()).isZero();
    assertThat(links.match(new RID(1, 1))).isEqualTo(-1);
  }

  /**
   * Updates in place, inserts, deletes, a record moving out of the WHERE and one moving back in, over a source spread on
   * several buckets and a subtype, while the view is not maintained on commit: one full pass must bring the view equal to the
   * query, keep every surviving source on the row it had, and leave the {@code _sourceRID} index naming exactly those rows.
   */
  @Test
  void fullPassAppliesUpdatesInsertsAndOrphanDeletes() {
    createSource();
    insert("Person", 0, 300);
    insert("Employee", 300, 100);
    createPerRecordView();
    database.command("sql", "ALTER MATERIALIZED VIEW PV REFRESH MANUAL");
    final Map<RID, RID> before = rowBySource();
    assertThat(before).hasSize(360); // ages 0..9 of every hundred are out of the WHERE

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Person WHERE name IN ['p15', 'p150', 'p350']");
      database.command("sql", "UPDATE Person SET age = age + 1000 WHERE name IN ['p20', 'p320']");
      database.command("sql", "UPDATE Person SET age = 1 WHERE name IN ['p30', 'p330']");
      database.command("sql", "UPDATE Person SET age = 50 WHERE name IN ['p5', 'p305']");
    });
    insert("Person", 1000, 20);
    insert("Employee", 2000, 20);

    fullPass();

    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    final Map<RID, RID> after = rowBySource();
    int kept = 0;
    for (final Map.Entry<RID, RID> entry : after.entrySet())
      if (before.containsKey(entry.getKey())) {
        assertThat(entry.getValue()).as("the row of a surviving source stays on its record").isEqualTo(before.get(entry.getKey()));
        ++kept;
      }
    assertThat(kept).as("360 rows, 3 deleted and 2 out of the WHERE").isEqualTo(355);
    assertThat(after).hasSize(355 + 2 + 2 * 10); // back into the WHERE, and the new ones of age >= 10
    assertIndexNamesEveryRow(after);
  }

  /** A pass over an unchanged source writes every row over itself and deletes nothing. */
  @Test
  void fullPassOverAnUnchangedSourceKeepsEveryRow() {
    createSource();
    insert("Person", 0, 200);
    insert("Employee", 200, 50);
    createPerRecordView();
    final Map<RID, RID> before = rowBySource();

    fullPass();
    fullPass();

    assertThat(rowBySource()).isEqualTo(before);
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
  }

  /**
   * Every source gone: every pair is an orphan. Then every source new again, over an empty view: every row is an insert. The
   * type scan reads the buckets in id order (the subtype's, created last, included), so every source is past every one created
   * before and no row takes the index-lookup fallback; that branch is covered by
   * {@link #fullPassOverASourceReadInKeyOrderCreatesOneRowPerSource}.
   */
  @Test
  void fullPassDeletesEveryOrphanAndCreatesEveryNewRow() {
    createSource();
    insert("Person", 0, 100);
    insert("Employee", 100, 100);
    createPerRecordView();
    database.command("sql", "ALTER MATERIALIZED VIEW PV REFRESH MANUAL");

    database.transaction(() -> database.command("sql", "DELETE FROM Person"));
    fullPass();
    assertThat(database.countType("PV", false)).isZero();

    insert("Person", 0, 100);
    insert("Employee", 100, 100);
    fullPass();
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    assertIndexNamesEveryRow(rowBySource());
  }

  /**
   * With an index on the WHERE column the planner reads the source in key order, not in RID order, so a source can come after a
   * greater one this pass created a row for: there the pass asks the {@code _sourceRID} index whether this pass already created
   * its row. The view still ends with one row per source, on the first pass over an empty view and on a pass that adds rows
   * among the ones it has.
   */
  @Test
  void fullPassOverASourceReadInKeyOrderCreatesOneRowPerSource() {
    createSource();
    database.transaction(() -> database.getSchema().getType("Person").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "age"));
    insert("Person", 0, 300);
    insert("Employee", 300, 100);
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + QUERY)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).as("the source is read through the index").contains("INDEX");
    }

    createPerRecordView();
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    assertIndexNamesEveryRow(rowBySource());

    database.command("sql", "ALTER MATERIALIZED VIEW PV REFRESH MANUAL");
    insert("Person", 1000, 200);
    insert("Employee", 2000, 100);
    fullPass();
    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    assertIndexNamesEveryRow(rowBySource());
  }

  /**
   * A row with no link (left by a view that was not per-record) is an orphan: the pass deletes it, and the row its source gets
   * is a new one, linked.
   */
  @Test
  void fullPassReplacesRowsWithNoLink() {
    createSource();
    insert("Person", 0, 50);
    createPerRecordView();
    database.command("sql", "ALTER MATERIALIZED VIEW PV REFRESH MANUAL");
    final Set<RID> unlinked = new HashSet<>();
    // only the refresh writes a backing type: borrow its scope
    try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view())) {
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          unlinked.add(database.newDocument("PV").set("name", "stray" + i).set("age", 99).save().getIdentity());
      });
    }
    assertThat(database.countType("PV", false)).isEqualTo(40 + 5);

    fullPass();

    assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    for (final RID rid : unlinked)
      assertThat(database.existsRecord(rid)).as("the row with no link is gone").isFalse();
    assertIndexNamesEveryRow(rowBySource());
  }

  private MaterializedViewImpl view() {
    return (MaterializedViewImpl) database.getSchema().getMaterializedView("PV");
  }

  private void fullPass() {
    final MaterializedViewImpl view = view();
    assertThat(view.isPerRecord()).as("the view keeps its link, so its pass is the per-record full pass").isTrue();
    MaterializedViewPerRecordRefresher.fullPass(database, view);
  }

  private void createSource() {
    database.transaction(() -> {
      final DocumentType person = database.getSchema().buildDocumentType().withName("Person").withTotalBuckets(4).create();
      person.createProperty("name", Type.STRING);
      person.createProperty("age", Type.INTEGER);
      database.getSchema().buildDocumentType().withName("Employee").withSuperType("Person").withTotalBuckets(2).create();
    });
  }

  private void insert(final String type, final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newDocument(type).set("name", "p" + i).set("age", i % 100).save();
    });
  }

  private void createPerRecordView() {
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("PV")
        .withQuery(QUERY)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());
    assertThat(view().isPerRecord()).isTrue();
  }

  private void assertIndexNamesEveryRow(final Map<RID, RID> rowBySource) {
    for (final Map.Entry<RID, RID> entry : rowBySource.entrySet()) {
      final IndexCursor cursor = database.lookupByKey("PV", MaterializedViewPerRecordRefresher.SOURCE_RID, entry.getKey());
      assertThat(cursor.hasNext()).isTrue();
      assertThat(cursor.next().getIdentity()).isEqualTo(entry.getValue());
      assertThat(cursor.hasNext()).as("one row per source").isFalse();
    }
  }

  private Map<RID, RID> rowBySource() {
    final Map<RID, RID> map = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM PV")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final RID source = row.getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID);
        assertThat(source).as("every row is linked").isNotNull();
        assertThat(map.put(source, row.getIdentity().get())).as("one row per source record").isNull();
      }
    }
    return map;
  }

  private List<String> viewRows() {
    return queryRows("SELECT name, age FROM PV");
  }

  private List<String> queryRows(final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("age"));
      }
    }
    return rows;
  }
}
