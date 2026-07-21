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
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Second-round regression for #5120: a strict-prefix lookup (5 of 9 components) on a wide composite NOTUNIQUE index
 * returns FEWER rows than the equivalent full scan once the index has been compacted, while the full-key lookup and
 * the same index with compaction disabled are correct.
 * <p>
 * Root cause is NOT the compaction atomicity bug fixed by #4946, but the signed/unsigned key-comparison desync fixed
 * by #5321 (commit 02d5800ea): {@code BinaryComparator.compareBytes(byte[], Binary)} - used by the LSM binary-search
 * seek - compared bytes as signed, while the compactor writes the compacted pages in the engine-wide UNSIGNED byte
 * order. For ASCII-only values the two orders agree (which is why a synthetic reproducer with plain values never
 * fails); as soon as a key component holds a multi-byte UTF-8 character (umlauts, accents: lead/continuation bytes
 * &gt;= 0x80, negative as a Java byte) the seek walks the compacted pages in the wrong order: it either stops short of
 * the matching range (rows missing from the lookup while the scan still sees them - the reported undercount) or lands
 * on an unrelated range and emits foreign rows. A rebuild repairs the in-RAM mutable pages, but the next compaction -
 * or a restart, which reads from the compacted pages - brings the divergence back, exactly the durability symptom
 * reported here.
 * <p>
 * Both methods pass on the fixed code and fail without it; on the signed comparator the compacted phase shows both
 * {@code index=0 scan=1} and wildly inflated counts on the very same dataset.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5120CompactedPrefixNonAsciiTest extends TestHelper {

  private static final String TYPE_NAME  = "Repro5120";
  private static final int    COLUMNS    = 9;
  private static final int    PREFIX_LEN = 5;
  private static final int    ROWS       = 20_000;

  // Low-cardinality values, as in the report. The non-ASCII ones are what makes the compacted seek diverge.
  // Each accented value has an ASCII sibling that shares its prefix and diverges at the very byte that carries the
  // multi-byte character, so the signed and the unsigned byte orders disagree on their relative position.
  private static final String[] VALUES = { "Müller", "Muster", "Straße", "Strasse", "Grün", "Gruppe", "Köln", "Kosten",
      "Weiß", "Weiss", "Zürich", "Zusatz" };

  @Override
  public void beforeTest() {
    // Compaction is driven explicitly so the before/after state is deterministic.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private LSMTreeIndex createSchema() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    final String[] properties = new String[COLUMNS];
    for (int c = 0; c < COLUMNS; c++) {
      properties[c] = "c" + c;
      type.createProperty(properties[c], String.class);
    }

    database.getSchema().buildTypeIndex(TYPE_NAME, properties).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
        .withPageSize(4096).create();

    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }

  private static String value(final int row, final int column) {
    return VALUES[(row / (1 + column * 3) + column) % VALUES.length];
  }

  private void populate() {
    database.transaction(() -> {
      for (int r = 0; r < ROWS; r++) {
        final var doc = database.newDocument(TYPE_NAME);
        for (int c = 0; c < COLUMNS; c++)
          doc.set("c" + c, value(r, c));
        doc.save();
      }
    });
  }

  private String prefixWhere() {
    final StringBuilder where = new StringBuilder();
    for (int c = 0; c < PREFIX_LEN; c++) {
      if (c > 0)
        where.append(" AND ");
      where.append("c").append(c).append(" = ?");
    }
    return where.toString();
  }

  private Object[] prefixParams(final int row) {
    final Object[] params = new Object[PREFIX_LEN];
    for (int c = 0; c < PREFIX_LEN; c++)
      params[c] = value(row, c);
    return params;
  }

  /** Prefix lookup through the index. */
  private long indexCount(final int row) {
    final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE " + prefixWhere(),
        prefixParams(row));
    return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
  }

  /** Same predicate evaluated by scanning every record, bypassing the index. */
  private long scanCount(final int row) {
    final Object[] params = prefixParams(row);
    long count = 0;
    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME);
    while (rs.hasNext()) {
      final var doc = rs.next().getRecord().get().asDocument();
      boolean matches = true;
      for (int c = 0; c < PREFIX_LEN && matches; c++)
        matches = params[c].equals(doc.getString("c" + c));
      if (matches)
        count++;
    }
    return count;
  }

  private void assertIndexAgreesWithScan(final String phase) {
    final List<String> mismatches = new ArrayList<>();
    for (int row = 0; row < 40; row++) {
      final long scan = scanCount(row);
      final long index = indexCount(row);
      if (index != scan)
        mismatches.add("row " + row + ": index=" + index + " scan=" + scan);
    }
    assertThat(mismatches).as(phase + ": every prefix lookup must return exactly what the scan returns").isEmpty();
  }

  /** Everything served from the mutable pages, the configuration the reporter used as "compaction disabled". */
  @Test
  void prefixLookupAgreesWithScanOnMutablePages() {
    createSchema();
    populate();

    assertIndexAgreesWithScan("mutable");
  }

  /** Same data read through the compacted sub-index, where the reporter sees the undercount. */
  @Test
  void prefixLookupAgreesWithScanAfterCompaction() throws Exception {
    final LSMTreeIndex index = createSchema();
    populate();

    if (index.scheduleCompaction())
      index.compact();
    assertThat(index.getMutableIndex().getSubIndex()).as("compaction produced a sub-index").isNotNull();

    assertIndexAgreesWithScan("compacted");
  }
}
