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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10382: the sorted build of a UNIQUE index (chosen automatically from
 * {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS} records) wrapped the {@link DuplicatedKeyException}
 * it raises in a reason-less {@link IndexException}, so {@code getMessage()} of what the caller catches named neither the
 * duplicated key nor the record already holding it, whereas the record-at-a-time build of the same statement did (issue
 * #5607). The message is what the embedded API, the console and every log line report, so it is what these tests read:
 * the root cause carrying the key was already covered by {@link Issue10334UniqueIndexSortedBuildTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10382UniqueIndexRefusalMessageTest extends TestHelper {
  private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

  @BeforeEach
  void captureSortedBuilds() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
  }

  @AfterEach
  void releaseHook() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
  }

  /**
   * One duplicated key, the same statement, either side of the sorted-build threshold: the message names the same
   * things. {@code 0} keeps the record-at-a-time build, {@code 1000} routes the 5,000 records to the sorted one.
   */
  @ParameterizedTest(name = "sorted build from {0} records")
  @ValueSource(longs = { 0L, 1_000L })
  void duplicateKeyIsNamedInTheMessageOfTheRefusal(final long sortedBuildMinRecords) {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, sortedBuildMinRecords);
    createPopulatedType("Work", 3, 5_000);
    database.transaction(() -> database.newDocument("Work").set("id", workId(2_500)).save());

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close())
        .isInstanceOf(IndexException.class)
        .hasMessageContaining("Error on creating index on type 'Work', properties [id]")
        .hasMessageContaining("Duplicated key [" + workId(2_500) + "] found on index 'Work[id]' already assigned to record #")
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(sortedBuilds).isEmpty();
    assertThat(database.getSchema().getType("Work").getIndexByProperties("id")).isNull();
  }

  /** A composite key is reported whole, in the sorted build as in the record-at-a-time one. */
  @ParameterizedTest(name = "sorted build from {0} records")
  @ValueSource(longs = { 0L, 1_000L })
  void duplicateCompositeKeyIsNamedInTheMessageOfTheRefusal(final long sortedBuildMinRecords) {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, sortedBuildMinRecords);
    final DocumentType type = database.getSchema().buildDocumentType().withName("Pair").withTotalBuckets(2).create();
    type.createProperty("a", Type.INTEGER);
    type.createProperty("b", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++)
        database.newDocument("Pair").set("a", i % 10).set("b", i).save();
      database.newDocument("Pair").set("a", 7).set("b", 1_007).save();
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Pair (a, b) UNIQUE").close())
        .isInstanceOf(IndexException.class)
        .hasMessageContaining("Duplicated key [7, 1007] found on index 'Pair[a,b]' already assigned to record #");
  }

  private void createPopulatedType(final String typeName, final int buckets, final int records) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
    type.createProperty("id", Type.STRING);
    for (int from = 0; from < records; from += 10_000) {
      final int first = from;
      database.transaction(() -> {
        for (int i = first; i < Math.min(records, first + 10_000); i++)
          database.newDocument(typeName).set("id", workId(i)).save();
      });
    }
  }

  /** OpenAlex-style ids in a scrambled order, as in the report: the keys never arrive sorted. */
  private static String workId(final int i) {
    return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
  }
}
