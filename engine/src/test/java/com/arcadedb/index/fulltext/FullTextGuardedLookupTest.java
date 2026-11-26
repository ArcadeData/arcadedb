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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The lookups of a FULL_TEXT index that take a {@link WorkGuard} stop at the deadline of the statement they run for, and answer
 * what the lookups that take none answer when nothing bounds them.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextGuardedLookupTest extends TestHelper {
  /** Well past the 1024 postings between two checks of a guard. */
  private static final int DOCUMENTS = 3_000;

  @ParameterizedTest
  @CsvSource({ "BM25,1", "BM25,4", "CLASSIC,1", "CLASSIC,4" })
  void aLookupWithAGuardStopsAtTheDeadline(final String similarity, final int buckets) {
    createCorpus(similarity, buckets, 1);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final WorkGuard expired = WorkGuard.forCommandDeadline(expiredContext());
    final Object[] keys = { "common" };

    assertThat(catchThrowable(() -> index.get(keys, -1, expired))).isInstanceOf(TimeoutException.class);
    assertThat(catchThrowable(() -> index.get(keys, 10, expired))).isInstanceOf(TimeoutException.class);

    // The same lookups with no bound answer every document, as the ones that take no guard do
    try (final IndexCursor cursor = index.get(keys, -1, WorkGuard.forCommandDeadline(null))) {
      assertThat(count(cursor)).isEqualTo(DOCUMENTS);
    }
    try (final IndexCursor cursor = index.get(keys)) {
      assertThat(count(cursor)).isEqualTo(DOCUMENTS);
    }
    try (final IndexCursor cursor = index.get(keys, 10)) {
      assertThat(count(cursor)).isEqualTo(10);
    }
  }

  @ParameterizedTest
  @CsvSource({ "BM25", "CLASSIC" })
  void aMultiPropertyLookupWithAGuardStopsAtTheDeadline(final String similarity) {
    createCorpus(similarity, 4, 2);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[title,content]");
    final WorkGuard expired = WorkGuard.forCommandDeadline(expiredContext());
    final Object[] keys = { "common", "common" };

    assertThat(catchThrowable(() -> index.get(keys, -1, expired))).isInstanceOf(TimeoutException.class);
    try (final IndexCursor cursor = index.get(keys, -1, WorkGuard.forCommandDeadline(null))) {
      assertThat(count(cursor)).isEqualTo(DOCUMENTS);
    }
  }

  @Test
  void aBucketIndexLookupWithAGuardStopsAtTheDeadline() {
    createCorpus("CLASSIC", 1, 1);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final LSMTreeFullTextIndex bucketIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
    final Object[] keys = { "common" };

    assertThat(catchThrowable(() -> bucketIndex.get(keys, -1, WorkGuard.forCommandDeadline(expiredContext()))))
        .isInstanceOf(TimeoutException.class);
    try (final IndexCursor cursor = bucketIndex.get(keys, -1, WorkGuard.forCommandDeadline(null))) {
      assertThat(count(cursor)).isEqualTo(DOCUMENTS);
    }
  }

  @Test
  void aLookupOfAnotherIndexKindIgnoresTheGuard() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "CREATE PROPERTY Plain.name STRING");
      database.command("sql", "CREATE INDEX ON Plain (name) NOTUNIQUE");
      database.newDocument("Plain").set("name", "a").save();
    });
    final Index index = database.getSchema().getIndexByName("Plain[name]");

    try (final IndexCursor cursor = ((TypeIndex) index).get(new Object[] { "a" }, -1, WorkGuard.forCommandDeadline(expiredContext()))) {
      assertThat(count(cursor)).isEqualTo(1);
    }
  }

  private BasicCommandContext expiredContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");
    return context;
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    while (cursor.hasNext()) {
      cursor.next();
      ++count;
    }
    return count;
  }

  private void createCorpus(final String similarity, final int buckets, final int properties) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (" + (properties == 1 ? "content" : "title, content")
          + ") FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
      for (int i = 0; i < DOCUMENTS; i++)
        database.newDocument("Doc")
            .set("title", "common headline" + i)
            .set("content", "common word" + i + " " + "common ".repeat(i % 7))
            .save();
    });
  }
}
