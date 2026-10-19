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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10334 over HTTP, the path of the report (Studio). A command request runs inside the auto-commit transaction of the
 * handler, so {@code CREATE INDEX} finds a transaction open - an empty one - and the sorted build refused to run under it:
 * the automatic choice of the sorted build for a UNIQUE index, and an explicit {@code buildMode: 'SORTED'} alike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10334UniqueIndexSortedBuildOverHttpIT extends BaseGraphServerTest {
  private static final int RECORDS = 15_000;

  @Test
  void uniqueIndexOverHttpTakesTheSortedBuild() throws Exception {
    populate("Work");

    command(0, "CREATE INDEX ON Work (id) UNIQUE");

    assertBuiltSorted("Work");
  }

  @Test
  void explicitSortedBuildModeOverHttpIsAccepted() throws Exception {
    populate("Work");

    command(0, "CREATE INDEX ON Work (id) NOTUNIQUE METADATA {buildMode: 'SORTED'}");

    assertBuiltSorted("Work");
  }

  private void populate(final String typeName) {
    final Database database = getServerDatabase(0, getDatabaseName());
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(2).create();
    type.createProperty("id", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(typeName).set("id", "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL)))
            .save();
    });
  }

  /**
   * The sorted build writes the entries straight into a compacted sub-index; the record-at-a-time build leaves them all in the
   * mutable pages, with no sub-index until a compaction runs.
   */
  private void assertBuiltSorted(final String typeName) {
    final Database database = getServerDatabase(0, getDatabaseName());
    final TypeIndex index = database.getSchema().getType(typeName).getIndexByProperties("id");
    assertThat(index).isNotNull();
    assertThat(index.countEntries()).isEqualTo(RECORDS);
    for (final Index bucketIndex : index.getIndexesOnBuckets())
      assertThat(((LSMTreeIndex) bucketIndex).getMutableIndex().getSubIndex()).as("compacted sub-index of %s", bucketIndex.getName())
          .isNotNull();
  }
}
