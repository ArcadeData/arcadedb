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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10020: {@link LocalSchema#loadIncremental} used to answer {@code false} without saying why, so the full load a
 * replica then ran on its apply thread could not be tied to anything. {@link LocalSchema#tryLoadIncremental} reports the
 * reason; this pins one test per way the incremental refresh refuses, and that a refusal names the file that caused it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10020IncrementalLoadRefusalReasonTest extends TestHelper {

  private static final String TYPE_NAME = "Issue10020Indexed";
  private static final int    RECORDS   = 2_000;

  @Override
  protected void beginTest() {
    // A small index page size makes the compaction write a real series with a bloom filter over it
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("value", Integer.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true).withPageSize(1024).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("value", i).save();
    });

    final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final Exception e) {
      throw new IllegalStateException("cannot compact the fixture index", e);
    }
  }

  @Test
  void anEntryThatCanBeAppliedIncrementallyReportsNoRefusal() throws Exception {
    assertThat(schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isNull();
    assertThat(schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();
  }

  @Test
  void aRetiredFileIsReported() throws Exception {
    final int anyFileId = database.getSchema().getType(TYPE_NAME).getBuckets(false).get(0).getFileId();

    final IncrementalLoadRefusal refusal = schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(anyFileId), Set.of());

    assertThat(refusal).isNotNull();
    assertThat(refusal.reason()).isEqualTo(FullSchemaLoadReason.FILE_RETIRED);
    assertThat(refusal.detail()).contains(String.valueOf(anyFileId));
    assertThat(schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(anyFileId), Set.of())).isFalse();
  }

  @Test
  void aTouchedBloomFilterIsReportedWithItsExtension() throws Exception {
    final int filterFileId = fileIdOfRegistered(LSMTreeIndexBloomFilter.class);
    assertThat(filterFileId).as("the fixture must produce a bloom filter, otherwise this test asserts nothing").isNotNegative();

    final IncrementalLoadRefusal refusal = schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(filterFileId));

    assertThat(refusal).isNotNull();
    assertThat(refusal.reason()).isEqualTo(FullSchemaLoadReason.NON_INCREMENTAL_TOUCHED_FILE);
    assertThat(refusal.detail()).contains(LSMTreeIndexBloomFilter.FILE_EXT).contains(String.valueOf(filterFileId));
  }

  @Test
  void aTouchedCompactedIndexIsReportedWithItsExtension() throws Exception {
    final int compactedFileId = fileIdOfRegistered(LSMTreeIndexCompacted.class);
    assertThat(compactedFileId).as("the fixture must produce a compacted index, otherwise this test asserts nothing")
        .isNotNegative();

    final IncrementalLoadRefusal refusal = schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(compactedFileId));

    assertThat(refusal).isNotNull();
    assertThat(refusal.reason()).isEqualTo(FullSchemaLoadReason.NON_INCREMENTAL_TOUCHED_FILE);
    assertThat(refusal.detail()).contains(String.valueOf(compactedFileId));
  }

  @Test
  void everyReasonHasADistinctMetricTag() {
    final Set<String> tags = new HashSet<>();
    for (final FullSchemaLoadReason reason : FullSchemaLoadReason.values()) {
      assertThat(reason.getTag()).as("tag of %s", reason).matches("[a-z][a-z0-9_]*");
      assertThat(tags.add(reason.getTag())).as("tag of %s must be unique", reason).isTrue();
    }
    assertThat(Arrays.stream(FullSchemaLoadReason.values()).map(FullSchemaLoadReason::getDescription))
        .allSatisfy(description -> assertThat(description).isNotBlank());
  }

  private int fileIdOfRegistered(final Class<? extends Component> componentClass) {
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
      if (file == null)
        continue;
      final Component component = schema().getFileByIdIfExists(file.getFileId());
      if (componentClass.isInstance(component))
        return component.getFileId();
    }
    return -1;
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }
}
