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
import com.arcadedb.engine.Dictionary;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for finding 3 of issue #7266.
 * <p>
 * {@link LocalSchema#loadIncremental} decides in two passes, and its second one - the files this entry wrote pages
 * into - narrowed on {@code getMainComponent() instanceof IndexInternal} BEFORE consulting the extension set that
 * names the components which cannot be refreshed in isolation. A bloom filter answers ITSELF from
 * {@code getMainComponent()} and is not an index, so a touched {@code .bfidx} short-circuited out of the loop and
 * never reached the guard: the entry stayed on the incremental path with the filter's in-RAM directory still
 * describing the file as it was before the entry appended to it. The class javadoc and the {@code attachBloomFilters}
 * skip comment both claimed the fallback happened "in both passes", so the guard a reader relies on to know when a
 * rebuild is forced said one thing and did another.
 * <p>
 * The dictionary is the deliberate exception, and the last test pins it: every DDL entry that adds a type or a
 * property name writes dictionary pages, so those file ids arrive in {@code touchedFileIds} on the common path.
 * Sending them to the full rebuild would undo issue #6988 - which is the whole reason this method exists - and the
 * second pass's own comment already argues the dictionary needs nothing here.
 */
class Issue7266IncrementalSchemaSecondPassTest extends TestHelper {

  private static final String TYPE_NAME = "Issue7266Indexed";
  private static final int    RECORDS   = 10_000;

  @Override
  protected void beginTest() {
    // A small index page size is what makes the compacted output span several pages, i.e. a real series with a real
    // bloom filter over it - the same fixture shape Issue5662IndexCursorFollowUpsTest uses. Without it the whole
    // index fits one page and the compactor has nothing to write.
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

    compactTheIndex();
  }

  /**
   * The defect as reported: a touched bloom filter is not an {@code IndexInternal}, so the narrowing dropped it
   * before the extension check could refuse the entry.
   */
  @Test
  void aTouchedBloomFilterForcesTheFullRebuild() throws Exception {
    final LocalSchema schema = schema();
    final int filterFileId = fileIdOfRegistered(schema, LSMTreeIndexBloomFilter.class);
    assertThat(filterFileId)
        .as("the fixture must produce a bloom filter component, otherwise this test asserts nothing")
        .isNotNegative();

    final Map<Integer, Component> before = componentsByFileId(schema);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(filterFileId)))
        .as("an entry writing pages into a bloom filter cannot be expressed incrementally")
        .isFalse();

    // A refusal must be a no-op, so the caller's fallback runs over consistent state.
    assertThat(componentsByFileId(schema)).isEqualTo(before);
  }

  /**
   * A touched compacted index refuses today too, but only as a side effect: its {@code getMainComponent()} answers
   * the mutable index that {@code LSMTreeIndexMutable.onAfterLoad} wired into it, and a compacted component the
   * factory builds starts with that field null. Nothing in the guard said so, so this pins the refusal itself
   * rather than the accident that produced it.
   */
  @Test
  void aTouchedCompactedIndexForcesTheFullRebuild() throws Exception {
    final LocalSchema schema = schema();
    final int compactedFileId = fileIdOfRegistered(schema, LSMTreeIndexCompacted.class);
    assertThat(compactedFileId)
        .as("the fixture must produce a compacted index component, otherwise this test asserts nothing")
        .isNotNegative();

    final Map<Integer, Component> before = componentsByFileId(schema);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(compactedFileId)))
        .as("an entry writing pages into a compacted index cannot be expressed incrementally")
        .isFalse();

    assertThat(componentsByFileId(schema)).isEqualTo(before);
  }

  /**
   * The other half of the contract, and the one a wholesale "check the extension first" fix would have broken: a
   * touched dictionary stays on the incremental path. Every DDL entry that adds a name writes dictionary pages, so
   * refusing here would send the common case back to the O(total files) rebuild #6988 removed.
   */
  @Test
  void aTouchedDictionaryStaysIncremental() throws Exception {
    final LocalSchema schema = schema();
    final int dictionaryFileId = fileIdOfRegistered(schema, Dictionary.class);
    assertThat(dictionaryFileId).as("every database has a dictionary").isNotNegative();

    final Map<Integer, Component> before = componentsByFileId(schema);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(dictionaryFileId)))
        .as("a DDL entry writing new names into the dictionary is the common case and must stay incremental")
        .isTrue();

    for (final Map.Entry<Integer, Component> entry : before.entrySet())
      assertThat(schema.getFileByIdIfExists(entry.getKey()))
          .as("component for file %d must not be re-instantiated for a dictionary write", entry.getKey())
          .isSameAs(entry.getValue());
  }

  private void compactTheIndex() {
    final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator()
        .next().getIndexesOnBuckets()[0];
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final Exception e) {
      throw new IllegalStateException("cannot compact the fixture index", e);
    }
  }

  /** The file id of the first registered component of the given class, or -1 when the fixture produced none. */
  private int fileIdOfRegistered(final LocalSchema schema, final Class<? extends Component> componentClass) {
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
      if (file == null)
        continue;
      final Component component = schema.getFileByIdIfExists(file.getFileId());
      if (componentClass.isInstance(component))
        return component.getFileId();
    }
    return -1;
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }

  private Map<Integer, Component> componentsByFileId(final LocalSchema schema) {
    final Map<Integer, Component> result = new HashMap<>();
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
      if (file == null)
        continue;
      final Component component = schema.getFileByIdIfExists(file.getFileId());
      if (component != null)
        result.put(file.getFileId(), component);
    }
    return result;
  }
}
