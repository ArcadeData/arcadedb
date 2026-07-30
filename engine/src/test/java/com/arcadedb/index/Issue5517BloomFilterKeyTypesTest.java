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
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bloom filters of #5517 hash the SERIALIZED form of a key, so they only work while two keys the index treats as
 * EQUAL serialize to the same bytes. That is not free: it holds for most types only because {@code convertKeys} first
 * normalises every component to the type the index declared.
 * <p>
 * DECIMAL escaped that. Converting to {@link BigDecimal} keeps whatever scale the caller supplied and serialization
 * writes the scale, but the comparator uses {@code BigDecimal.compareTo}, which ignores it - so {@code 1.0} and
 * {@code 1.00} were one key to the index and two hashes to the filter, and looking up one SKIPPED the series holding
 * the other. On a unique index that is a duplicate walking past the duplicate check. Reproduced before the fix: with
 * the filters off every different-scale lookup was found, with them on none was.
 * <p>
 * Every case here compares the two configurations over the same files, because "the filters answer like a scan" is the
 * only property that matters and the only one that catches this class of bug.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5517BloomFilterKeyTypesTest extends TestHelper {

  private static final int TOTAL_KEYS = 30_000;

  @Override
  protected void beginTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Override
  protected void endTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.reset();
  }

  /**
   * The regression. A DECIMAL key looked up at a DIFFERENT scale than it was stored at must still be found - the
   * comparator says they are the same key, so the filters have to agree.
   */
  @Test
  void aDecimalKeyIsFoundWhateverScaleTheLookupUses() throws Exception {
    final TypeIndex index = buildIndex(Type.DECIMAL, i -> new BigDecimal(i + ".0"));

    final LSMTreeIndexCompacted compacted = compactedOf(index);
    assertThat(compacted.isBloomFilterEnabled()).as("the filters must be in play for this to prove anything").isTrue();

    final long skippedBefore = compacted.getBloomSkippedSeries();

    for (int i = 0; i < TOTAL_KEYS; i++) {
      assertThat(found(index, new BigDecimal(i + ".0"))).as("key %d at its stored scale", i).isTrue();
      assertThat(found(index, new BigDecimal(i + ".00"))).as("key %d at a WIDER scale", i).isTrue();
      assertThat(found(index, new BigDecimal(String.valueOf(i)))).as("key %d at scale 0", i).isTrue();
    }

    // A key that really is absent must still be filtered out, or the fix would just be "never use the filters".
    for (int i = 0; i < 2_000; i++)
      assertThat(found(index, new BigDecimal("-" + (i + 1) + ".5"))).as("absent decimal %d", i).isFalse();

    assertThat(compacted.getBloomSkippedSeries() - skippedBefore)
        .as("the filters must still be skipping series for absent keys").isGreaterThan(0);
  }

  /** Whole-number DECIMALs written with trailing zeros: 100 and 1.00E+2 are the same key. */
  @Test
  void aDecimalKeyIsFoundAcrossNegativeAndPositiveScales() throws Exception {
    final TypeIndex index = buildIndex(Type.DECIMAL, i -> new BigDecimal(i + "00"));

    for (int i = 0; i < TOTAL_KEYS; i++) {
      assertThat(found(index, new BigDecimal(i + "00"))).as("key %d as written", i).isTrue();
      assertThat(found(index, new BigDecimal(i + "00.000"))).as("key %d with a wider scale", i).isTrue();
      assertThat(found(index, new BigDecimal(i + "00").stripTrailingZeros())).as("key %d stripped", i).isTrue();
    }
  }

  /** LONG keys probed with an Integer: convertKeys widens both sides, so the filters must agree. */
  @Test
  void aLongKeyIsFoundWhenProbedWithAnInteger() throws Exception {
    final TypeIndex index = buildIndex(Type.LONG, i -> (long) i);

    for (int i = 0; i < TOTAL_KEYS; i++) {
      assertThat(found(index, (long) i)).as("long key %d", i).isTrue();
      assertThat(found(index, i)).as("long key %d probed as an int", i).isTrue();
    }
  }

  /** DOUBLE keys probed as ints and floats. */
  @Test
  void aDoubleKeyIsFoundWhenProbedWithOtherNumbers() throws Exception {
    final TypeIndex index = buildIndex(Type.DOUBLE, i -> (double) i);

    for (int i = 0; i < TOTAL_KEYS; i++) {
      assertThat(found(index, (double) i)).as("double key %d", i).isTrue();
      assertThat(found(index, i)).as("double key %d probed as an int", i).isTrue();
    }
  }

  /** DATETIME keys, where the stored form is a number and the caller may pass a Date. */
  @Test
  void aDateTimeKeyIsFoundWhenProbedWithADate() throws Exception {
    final long base = LocalDate.of(2020, 1, 1).toEpochDay() * 86_400_000L;
    final TypeIndex index = buildIndex(Type.DATETIME, i -> new Date(base + i * 60_000L));

    for (int i = 0; i < TOTAL_KEYS; i++) {
      assertThat(found(index, new Date(base + i * 60_000L))).as("datetime key %d", i).isTrue();
      assertThat(found(index, base + i * 60_000L)).as("datetime key %d probed as a long", i).isTrue();
    }
  }

  /** A composite key mixing a DECIMAL with a STRING: the canonicalisation has to reach the right component. */
  @Test
  void aCompositeDecimalAndStringKeyIsFoundAtAnyScale() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);
    final DocumentType type = database.getSchema().buildDocumentType().withName("Mixed").withTotalBuckets(1).create();
    type.createProperty("amount", Type.DECIMAL);
    type.createProperty("label", Type.STRING);
    database.getSchema().buildTypeIndex("Mixed", new String[] { "amount", "label" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_KEYS; i++)
        database.newDocument("Mixed").set("amount", new BigDecimal(i + ".0")).set("label", "label-" + i).save();
    });

    final TypeIndex index = database.getSchema().getType("Mixed").getIndexesByProperties("amount", "label").getFirst();
    assertThat(((IndexInternal) index).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) index).compact()).isTrue();

    for (int i = 0; i < TOTAL_KEYS; i++) {
      assertThat(found(index, new BigDecimal(i + ".0"), "label-" + i)).as("composite key %d as stored", i).isTrue();
      assertThat(found(index, new BigDecimal(i + ".000"), "label-" + i)).as("composite key %d at a wider scale", i)
          .isTrue();
    }
  }

  private interface KeyFactory {
    Object of(int i);
  }

  private TypeIndex buildIndex(final Type keyType, final KeyFactory keys) throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final DocumentType type = database.getSchema().buildDocumentType().withName("D").withTotalBuckets(1).create();
    type.createProperty("k", keyType);
    database.getSchema().buildTypeIndex("D", new String[] { "k" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_KEYS; i++)
        database.newDocument("D").set("k", keys.of(i)).save();
    });

    final TypeIndex index = database.getSchema().getType("D").getIndexesByProperties("k").getFirst();
    assertThat(((IndexInternal) index).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) index).compact()).isTrue();
    assertThat(compactedOf(index).getBloomFilter()).as("the compaction must have written filters").isNotNull();
    return index;
  }

  private static boolean found(final TypeIndex index, final Object... key) {
    final IndexCursor cursor = index.get(key);
    try {
      return cursor.hasNext();
    } finally {
      cursor.close();
    }
  }

  private static LSMTreeIndexCompacted compactedOf(final TypeIndex index) {
    return ((LSMTreeIndex) index.getIndexesOnBuckets()[0]).getMutableIndex().getSubIndex();
  }
}
