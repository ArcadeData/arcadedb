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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12348: a node whose LSM index could not attach its compacted sub-index ("Invalid sub-index ... File with id N was not
 * found") served the index from its mutable pages alone, a partial index that a UNIQUE constraint enforces too, while
 * {@code CHECK DATABASE} reported it healthy. The detach was visible only as a log line at load.
 * <p>
 * The node's state is rebuilt here the way it is reached: the compacted file of a compacted index is gone from the disk when the
 * database opens.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12348DetachedSubIndexTest extends TestHelper {
  private static final String TYPE_NAME      = "Event";
  private static final int    COMPACTED_KEYS = 3_000;
  private static final int    MUTABLE_KEYS   = 200;

  @Test
  void aSubIndexTheLoadCouldNotAttachIsReportedHeldFromCompactionAndRepairedByFix() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("eid", Type.INTEGER);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "eid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(4_096).create();
    });

    insert(0, COMPACTED_KEYS);

    final LSMTreeIndex bucketIndex = bucketIndex();
    assertThat(bucketIndex.scheduleCompaction()).isTrue();
    assertThat(bucketIndex.compact()).as("the index must be compacted to have a sub-index to lose").isTrue();

    final LSMTreeIndexCompacted compacted = bucketIndex().getMutableIndex().getSubIndex();
    assertThat(compacted).isNotNull();
    final String compactedPath = ((DatabaseInternal) database).getFileManager().getFile(compacted.getFileId()).getFilePath();
    final String compactedName = new File(compactedPath).getName();
    final String compactedBase = compactedName.substring(0, compactedName.lastIndexOf('.'));

    // keys written after the compaction stay in the mutable file, the only part a detached index still serves
    insert(COMPACTED_KEYS, MUTABLE_KEYS);

    database.close();
    final File[] files = new File(database.getDatabasePath()).listFiles((dir, name) -> name.startsWith(compactedBase + "."));
    assertThat(files).isNotEmpty();
    for (final File f : files)
      assertThat(f.delete()).isTrue();
    database = factory.open();

    final LSMTreeIndexMutable mutable = bucketIndex().getMutableIndex();
    assertThat(mutable.getSubIndex()).isNull();
    assertThat(mutable.getDetachedSubIndex()).as("the load must remember why the sub-index is not attached").contains("could not be loaded");
    assertThat(countByIndex()).as("the index serves its mutable pages only").isGreaterThanOrEqualTo(MUTABLE_KEYS)
        .isLessThan(COMPACTED_KEYS);

    // the read-only check reports it as a corrupted index, naming why
    final Result check = check("CHECK DATABASE");
    assertThat((Collection<String>) check.getProperty("corruptedIndexes")).contains(bucketIndex().getName());
    assertThat(String.valueOf(check.<Collection<String>>getProperty("warnings"))).contains("could not be loaded")
        .contains("must be rebuilt");

    // a compaction would write the partial index out as the whole one and drop the only trace of the loss
    assertThat(bucketIndex().scheduleCompaction()).isTrue();
    assertThat(bucketIndex().compact()).isFalse();
    assertThat(bucketIndex().getMutableIndex().getDetachedSubIndex()).isNotNull();

    final Result fix = check("CHECK DATABASE FIX");
    assertThat((Collection<String>) fix.getProperty("rebuiltIndexes")).contains(bucketIndex().getName());

    assertThat(bucketIndex().getMutableIndex().getDetachedSubIndex()).isNull();
    assertThat(countByIndex()).as("FIX rebuilds the index from the records").isEqualTo(COMPACTED_KEYS + MUTABLE_KEYS);

    final Result after = check("CHECK DATABASE");
    assertThat((Collection<String>) after.getProperty("corruptedIndexes")).isEmpty();
  }

  private void insert(final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newDocument(TYPE_NAME).set("eid", i).save();
    });
  }

  private LSMTreeIndex bucketIndex() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private long countByIndex() {
    long found = 0;
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    for (int i = 0; i < COMPACTED_KEYS + MUTABLE_KEYS; i++)
      if (typeIndex.get(new Object[] { i }).hasNext())
        ++found;
    return found;
  }

  private Result check(final String statement) {
    try (final ResultSet rs = database.command("sql", statement)) {
      return rs.next();
    }
  }
}
