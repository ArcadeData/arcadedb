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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review C-M8: {@code LOCK TYPE} / {@code LOCK BUCKET} lock the files a commit of the type writes, its buckets and the files of
 * the indexes the transaction maintains; the shell file of a COLUMNAR index is written by no commit, so it is not locked.
 */
class ColumnarExplicitLockTest extends TestHelper {

  @Test
  void explicitLocksLeaveTheColumnarShellOut() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final DocumentType type = database.getSchema().getType("Asset");
    final IndexInternal key = type.getIndexByProperties("brand").getIndexesOnBuckets()[0];
    final IndexInternal columnar = type.getColumnarIndex("brand").getIndexesOnBuckets()[0];
    final String bucketName = type.getBuckets(false).getFirst().getName();

    database.begin();
    try {
      final LocalTransactionExplicitLock byType = (LocalTransactionExplicitLock) database.acquireLock().type("Asset");
      final LocalTransactionExplicitLock byBucket = (LocalTransactionExplicitLock) database.acquireLock().bucket(bucketName);
      for (final LocalTransactionExplicitLock lock : new LocalTransactionExplicitLock[] { byType, byBucket }) {
        assertThat(lock.collectsFile(key.getFileId())).isTrue();
        for (final int fileId : columnar.getFileIds())
          assertThat(lock.collectsFile(fileId)).as("COLUMNAR file %d", fileId).isFalse();
      }
      byType.lock();
      database.newDocument("Asset").set("brand", "a").save();
    } finally {
      database.commit();
    }
    assertThat(database.countType("Asset", false)).isEqualTo(1);
  }
}
