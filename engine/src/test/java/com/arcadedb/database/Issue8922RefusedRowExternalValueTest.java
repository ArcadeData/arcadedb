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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #8922. A row refused by a unique index after its body was written is taken back by
 * {@code undoRecordWrite}, which freed the primary record only: the externalised values of an EXTERNAL property stayed
 * committed in the paired {@code *_ext} bucket, unreferenced, and its persisted record count stayed one too high.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8922RefusedRowExternalValueTest extends TestHelper {

  private static final String BLOB = "x".repeat(200);

  @Test
  void refusedRowInCallerOwnedTransactionLeavesNoExternalRecord() {
    final DocumentType type = database.getSchema().createDocumentType("T");
    type.createProperty("id", Type.STRING);
    type.createProperty("blob", Type.STRING).setExternal(true);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T", "id");

    database.transaction(() -> {
      database.newDocument("T").set("id", "a").set("blob", "A" + BLOB).save();
      database.newDocument("T").set("id", "b").set("blob", "B" + BLOB).save();
      assertThat(catchThrowable(() -> database.newDocument("T").set("id", "a").set("blob", "C" + BLOB).save()))
          .isInstanceOf(DuplicatedKeyException.class);
    });

    assertThat(database.countType("T", false)).isEqualTo(2);
    assertThat(externalCount(type)).isEqualTo(2);

    database.close();
    database = factory.open();

    final DocumentType reopened = database.getSchema().getType("T");
    assertThat(externalCount(reopened)).isEqualTo(2);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Object orphans = rs.next().getProperty("orphanedExternalRecords");
      assertThat(orphans == null ? 0L : ((Number) orphans).longValue()).isZero();
    }
  }

  private long externalCount(final DocumentType type) {
    long total = 0;
    for (final Bucket bucket : type.getBuckets(false)) {
      final Integer externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId());
      assertThat(externalId).isNotNull();
      total += ((LocalBucket) database.getSchema().getBucketById(externalId)).count();
    }
    return total;
  }
}
