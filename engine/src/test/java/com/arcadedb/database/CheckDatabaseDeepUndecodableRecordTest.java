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

import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A record whose slot and chain are sound but whose CONTENT does not decode - what a record keeps after its tail
 * chunk was overwritten by another record's bytes - passes every structural check. CHECK DATABASE DEEP decodes every
 * property of every record and lists the ones that fail, so they can be restored from their source. It never deletes
 * them: part of their content still reads, and nothing here can say which part is wrong.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseDeepUndecodableRecordTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Doc";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the record is left undecodable on purpose, and DEEP findings are not repaired
    return false;
  }

  @Test
  void deepListsARecordWhoseContentDoesNotDecode() throws Exception {
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1);
      for (int i = 0; i < rids.length; i++)
        rids[i] = database.newDocument(TYPE).set("v", "value-" + i).save().getIdentity();
    });

    // The last property's value is [type][length]["value-1"]: turn its type byte into one no decoder knows, written
    // to the file behind the database's back so no commit re-flows the page first
    final long[] recordEnd = new long[1];
    database.transaction(() -> recordEnd[0] = onSlot(rids[1], page -> {
      final int offset = recordOffsetOf(page, rids[1]);
      final long[] size = page.readNumberAndSize(offset);
      return offset + size[1] + size[0];
    }));
    final LocalBucket bucket = bucketOf(TYPE);
    final String filePath = ((DatabaseInternal) database).getFileManager().getFile(bucket.getFileId()).getFilePath();
    final long pageNumber = rids[1].getPosition() / bucket.getMaxRecordsInPage();
    final long typeBytePosition = pageNumber * bucket.getPageSize() + BasePage.PAGE_HEADER_SIZE + recordEnd[0] - ("value-1".length() + 2);
    database.close();
    try (final RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
      file.seek(typeBytePosition);
      file.write(0x7F);
    }
    reopenDatabase();

    final Result plain = checkDatabaseRow(false);
    assertThat(numberProperty(plain, "totalErrors")).as("structurally the record is sound: " + plain.toJSON()).isZero();
    assertThat(numberProperty(plain, "totalUndecodableRecords")).as("only DEEP decodes content").isZero();

    final Result deep = row("check database deep");
    assertThat(numberProperty(deep, "totalUndecodableRecords")).as(deep.toJSON().toString()).isEqualTo(1L);
    assertThat(((Collection<?>) deep.getProperty("undecodableRecords")).stream().map(Object::toString).toList())
        .containsExactly(rids[1].toString());
    assertThat(warningsOf(deep).toString()).contains(rids[1].toString());

    row("check database fix deep");
    assertThat(countRecords(TYPE)).as("DEEP findings are reported, never deleted").isEqualTo(3L);
  }

  @Test
  void deepFindsNothingOnHealthyRecordsOfEveryShape() {
    database.transaction(() -> {
      final var vertexType = database.getSchema().createVertexType("V");
      vertexType.createProperty("embedding", com.arcadedb.schema.Type.ARRAY_OF_FLOATS).setExternal(true);
      database.getSchema().createEdgeType("E");
      database.getSchema().createDocumentType(TYPE, 1);
      com.arcadedb.graph.MutableVertex previous = null;
      for (int i = 0; i < 50; i++) {
        final com.arcadedb.graph.MutableVertex v = database.newVertex("V").set("name", "v" + i)
            .set("embedding", new float[] { i, i + 1, i + 2 }).set("tags", java.util.List.of("a", "b" + i))
            .set("attributes", java.util.Map.of("k", i)).set("big", "x".repeat(i % 5 == 0 ? 90_000 : 10)).save();
        if (previous != null)
          previous.newEdge("E", v, "weight", i);
        previous = v;
        final MutableDocument doc = database.newDocument(TYPE).set("when", new java.util.Date());
        doc.newEmbeddedDocument(TYPE, "child").set("n", i);
        doc.save();
      }
    });

    final Result deep = row("check database deep");
    assertThat(numberProperty(deep, "totalUndecodableRecords")).as(deep.toJSON().toString()).isZero();
    assertThat(numberProperty(deep, "totalErrors")).as(deep.toJSON().toString()).isZero();
  }

  private Result row(final String sql) {
    try (final var rs = database.command("sql", sql)) {
      return rs.next();
    }
  }
}
