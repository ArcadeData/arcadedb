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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4420: a record whose on-disk buffer is corrupted (e.g. produced by a version affected by
 * the HA WAL-range bug #4319) used to be impossible to delete - reading its indexed property to clean up the index threw
 * {@link NegativeArraySizeException} during commit, aborting the whole transaction ("records that can't be deleted").
 *
 * <p>This test corrupts the length prefix of an indexed string property directly on the page, then verifies that:
 * <ol>
 *   <li>reading the property now surfaces a clear {@link SerializationException} (not a cryptic NegativeArraySizeException), and</li>
 *   <li>the corrupted record can still be deleted (best-effort index cleanup) instead of staying stuck.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4420TolerantDeleteTest extends TestHelper {

  private static final String MARKER = "CORRUPT4420MARKERVALUE";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("doc");
      database.getSchema().getType("doc").createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "doc", "name");
    });
  }

  @Test
  void corruptedRecordCanStillBeDeleted() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("doc").set("name", MARKER).save().getIdentity());

    database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo(MARKER));

    corruptPropertyLength(rid[0]);

    // Reopen so the record is re-read fresh from the corrupted page instead of the in-memory record cache.
    reopenDatabase();

    // 1) Reading the corrupted property is tolerated (the corrupt length no longer crashes the reader with a cryptic
    //    NegativeArraySizeException; the unreadable value is reported as absent).
    database.transaction(() -> assertThat((Object) database.lookupByRID(rid[0], true).asDocument().get("name")).isNull());

    // 2) The corrupted record can still be deleted (the user's complaint: "records that can't be deleted").
    database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().delete());

    database.transaction(() -> assertThat(database.countType("doc", false)).isEqualTo(0L));
  }

  // 4_294_967_245L == (2^32 - 51); cast to int it becomes -51, the exact value reported in issue #4420.
  private static final long CORRUPT_LENGTH = 4_294_967_245L;

  /**
   * Overwrites the length-prefix varint of the inline {@link #MARKER} string value with a varint that decodes to a value
   * larger than {@link Integer#MAX_VALUE} (wrapping to -51 when cast to int). The record's declared size in the page
   * record table is left untouched, so the record still loads; only deserializing the property value fails.
   */
  private void corruptPropertyLength(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final byte[] marker = MARKER.getBytes(StandardCharsets.UTF_8);

    final Binary varintBuffer = new Binary();
    varintBuffer.putUnsignedNumber(CORRUPT_LENGTH);
    final byte[] corruptVarint = varintBuffer.toByteArray();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        final int contentSize = page.getContentSize();
        final byte[] content = new byte[contentSize];
        page.readByteArray(0, content);

        final int markerStart = indexOf(content, marker);
        assertThat(markerStart).as("marker value must be present inline in the page").isGreaterThan(0);

        // The byte immediately before the inline value is its length varint; overwrite it (and the following bytes it now
        // spans) with the multi-byte corrupt length. The marker is long enough that this stays inside the record content.
        final int lengthPos = markerStart - 1;
        for (int i = 0; i < corruptVarint.length; i++)
          page.writeByte(lengthPos + i, corruptVarint[i]);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static int indexOf(final byte[] haystack, final byte[] needle) {
    outer:
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      for (int j = 0; j < needle.length; j++)
        if (haystack[i + j] != needle[j])
          continue outer;
      return i;
    }
    return -1;
  }
}
