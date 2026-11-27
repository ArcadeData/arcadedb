/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A same-size overwrite of a record, with the pre-image the disjoint-slot merge keeps, writes to the page the span from the first to
 * the last byte that differ and no more. The boundaries are the first byte, the last byte, both ends, a byte in the middle and no
 * difference at all (written whole, as before): each leaves the page holding exactly the new record.
 */
class LocalBucketSameSizeOverwriteTest extends TestHelper {
  private static final int LENGTH = 4_000;

  @ParameterizedTest(name = "{0}")
  @CsvSource({ "identical,-1,-1,4000", "first byte,0,-1,1", "last byte,3999,-1,1", "middle byte,2000,-1,1", "both ends,0,3999,4000",
      "adjacent pair,1500,1501,2" })
  void anOverwriteWritesTheSpanOfTheBytesThatDiffer(final String name, final int firstChange, final int secondChange,
      final int expectedSpan) throws ReflectiveOperationException {
    database.getSchema().createDocumentType("Wide", 1);
    final String stored = "a".repeat(LENGTH);
    final RID[] created = new RID[1];
    database.transaction(() -> created[0] = database.newDocument("Wide").set("s", stored).save().getIdentity());
    final RID rid = created[0];

    final StringBuilder changed = new StringBuilder(stored);
    if (firstChange >= 0)
      changed.setCharAt(firstChange, 'b');
    if (secondChange >= 0)
      changed.setCharAt(secondChange, 'b');

    database.begin();
    final MutableDocument document = rid.asDocument(true).modify();
    document.set("s", changed.toString());
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    bucket.updateRecord(document, false);
    final int written = bytesMarkedModified();
    database.commit();

    if (expectedSpan >= LENGTH)
      // a record that differs nowhere, or at both ends, is written whole
      assertThat(written).as("bytes marked modified for '%s'", name).isGreaterThanOrEqualTo(LENGTH);
    else
      // the changed span, plus the few bytes a range may be widened by when it is merged with a neighbour
      assertThat(written).as("bytes marked modified for '%s'", name).isBetween(expectedSpan, expectedSpan + 8);

    assertThat(database.lookupByRID(rid, true).asDocument().getString("s")).as("record after '%s'", name).isEqualTo(changed.toString());
  }

  /** The bytes of the pages of the open transaction that its writes marked modified. */
  @SuppressWarnings("unchecked")
  private int bytesMarkedModified() throws ReflectiveOperationException {
    // TransactionContext offers no accessor for the pages it holds: read the field as StatementSavepointPageBytesTest does
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final Field field = TransactionContext.class.getDeclaredField("modifiedPages");
    field.setAccessible(true);
    int written = 0;
    for (final MutablePage page : ((Map<PageId, MutablePage>) field.get(tx)).values()) {
      final int[] ranges = page.getModifiedRanges();
      for (int r = 0; r < page.getModifiedRangeCount(); r++)
        written += ranges[r * 2 + 1] - ranges[r * 2] + 1;
    }
    return written;
  }
}
