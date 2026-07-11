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
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class MultiIndexCursorLifecycleTest extends TestHelper {

  @Test
  void closesEmptyAndExhaustedChildrenExactlyOnce() {
    final TrackingCursor empty = new TrackingCursor();
    final RID expected = new RID(1, 42);
    final TrackingCursor oneEntry = new TrackingCursor(expected);
    final MultiIndexCursor cursor = new MultiIndexCursor(new ArrayList<>(List.of(empty, oneEntry)), -1, true);

    assertThat(empty.closeCalls).isOne();
    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next()).isEqualTo(expected);
    assertThat(cursor.hasNext()).isFalse();
    assertThat(cursor.getRecord()).isEqualTo(expected);
    assertThat(oneEntry.closeCalls).isOne();
    assertThat(cursor.estimateSize()).isZero();

    cursor.close();
    cursor.close();
    assertThat(empty.closeCalls).isOne();
    assertThat(oneEntry.closeCalls).isOne();
  }

  @Test
  void closesRemainingChildrenIdempotently() {
    final TrackingCursor child = new TrackingCursor(new RID(1, 1), new RID(1, 2));
    final MultiIndexCursor cursor = new MultiIndexCursor(new ArrayList<>(List.of(child)), -1, true);

    cursor.close();
    cursor.close();

    assertThat(child.closeCalls).isOne();
    assertThat(cursor.estimateSize()).isZero();
  }

  @Test
  void closesAfterExhaustingMultipleBucketCursors() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("MultiCursorLifecycle").withTotalBuckets(3)
        .create();
    type.createProperty("lookupKey", Type.INTEGER);
    final TypeIndex index = database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "MultiCursorLifecycle",
        "lookupKey");

    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("MultiCursorLifecycle").set("lookupKey", i).save();
    });

    final IndexCursor cursor = index.iterator(true);
    Identifiable last = null;
    int          count = 0;
    while (cursor.hasNext()) {
      last = cursor.next();
      count++;
    }

    assertThat(count).isEqualTo(300);
    assertThat(cursor.getRecord()).isEqualTo(last);
    assertThatCode(cursor::close).doesNotThrowAnyException();
    assertThat(cursor.estimateSize()).isZero();
  }

  private static final class TrackingCursor implements IndexCursor {
    private final List<Identifiable> values;
    private       int                position;
    private       int                closeCalls;
    private       Object[]           keys;

    private TrackingCursor(final Identifiable... values) {
      this.values = List.of(values);
    }

    @Override
    public Object[] getKeys() {
      return keys;
    }

    @Override
    public Identifiable getRecord() {
      return position == 0 ? null : values.get(position - 1);
    }

    @Override
    public boolean hasNext() {
      return position < values.size();
    }

    @Override
    public Identifiable next() {
      if (!hasNext())
        throw new NoSuchElementException();
      keys = new Object[] { position };
      return values.get(position++);
    }

    @Override
    public void close() {
      closeCalls++;
    }

    @Override
    public long estimateSize() {
      return values.size() - position;
    }

    @Override
    public BinaryComparator getComparator() {
      return null;
    }

    @Override
    public byte[] getBinaryKeyTypes() {
      return new byte[0];
    }

    @Override
    public Iterator<Identifiable> iterator() {
      return this;
    }
  }
}
