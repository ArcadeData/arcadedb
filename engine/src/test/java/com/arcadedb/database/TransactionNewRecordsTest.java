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
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The structure behind issue #10813: positions, RIDs and the identity reset of the records a transaction created, held strongly or
 * weakly, across growth, removal from the middle, truncation and entries whose record was collected.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionNewRecordsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void growthRemovalAndTruncationKeepPositionsAndIdentities() {
    final TransactionNewRecords records = new TransactionNewRecords();
    final List<MutableDocument> held = new ArrayList<>();
    // past the initial capacity of 16, alternating strong and weak entries: a toggle of read-your-writes mid-transaction
    for (int i = 0; i < 40; i++) {
      final MutableDocument document = document(i);
      held.add(document);
      records.add(document, i % 2 == 1);
    }
    assertThat(records.size()).isEqualTo(40);
    for (int i = 0; i < 40; i++) {
      assertThat(records.get(i)).isSameAs(held.get(i));
      assertThat(records.getIdentity(i)).isEqualTo(held.get(i).getIdentity());
    }

    // removal from the middle shifts the rest, RIDs with their records
    assertThat(records.lastIndexOf(held.get(20))).isEqualTo(20);
    records.remove(20);
    assertThat(records.size()).isEqualTo(39);
    assertThat(records.lastIndexOf(held.get(20))).isEqualTo(-1);
    assertThat(records.get(20)).isSameAs(held.get(21));
    assertThat(records.getIdentity(20)).isEqualTo(held.get(21).getIdentity());

    // truncation resets the identity of what it takes back, and nothing before the mark
    records.truncateResettingIdentity(30);
    assertThat(records.size()).isEqualTo(30);
    assertThat(held.get(31).getIdentity()).isNull();
    assertThat(held.get(39).getIdentity()).isNull();
    assertThat(held.get(30).getIdentity()).as("at position 29 after the removal, kept").isNotNull();

    records.resetIdentities();
    assertThat(held.get(0).getIdentity()).isNull();
    assertThat(held.get(29).getIdentity()).isNull();

    records.clear();
    assertThat(records.isEmpty()).isTrue();
  }

  @Test
  void aCollectedEntryKeepsItsRidAndIsSkippedByTheReset() {
    final TransactionNewRecords records = new TransactionNewRecords();
    final MutableDocument held = document(1);
    records.add(held, true);
    final WeakReference<MutableDocument> dropped = addDropped(records, 2);
    records.add(document(3), false);

    assertThat(collected(dropped)).isTrue();
    assertThat(records.get(1)).isNull();
    assertThat(records.getIdentity(1)).as("the RID outlives the record").isNotNull();
    assertThat(records.lastIndexOf(held)).isZero();

    records.truncateResettingIdentity(1);
    assertThat(records.size()).isEqualTo(1);
    records.resetIdentities();
    assertThat(held.getIdentity()).isNull();
  }

  private WeakReference<MutableDocument> addDropped(final TransactionNewRecords records, final int id) {
    final MutableDocument document = document(id);
    records.add(document, true);
    return new WeakReference<>(document);
  }

  /** A document with an identity of its own, as a created record has; nothing is written. */
  private MutableDocument document(final int position) {
    final MutableDocument document = database.newDocument("Doc");
    document.setIdentity(new RID(0, position));
    return document;
  }

  private static boolean collected(final WeakReference<?> reference) {
    for (int i = 0; i < 50 && reference.get() != null; i++) {
      System.gc();
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return reference.get() == null;
  }
}
