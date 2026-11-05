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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11206: the write listeners took the record's type from whoever built the record, so a walk that mis-typed a
 * subtype record as its supertype (#11043) fired the supertype's delete listeners and skipped the subtype's veto, silently.
 * The type now refuses to dispatch a record whose bucket another type owns, for every walk rather than for the walks
 * that were checked, and the API refuses to create a record in a bucket another type owns, which is where such a record
 * came from without any walk at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11206ListenerDispatchOwnerGuardTest extends TestHelper {
  private final AtomicInteger baseListenerCalls = new AtomicInteger();
  private final AtomicInteger subListenerCalls  = new AtomicInteger();
  private       RID           subRecord;

  @BeforeEach
  void createHierarchy() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Base").create();
      database.getSchema().buildDocumentType().withName("Sub").withSuperType("Base").create();
      subRecord = database.newDocument("Sub").set("n", "sub").save().getIdentity();
    });

    for (final String typeName : new String[] { "Base", "Sub" }) {
      final AtomicInteger calls = typeName.equals("Base") ? baseListenerCalls : subListenerCalls;
      final DocumentType type = database.getSchema().getType(typeName);
      type.getEvents().registerListener((BeforeRecordDeleteListener) record -> calls.incrementAndGet() >= 0);
      type.getEvents().registerListener((AfterRecordDeleteListener) record -> calls.incrementAndGet());
      type.getEvents().registerListener((BeforeRecordUpdateListener) record -> calls.incrementAndGet() >= 0);
      type.getEvents().registerListener((AfterRecordUpdateListener) record -> calls.incrementAndGet());
    }
  }

  @Test
  void aRecordBuiltAsTheSupertypeIsNotDeletedThroughTheSupertypeListeners() {
    assertThatThrownBy(() -> database.transaction(() -> database.deleteRecord(misTyped())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'Base'").hasMessageContaining("'Sub'");

    assertThat(baseListenerCalls.get()).as("no listener of the wrong type may be consulted").isZero();
    assertThat(subListenerCalls.get()).isZero();
    assertThat(database.countType("Sub", false)).as("the refused delete deleted nothing").isEqualTo(1L);
  }

  @Test
  void aRecordBuiltAsTheSupertypeIsNotUpdatedThroughTheSupertypeListeners() {
    assertThatThrownBy(() -> database.transaction(() -> misTyped().modify().set("n", "changed").save()))
        .isInstanceOf(IllegalStateException.class);

    assertThat(baseListenerCalls.get()).isZero();
    assertThat(database.lookupByRID(subRecord, true).asDocument().getString("n")).isEqualTo("sub");
  }

  @Test
  void aRecordTypedByItsBucketOwnerReachesItsOwnListeners() {
    database.transaction(() -> database.lookupByRID(subRecord, true).asDocument().modify().set("n", "changed").save());
    assertThat(subListenerCalls.get()).as("before and after update").isEqualTo(2);

    database.transaction(() -> database.deleteRecord(database.lookupByRID(subRecord, true)));
    assertThat(subListenerCalls.get()).as("before and after delete").isEqualTo(4);
    assertThat(baseListenerCalls.get()).isZero();
    assertThat(database.countType("Sub", false)).isZero();
  }

  @Test
  void aRecordCannotBeCreatedInABucketAnotherTypeOwns() {
    final String subBucket = database.getSchema().getType("Sub").getBuckets(false).get(0).getName();

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Base").set("n", "x").save(subBucket)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'Base'").hasMessageContaining(subBucket).hasMessageContaining("'Sub'");
    assertThat(database.countType("Sub", false)).isEqualTo(1L);
    assertThat(database.countType("Base", false)).isZero();

    // the type's own bucket, named explicitly, is still a valid target
    database.transaction(() -> database.newDocument("Sub").set("n", "y").save(subBucket));
    assertThat(database.countType("Sub", false)).isEqualTo(2L);
  }

  /** The Sub record, built as a Base: the shape a walk that hoisted the scanned type produced (#11043). */
  private Document misTyped() {
    return (Document) ((DatabaseInternal) database).getRecordFactory()
        .newImmutableRecord(database, database.getSchema().getType("Base"), subRecord, Document.RECORD_TYPE);
  }
}
