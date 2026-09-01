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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link LocalBucket#hasRecordChangedSinceRead(RID, BasePage, Binary)} branch by branch (#6950).
 * <p>
 * The method is the guard that turns a read-modify-write computed from a replaced record image into a retryable
 * conflict, and it deliberately FAILS OPEN: every shape it cannot compare - and any exception while reading the page -
 * answers "unchanged", so that a write which would otherwise succeed is never refused by the guard itself. That is
 * also what makes it worth pinning directly: a refactor that widened one of those "unchanged" answers would silently
 * reopen the lost-update this test's sibling
 * ({@code com.arcadedb.query.sql.executor.ConcurrentUpdateStatementLostUpdateTest}) exists to close, without failing
 * anything.
 */
public class Issue6950RecordImageGuardTest extends TestHelper {
  private static final String TYPE = "Guarded";

  @Test
  void anUnchangedPlainRecordIsNotReportedAsChanged() {
    final RID rid = createRecord("aaa");
    final Binary image = imageOf(rid);

    database.transaction(() -> assertThat(guard(rid, image))
        .as("the very image the record still holds")
        .isFalse());
  }

  @Test
  void aPlainRecordRewrittenWithTheSameLengthIsReportedAsChanged() {
    final RID rid = createRecord("aaa");
    final Binary image = imageOf(rid);

    replaceValueWith(rid, "bbb");

    database.transaction(() -> assertThat(guard(rid, image))
        .as("same length, different bytes: a length check alone would miss this one")
        .isTrue());
  }

  @Test
  void aPlainRecordRewrittenWithADifferentLengthIsReportedAsChanged() {
    final RID rid = createRecord("aaa");
    final Binary image = imageOf(rid);

    replaceValueWith(rid, "a much longer value than the one the image was taken from");

    database.transaction(() -> assertThat(guard(rid, image)).isTrue());
  }

  @Test
  void aDeletedRecordIsNotReportedAsChanged() {
    final RID rid = createRecord("aaa");
    final Binary image = imageOf(rid);

    database.transaction(() -> database.lookupByRID(rid, true).delete());

    database.transaction(() -> assertThat(guard(rid, image))
        .as("a vanished record is reported by the commit's own check (#4959), not by this guard")
        .isFalse());
  }

  @Test
  void aNeverUsedSlotIsNotReportedAsChanged() {
    final RID rid = createRecord("aaa");
    final Binary image = imageOf(rid);
    final RID unusedSlot = new RID(rid.getBucketId(), rid.getPosition() + 1);

    database.transaction(() -> assertThat(guard(unusedSlot, image))
        .as("a slot past the page's record count carries no record to compare")
        .isFalse());
  }

  @Test
  void aMultiPageRecordIsNotReportedAsChanged() {
    final RID rid = createRecord("x".repeat(200_000));
    final Binary image = imageOf(rid);

    replaceValueWith(rid, "y".repeat(200_000));

    database.transaction(() -> assertThat(guard(rid, image))
        .as("a head chunk keeps its body off this page, so the slot cannot answer: the off-page fingerprint does")
        .isFalse());
  }

  private boolean guard(final RID rid, final Binary image) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    try {
      return bucket.hasRecordChangedSinceRead(rid, bucket.fetchPageInTransaction(rid), image);
    } catch (final Exception e) {
      throw new IllegalStateException("Unable to load the page of " + rid, e);
    }
  }

  private RID createRecord(final String value) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      if (!database.getSchema().existsType(TYPE))
        database.getSchema().createDocumentType(TYPE).createProperty("v", Type.STRING);
      rid[0] = database.newDocument(TYPE).set("v", value).save().getIdentity();
    });
    return rid[0];
  }

  private void replaceValueWith(final RID rid, final String value) {
    database.transaction(() -> {
      final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
      doc.set("v", value).save();
    });
  }

  /** The committed image of {@code rid}, detached from the page it was read from. */
  private Binary imageOf(final RID rid) {
    final Binary[] image = new Binary[1];
    database.transaction(
        () -> image[0] = ((RecordInternal) database.lookupByRID(rid, true).asDocument().modify()).getBuffer().copyOfContent());
    return image[0];
  }
}
