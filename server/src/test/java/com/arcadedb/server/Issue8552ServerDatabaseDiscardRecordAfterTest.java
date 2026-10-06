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
package com.arcadedb.server;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8552: {@code ServerDatabase.createRecordNoLock(record, bucketName, discardRecordAfter)} dropped its third
 * argument and always delegated with {@code false}, so a caller asking a server handle to discard the record buffer
 * after the write had the request silently ignored. The sibling {@code updateRecordNoLock} always passed its flag on.
 * <p>
 * {@code LocalBucket.createRecord} keeps the serialized buffer on the record only when the flag is {@code false}; a new
 * {@link MutableDocument} starts with no buffer, so whether one is set after the create tells which value reached the
 * bucket.
 */
class Issue8552ServerDatabaseDiscardRecordAfterTest extends TestHelper {

  private static final String TYPE = "Issue8552Doc";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE).createProperty("name", Type.STRING);
  }

  private ServerDatabase handle() {
    return new ServerDatabase(null, (DatabaseInternal) ((DatabaseInternal) database).getEmbedded());
  }

  private String bucketName() {
    final DocumentType type = database.getSchema().getType(TYPE);
    return type.getBuckets(false).getFirst().getName();
  }

  @Test
  void createRecordNoLockPassesDiscardTrueThrough() {
    final ServerDatabase handle = handle();
    final MutableDocument doc = database.newDocument(TYPE).set("name", "discarded");
    final RID[] rid = new RID[1];

    database.transaction(() -> {
      handle.createRecordNoLock(doc, bucketName(), true);
      rid[0] = doc.getIdentity();
    });

    assertThat(rid[0]).isNotNull();
    assertThat(((RecordInternal) doc).getBuffer()).as("discardRecordAfter=true must reach the bucket: no buffer kept").isNull();
    // The write itself is unaffected: the record is persisted and readable.
    assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo("discarded");
  }

  @Test
  void createRecordNoLockPassesDiscardFalseThrough() {
    final ServerDatabase handle = handle();
    final MutableDocument doc = database.newDocument(TYPE).set("name", "kept");

    database.transaction(() -> handle.createRecordNoLock(doc, bucketName(), false));

    assertThat(doc.getIdentity()).isNotNull();
    assertThat(((RecordInternal) doc).getBuffer()).as("discardRecordAfter=false keeps the buffer on the record").isNotNull();
    assertThat(database.countType(TYPE, true)).isEqualTo(1L);
  }

  @Test
  void updateRecordNoLockPassesDiscardTrueThrough() {
    final ServerDatabase handle = handle();
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set("name", "before").save().getIdentity());

    final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify();
    doc.set("name", "after");
    // Detach the buffer the load attached, so a non-null buffer after the update can only come from the bucket.
    ((RecordInternal) doc).setBuffer(null);
    database.transaction(() -> handle.updateRecordNoLock(doc, true));

    assertThat(((RecordInternal) doc).getBuffer()).as("parity: updateRecordNoLock already passed its flag on").isNull();
    assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo("after");
  }
}
