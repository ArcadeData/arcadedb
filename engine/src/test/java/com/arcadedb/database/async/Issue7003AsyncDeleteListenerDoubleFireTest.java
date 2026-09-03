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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7003: {@code async().deleteRecord()} fired every before/after-delete listener twice,
 * once from the async task and once more from the {@code deleteRecordNoLock} path it delegates to. Both the
 * database-level and the type-level registries are counted, and the veto path is pinned as well: a vetoed delete must
 * neither delete the record nor report success.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7003AsyncDeleteListenerDoubleFireTest extends TestHelper {

  private static final String TYPE = "Issue7003Doc";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
  }

  @Test
  void everyDeleteListenerFiresExactlyOncePerAsyncDelete() {
    final AtomicInteger dbBefore = new AtomicInteger();
    final AtomicInteger dbAfter = new AtomicInteger();
    final AtomicInteger typeBefore = new AtomicInteger();
    final AtomicInteger typeAfter = new AtomicInteger();

    final DocumentType type = database.getSchema().getType(TYPE);
    database.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      dbBefore.incrementAndGet();
      return true;
    }).registerListener((AfterRecordDeleteListener) record -> dbAfter.incrementAndGet());
    type.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      typeBefore.incrementAndGet();
      return true;
    }).registerListener((AfterRecordDeleteListener) record -> typeAfter.incrementAndGet());

    final RID rid = createRecord();

    final AtomicInteger okCalls = new AtomicInteger();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    database.async().deleteRecord(rid.getRecord(), record -> okCalls.incrementAndGet(), error::set);
    database.async().waitCompletion();

    assertThat(error.get()).isNull();
    assertThat(okCalls.get()).isEqualTo(1);
    assertThat(dbBefore.get()).as("database-level beforeDelete").isEqualTo(1);
    assertThat(dbAfter.get()).as("database-level afterDelete").isEqualTo(1);
    assertThat(typeBefore.get()).as("type-level beforeDelete").isEqualTo(1);
    assertThat(typeAfter.get()).as("type-level afterDelete").isEqualTo(1);
    assertThat(database.existsRecord(rid)).isFalse();
  }

  @Test
  void aVetoedAsyncDeleteKeepsTheRecordAndSkipsTheSuccessCallback() {
    final AtomicInteger before = new AtomicInteger();
    final AtomicInteger after = new AtomicInteger();
    database.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      before.incrementAndGet();
      return false;
    }).registerListener((AfterRecordDeleteListener) record -> after.incrementAndGet());

    final RID rid = createRecord();

    final AtomicInteger okCalls = new AtomicInteger();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    database.async().deleteRecord(rid.getRecord(), record -> okCalls.incrementAndGet(), error::set);
    database.async().waitCompletion();

    assertThat(error.get()).isNull();
    assertThat(before.get()).as("the vetoing listener is asked once").isEqualTo(1);
    assertThat(after.get()).as("nothing was deleted, so no afterDelete").isZero();
    assertThat(okCalls.get()).as("a vetoed delete is not a success").isZero();
    assertThat(database.existsRecord(rid)).isTrue();
  }

  private RID createRecord() {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> rid.set(database.newDocument(TYPE).set("id", 1).save().getIdentity()));
    return rid.get();
  }
}
