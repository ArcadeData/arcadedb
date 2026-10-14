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
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10012: a schema change takes the database WRITE lock, and a thread that already holds the READ lock - code
 * running inside {@code lookupByRID}, such as a record read listener - can never be granted it, because a
 * {@code ReentrantReadWriteLock} does not upgrade. The thread parked on itself forever. It is now refused with an
 * exception that says why, and the database stays usable.
 * <p>
 * Every test carries a {@link Timeout}: before the fix the first one does not fail, it hangs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10012ReadLockUpgradeTest extends TestHelper {

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // HANG DETECTOR, NOT A LATENCY BOUND
  void aSchemaChangeFromAReadListenerIsRefusedInsteadOfDeadlocking() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("name", "a").save().getIdentity());

    final AfterRecordReadListener ddlOnRead = record -> {
      database.getSchema().getType("Doc").getProperty("name").setMandatory(true);
      return record;
    };
    type.getEvents().registerListener(ddlOnRead);
    try {
      assertThatThrownBy(() -> database.lookupByRID(rid[0], true))
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("a read lock cannot be upgraded");
    } finally {
      type.getEvents().unregisterListener(ddlOnRead);
    }

    // NOTHING WAS LEFT HALF-TAKEN: THE SAME SCHEMA CHANGE WORKS OUTSIDE THE LISTENER, AND THE RECORD READS
    assertThat(database.getSchema().getType("Doc").getProperty("name").isMandatory()).isFalse();
    database.getSchema().getType("Doc").getProperty("name").setMandatory(true);
    assertThat(database.getSchema().getType("Doc").getProperty("name").isMandatory()).isTrue();
    assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo("a");
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aReaderAskingForTheWriteLockIsRefused() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();

    assertThatThrownBy(() -> local.executeInReadLock(() -> local.executeInWriteLock(() -> null)))
        .isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("a read lock cannot be upgraded");

    // AND THE LOCK IS STILL FREE FOR A WRITER AFTERWARDS
    assertThat(local.executeInWriteLock(() -> "written")).isEqualTo("written");
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aWriterThatAlsoTookTheReadLockStillReentersTheWriteLock() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();

    // A DOWNGRADE IS LEGAL: THE THREAD OWNS THE WRITE LOCK, SO ASKING FOR IT AGAIN IS A RE-ENTRY, NOT AN UPGRADE
    final Object result = local.executeInWriteLock(
        () -> local.executeInReadLock(() -> local.executeInWriteLock(() -> "nested")));

    assertThat(result).isEqualTo("nested");
  }
}
