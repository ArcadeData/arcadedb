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
import com.arcadedb.database.LocalDatabase;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5277: the {@code last-tx-id.bin} recency marker was written only on a
 * clean close, while the runtime WAL rotation dropped the log files that were the counter's only
 * other recovery source. A node force-killed after a rotation (no clean close, no WAL) therefore
 * reported {@code lastTxId=-1} for a fully intact database and was needlessly re-installed from a
 * full leader snapshot at the next HA cold bootstrap. The rotation pass must persist the marker the
 * moment the WAL it replaces is dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerLastTxIdRotationTest extends TestHelper {

  @Test
  void runtimeWalRotationPersistsLastTxIdMarker() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    db.transaction(() -> {
      if (!db.getSchema().existsType("RotationType"))
        db.getSchema().createDocumentType("RotationType");
    });
    db.transaction(() -> db.newDocument("RotationType").set("k", 1).save());

    final long lastTxId = db.getLastTransactionId();
    assertThat(lastTxId).isGreaterThan(0);

    // Precondition of the bug: before any clean close the marker does not exist, so after a WAL drop
    // a force-killed process would have no recency source at all.
    final File marker = new File(db.getDatabasePath(), TransactionManager.LAST_TX_ID_FILE_NAME);
    assertThat(marker).doesNotExist();

    // One runtime rotation pass, as the background timer runs it (retire actives + drop inactives).
    db.getTransactionManager().rotateAndDropWALForTesting();

    assertThat(marker).as("the runtime WAL rotation must persist the recency marker it is deleting the source of")
        .exists();
    try (final DataInputStream in = new DataInputStream(new FileInputStream(marker))) {
      assertThat(in.readLong()).isEqualTo(lastTxId);
    }
  }
}
