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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.index.Index;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #6168, item 1: the wiring between the HA gauge and the memoized walk.
 * <p>
 * {@code UnreferencedFiles.MemoizedCount} is tested on its own against a real database in the engine module
 * ({@code Issue6168MemoizedUnreferencedCountTest}); what is left over is the delegation this class holds -
 * {@link RaftHAServer#getUnreferencedFilesSamples()} asks the DATABASE for its count, and the database answers from
 * an instance of the cache it owns, over the database it wraps. Two things can be wrong there and neither is caught
 * by the engine test: the count could be taken over the wrong instance, and the cache could be re-created per call,
 * which would publish correct numbers while quietly doing the very work the memoization exists to avoid.
 * <p>
 * Mocked rather than clustered, deliberately. A gauge reading a per-instance field needs no consensus to exercise,
 * and #6168 item 6 is about the integration lanes being red for reasons unrelated to the change under test - adding
 * an integration test for a delegation would be moving in the wrong direction. The mock's file manager is what makes
 * the second property observable at all: how many times the walk actually read the file list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6168UnreferencedFilesGaugeWiringTest {

  private static final String DB_PATH = "/tmp/issue6168-gauge-wiring-test";

  private LocalDatabase          proxied;
  private FileManager            fileManager;
  private LocalSchema            localSchema;
  private RaftReplicatedDatabase database;

  @BeforeEach
  void setUp() {
    fileManager = mock(FileManager.class);
    when(fileManager.getModificationCount()).thenReturn(1L);
    // One file the schema has no component for: the shape an abandoned instalment leaves on a follower, and the
    // cheapest thing the walk can find. Reported by the FIRST arm of the walk, so no type or index is needed.
    final ComponentFile orphan = mock(ComponentFile.class);
    when(orphan.getFileName()).thenReturn("orphan.0.65536.v0.bucket");
    when(fileManager.getFiles()).thenReturn(List.of(orphan));

    localSchema = mock(LocalSchema.class);
    when(localSchema.getVersion()).thenReturn(7L);
    when(localSchema.getTypes()).thenReturn(List.of());
    when(localSchema.getIndexes()).thenReturn(new Index[0]);
    when(localSchema.getFileByIdIfExists(0)).thenReturn(null);

    final Schema schema = mock(Schema.class);
    when(schema.getEmbedded()).thenReturn(localSchema);

    proxied = mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(DB_PATH);
    when(proxied.getName()).thenReturn("issue6168");
    when(proxied.getTransactionManager()).thenReturn(mock(TransactionManager.class));
    when(proxied.getFileManager()).thenReturn(fileManager);
    when(proxied.getSchema()).thenReturn(schema);

    database = new RaftReplicatedDatabase(null, proxied, mock(RaftHAServer.class));
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.removeContext(DB_PATH);
  }

  /**
   * The count the gauge publishes is taken over the database this wrapper proxies, and the cache is a field of the
   * wrapper rather than something built per call - so the refresh that runs every 5 seconds for the life of the
   * process walks the schema once and then stops.
   */
  @Test
  void theGaugeAnswersFromTheDatabasesOwnCacheAndWalksOnlyOnce() {
    assertThat(database.getUnreferencedFilesCount()).as("the file no component claims is counted").isEqualTo(1);

    for (int refresh = 0; refresh < 10; refresh++)
      assertThat(database.getUnreferencedFilesCount()).isEqualTo(1);

    verify(fileManager, times(1)).getFiles();
  }

  /** ...and the gate still reaches through the delegation: a file change makes the next refresh walk again. */
  @Test
  void aFileChangeIsSeenThroughTheDelegation() {
    assertThat(database.getUnreferencedFilesCount()).isEqualTo(1);

    when(fileManager.getModificationCount()).thenReturn(2L);

    assertThat(database.getUnreferencedFilesCount()).isEqualTo(1);
    verify(fileManager, times(2)).getFiles();
  }

  /** Same for the other half of the gate, which no file counter could cover. */
  @Test
  void aSchemaChangeIsSeenThroughTheDelegation() {
    assertThat(database.getUnreferencedFilesCount()).isEqualTo(1);

    when(localSchema.getVersion()).thenReturn(8L);

    assertThat(database.getUnreferencedFilesCount()).isEqualTo(1);
    verify(fileManager, times(2)).getFiles();
  }
}
