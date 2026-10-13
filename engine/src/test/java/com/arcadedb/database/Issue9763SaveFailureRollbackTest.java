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
package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9763: {@code ALTER DATABASE} undoes its in-memory change when the save fails, because under Raft HA the save
 * is what replicates the setting. It undoes that one setting only, so a change to another setting that landed while
 * the save was in flight survives. A save that fails with {@link TransactionCommittedRemotelyException} is not undone
 * at all: the change IS committed cluster-wide and every node installs it, this one included, so undoing it here
 * would leave this node alone on the previous value.
 */
class Issue9763SaveFailureRollbackTest {
  private static final String DB_PATH = "target/databases/Issue9763SaveFailureRollbackTest";

  private FailingSaveDatabase database;

  /** A database whose configuration save fails the way the HA save can, optionally after a concurrent change. */
  private static final class FailingSaveDatabase extends LocalDatabase {
    private volatile RuntimeException saveFailure;
    private volatile Runnable         duringSave;

    private FailingSaveDatabase(final String path) {
      super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    }

    @Override
    public void saveConfiguration() throws IOException {
      if (duringSave != null)
        duringSave.run();
      if (saveFailure != null)
        throw saveFailure;
      super.saveConfiguration();
    }

    /** What another save made on this node does when it ships, and so commits, this change too. */
    private void saveForReal() {
      try {
        super.saveConfiguration();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    database = new FailingSaveDatabase(DB_PATH);
    database.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aSettingCommittedRemotelyStaysInForce() {
    database.saveFailure = new TransactionCommittedRemotelyException("committed by the majority, not confirmed by every peer");

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(TransactionCommittedRemotelyException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
  }

  /**
   * Possibly committed: the setting settles on what configuration.json holds, which the apply writes if the change
   * commits (issue #9904). Nothing was applied here, so the previous value is back and nothing stays pending for the
   * next change made on this node to carry along. Issue9904UnknownSaveOutcomeTest covers the other orders of events.
   */
  @Test
  void aSettingWhoseOutcomeIsUnknownSettlesOnTheSavedConfiguration() throws Exception {
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 3");
    database.saveFailure = new TransactionOutcomeUnknownException("dispatched to Raft, then the wait gave up");

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  @Test
  void aFailedSaveUndoesItsOwnSettingOnlyAndKeepsAConcurrentChange() {
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 3");
    database.saveFailure = new IllegalStateException("refused: a peer cannot decode the entry yet");
    // Another setting changes while the save is in flight, as a configuration entry replicated by another node would
    database.duringSave = () -> database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(IllegalStateException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("the failed change is undone").isEqualTo(3);
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH))
        .as("the concurrent change to another setting is kept").isEqualTo(2);
  }

  /**
   * Under HA a concurrent change made on this node ships every setting memory holds that the file does not, so it can
   * commit this change before this save fails. The value is then in the file, committed, and must stay.
   */
  @Test
  void aFailedSaveKeepsAValueAnotherSaveAlreadyCommitted() {
    database.saveFailure = new IllegalStateException("refused: a peer cannot decode the entry yet");
    database.duringSave = database::saveForReal;

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(IllegalStateException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
  }

  @Test
  void aFailedSaveOfASettingThatWasNotSetPutsItBackToItsDefault() {
    database.saveFailure = new IllegalStateException("refused: a peer cannot decode the entry yet");

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(IllegalStateException.class);

    assertThat(database.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey());
  }
}
