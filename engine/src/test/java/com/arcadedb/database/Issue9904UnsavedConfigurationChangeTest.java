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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9904: a configuration change whose replicated entry never commits stays in memory on its originator. Under
 * Raft HA the originator resolves the entry's fate; when it is known not to have committed, the in-memory value of
 * exactly the keys the entry named goes back to what {@code configuration.json} holds. These are the engine halves of
 * that: telling a change that reached the file from one that did not, and putting back only what is still unsaved.
 */
class Issue9904UnsavedConfigurationChangeTest {
  private static final String DB_PATH = "target/databases/Issue9904UnsavedConfigurationChangeTest";

  private LocalDatabase database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    database = new LocalDatabase(DB_PATH, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    database.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private int buckets() {
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
  }

  @Test
  void aChangeThatNeverReachedTheFileIsNotSaved() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 13);
    final String changes = database.pendingConfigurationChanges();

    assertThat(database.isConfigurationChangeSaved(changes)).isFalse();

    database.saveConfiguration();
    assertThat(database.isConfigurationChangeSaved(changes)).isTrue();
  }

  @Test
  void anUnsavedChangeGoesBackToTheValueTheFileHolds() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 13);
    database.saveConfiguration();
    final int stored = buckets();

    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 21);
    final String changes = database.pendingConfigurationChanges();

    database.restoreUnsavedConfigurationChanges(changes);

    assertThat(buckets()).isEqualTo(stored).isEqualTo(13);
    assertThat(database.pendingConfigurationChanges()).as("nothing left to ship by the next change").isNull();
  }

  @Test
  void aSettingTheFileDoesNotHoldFallsBackToItsDefault() throws Exception {
    final int defaultValue = buckets();
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 21);
    final String changes = database.pendingConfigurationChanges();

    database.restoreUnsavedConfigurationChanges(changes);

    assertThat(buckets()).isEqualTo(defaultValue);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  @Test
  void aRemovedSettingComesBackFromTheFile() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 13);
    database.saveConfiguration();
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey(), null);
    final String changes = database.pendingConfigurationChanges();
    assertThat(changes).contains("removed");

    database.restoreUnsavedConfigurationChanges(changes);

    assertThat(buckets()).isEqualTo(13);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  @Test
  void aSettingChangedAgainSinceIsLeftAlone() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 21);
    final String changes = database.pendingConfigurationChanges();
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 34);

    database.restoreUnsavedConfigurationChanges(changes);

    assertThat(buckets()).as("a later change of the same setting is somebody's new intent").isEqualTo(34);
  }

  @Test
  void aSettingTheFileNowHoldsIsLeftAlone() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 21);
    final String changes = database.pendingConfigurationChanges();
    database.saveConfiguration();

    database.restoreUnsavedConfigurationChanges(changes);

    assertThat(buckets()).as("the entry committed after all: its value is the cluster's").isEqualTo(21);
  }

  @Test
  void onlyTheNamedSettingsAreRestored() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 21);
    final String changes = database.pendingConfigurationChanges();
    database.getConfiguration().setValue(GlobalConfiguration.ASYNC_WORKER_THREADS, 3);

    database.restoreUnsavedConfigurationChanges(changes);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(3);
  }
}
