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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9959: under Raft HA the entry a configuration change replicates was built from the WHOLE difference between
 * the in-memory configuration and {@code configuration.json}. A value another statement on the same node had put in
 * force and not submitted yet was therefore shipped with this statement's entry, and committed cluster-wide even when
 * that other statement's own save was then refused and its caller told nothing had changed.
 * <p>
 * These tests pin the CONTENT of the entry, not the final value of a setting: the entry a statement submits names the
 * setting it changed and nothing else. The other statement is the in-memory store it makes before its save, done here
 * directly on the configuration.
 */
class Issue9959PerSettingConfigurationEntryTest {
  private static final String DB_PATH = "target/databases/Issue9959PerSettingConfigurationEntryTest";

  /** What the Raft HA wrapper would submit for the setting saved, captured at the save. */
  private static final class RecordingDatabase extends LocalDatabase {
    private volatile String savedKey;
    private volatile String submittedEntry;

    private RecordingDatabase(final String path) {
      super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    }

    @Override
    public void saveConfiguration(final SettingChange change) throws IOException {
      savedKey = change.key();
      // Before the save, as the HA wrapper computes it: the save writes the file, after which nothing is pending
      submittedEntry = pendingConfigurationChange(change);
      super.saveConfiguration(change);
    }
  }

  private RecordingDatabase database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    database = new RecordingDatabase(DB_PATH);
    database.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theChangeToOneSettingLeavesOutAnotherPendingOne() throws IOException {
    // Another statement's setting, in force and not submitted yet
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);

    final JSONObject entry = new JSONObject(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()));
    assertThat(entry.getJSONObject("configuration").keySet()).containsExactly("typeDefaultBuckets");
    assertThat(entry.getJSONObject("configuration").getInt("typeDefaultBuckets")).isEqualTo(5);
    assertThat(entry.getJSONArray("removed").length()).isZero();

    // The whole difference, which the entry used to be, holds both
    assertThat(new JSONObject(database.pendingConfigurationChanges()).getJSONObject("configuration").keySet())
        .containsExactlyInAnyOrder("typeDefaultBuckets", "txWalFlush");
  }

  @Test
  void aDeclaredSettingIsFoundHoweverTheKeyIsSpelled() throws IOException {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);

    for (final String spelling : new String[] { "typeDefaultBuckets", "ARCADEDB.TYPEDEFAULTBUCKETS" })
      assertThat(new JSONObject(database.pendingConfigurationChange(spelling)).getJSONObject("configuration").keySet())
          .as(spelling).containsExactly("typeDefaultBuckets");
  }

  @Test
  void aSettingFileAndMemoryAgreeOnHasNoChange() throws IOException {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);
    database.saveConfiguration();
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    assertThat(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()))
        .as("only another setting is pending, and it is not this one's to ship").isNull();
  }

  @Test
  void aRemovedSettingIsTheOnlyOneNamed() throws IOException {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 9);
    database.saveConfiguration();

    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, null);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, null);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    final JSONObject entry = new JSONObject(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()));
    assertThat(entry.getJSONObject("configuration").keySet()).isEmpty();
    assertThat(entry.getJSONArray("removed").toList()).containsExactly("typeDefaultBuckets");
  }

  @Test
  void theFileNameOfASettingKeepsAKeyWithoutThePrefixWhole() {
    assertThat(ContextConfiguration.jsonKey("arcadedb.typeDefaultBuckets")).isEqualTo("typeDefaultBuckets");
    assertThat(ContextConfiguration.jsonKey("custom")).isEqualTo("custom");
  }

  /** The writer: ALTER DATABASE saves its own setting by key, so the entry it submits names that setting alone. */
  @Test
  void alterDatabaseSubmitsItsOwnSettingOnly() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");

    assertThat(database.savedKey).isEqualTo(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey());
    assertThat(database.submittedEntry).isNotNull();
    final JSONObject entry = new JSONObject(database.submittedEntry);
    assertThat(entry.getJSONObject("configuration").keySet()).containsExactly("typeDefaultBuckets");
    assertThat(entry.getJSONObject("configuration").getInt("typeDefaultBuckets")).isEqualTo(4);
    assertThat(entry.getJSONArray("removed").length()).isZero();
  }
}
