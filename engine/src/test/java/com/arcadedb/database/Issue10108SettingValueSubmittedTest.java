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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10108: under Raft HA the entry a configuration change replicates named the setting its statement changed (issue
 * #9959) but carried the value the in-memory configuration held WHEN IT WAS SUBMITTED, which is not necessarily the value that
 * statement set: a second statement on the same setting can store its own value in between. Statement A then shipped B's value,
 * and when B's own save was refused, B's caller was told nothing had changed anywhere in the cluster while A's entry had
 * committed B's value cluster-wide.
 * <p>
 * The change now carries the entry, captured under the lock the value is stored under, so what is submitted is what that
 * statement set whatever the others do. The tests pin the CONTENT of each submitted entry: the submit is modelled by recording
 * the entry and leaving the file alone, since under HA the file is written by the apply of the entry, after the submit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10108SettingValueSubmittedTest {
  private static final String DB_PATH = "target/databases/Issue10108SettingValueSubmittedTest";
  private static final String KEY     = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();

  /** Records what the Raft HA wrapper would submit for a change, at its save. */
  private static final class RecordingDatabase extends LocalDatabase {
    private final List<String> submitted = new ArrayList<>();

    private RecordingDatabase(final String path) {
      super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    }

    @Override
    public void saveConfiguration(final SettingChange change) throws IOException {
      submitted.add(pendingConfigurationChange(change));
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

  private LocalDatabase.SettingChange set(final Object value) {
    return database.changeSetting(KEY, () -> database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, value));
  }

  @Test
  void twoStatementsOnTheSameSettingEachSubmitTheirOwnValue() throws IOException {
    final LocalDatabase.SettingChange a = set(5);
    final LocalDatabase.SettingChange b = set(7);
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).as("memory holds B's value")
        .isEqualTo(7);

    // A reaches its save after B has stored: its entry still carries what A set
    database.saveConfiguration(a);
    database.saveConfiguration(b);

    assertThat(database.submitted).hasSize(2);
    assertThat(new JSONObject(database.submitted.get(0)).getJSONObject("configuration").getInt("typeDefaultBuckets"))
        .as("A's entry carries A's value, not the one B stored meanwhile").isEqualTo(5);
    assertThat(new JSONObject(database.submitted.get(1)).getJSONObject("configuration").getInt("typeDefaultBuckets"))
        .as("B's entry carries B's value").isEqualTo(7);
  }

  @Test
  void theEntryNamesTheSettingAloneWhateverElseIsPending() throws IOException {
    final LocalDatabase.SettingChange a = set(5);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    database.saveConfiguration(a);

    final JSONObject entry = new JSONObject(database.submitted.getFirst());
    assertThat(entry.getJSONObject("configuration").keySet()).containsExactly("typeDefaultBuckets");
    assertThat(entry.getJSONArray("removed").length()).isZero();
  }

  @Test
  void aSettingTheStatementRemovedIsSubmittedAsRemovedWhateverStoredItAfterwards() throws IOException {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);
    database.saveConfiguration();

    final LocalDatabase.SettingChange removal = set(null);
    set(9);

    database.saveConfiguration(removal);

    final JSONObject entry = new JSONObject(database.submitted.getFirst());
    assertThat(entry.getJSONObject("configuration").keySet()).isEmpty();
    assertThat(entry.getJSONArray("removed").toList()).containsExactly("typeDefaultBuckets");
  }

  @Test
  void aValueTheFileAlreadyHoldsSubmitsNothing() throws IOException {
    final LocalDatabase.SettingChange a = set(5);
    database.saveConfiguration();

    database.saveConfiguration(a);

    assertThat(database.submitted.getFirst()).as("the value set was already the stored one").isNull();
  }

  /** The value is compared with what the file holds by its text, so a type that renders differently once stored would resubmit. */
  @Test
  void aBooleanAnIntegerAndAStringAreEachTheSameAsTheFileOnceSaved() throws IOException {
    final LocalDatabase.SettingChange flag = database.changeSetting(GlobalConfiguration.TX_WAL.getKey(),
        () -> database.getConfiguration().setValue(GlobalConfiguration.TX_WAL, false));
    final LocalDatabase.SettingChange number = database.changeSetting(GlobalConfiguration.TX_WAL_FLUSH.getKey(),
        () -> database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2));
    final LocalDatabase.SettingChange text = database.changeSetting("custom", () -> database.getConfiguration().setValue("custom", "x"));

    database.saveConfiguration(flag);
    database.saveConfiguration(number);
    database.saveConfiguration(text);

    assertThat(new JSONObject(database.submitted.get(0)).getJSONObject("configuration")
        .getBoolean(ContextConfiguration.jsonKey(GlobalConfiguration.TX_WAL.getKey()))).isFalse();
    assertThat(new JSONObject(database.submitted.get(1)).getJSONObject("configuration").getInt("txWalFlush")).isEqualTo(2);
    assertThat(new JSONObject(database.submitted.get(2)).getJSONObject("configuration").getString("custom")).isEqualTo("x");

    // Written to the file the way an apply does, then submitted again: nothing is left to ship for any of them
    database.saveConfiguration();
    database.saveConfiguration(flag);
    database.saveConfiguration(number);
    database.saveConfiguration(text);
    assertThat(database.submitted.subList(3, 6)).containsOnlyNulls();
  }

  @Test
  void aChangeMadeWithoutTheApplyLockFallsBackToTheValueMemoryHolds() throws IOException {
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 5);

    database.saveConfiguration(new LocalDatabase.SettingChange(KEY, null, 5));

    assertThat(new JSONObject(database.submitted.getFirst()).getJSONObject("configuration").getInt("typeDefaultBuckets"))
        .isEqualTo(5);
  }

  /** The writer: ALTER DATABASE submits through the change it made, so its entry is the value it set. */
  @Test
  void alterDatabaseSubmitsTheValueItSet() {
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");

    assertThat(database.submitted).hasSize(1);
    assertThat(new JSONObject(database.submitted.getFirst()).getJSONObject("configuration").getInt("typeDefaultBuckets"))
        .isEqualTo(4);
  }
}
