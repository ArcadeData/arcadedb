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
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9904: under Raft HA the save of {@code ALTER DATABASE} replicates the change, and the replication can give up
 * waiting on an entry it already dispatched ({@link TransactionOutcomeUnknownException}): the change may still commit,
 * and this node applies it like every other node, writing its own {@code configuration.json}. Putting the previous
 * value back then raced that apply: run after it, it left this node alone on the old value until its next restart.
 * <p>
 * The setting now settles on what this node's {@code configuration.json} holds - which the apply writes - so each
 * order of events ends where the cluster does. The apply is reproduced here with the same two calls the HA layer
 * makes: {@link LocalDatabase#pendingConfigurationChanges()} on the submitting side and
 * {@link LocalDatabase#applyConfigurationChanges(String)} on every node.
 */
class Issue9904UnknownSaveOutcomeTest {
  private static final String DB_PATH = "target/databases/Issue9904UnknownSaveOutcomeTest";

  private UnknownOutcomeDatabase database;

  /**
   * A database whose configuration save behaves like the HA save that dispatched its entry and then stopped waiting:
   * the changes are captured as the HA layer would submit them, optionally applied, and the save reports an unknown
   * outcome.
   */
  private static final class UnknownOutcomeDatabase extends LocalDatabase {
    private volatile boolean                 unknownOutcome;
    private volatile boolean                 appliedBeforeTheWaitGivesUp;
    private final    AtomicReference<String> submitted = new AtomicReference<>();

    private UnknownOutcomeDatabase(final String path) {
      super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    }

    @Override
    public void saveConfiguration() throws IOException {
      if (!unknownOutcome) {
        super.saveConfiguration();
        return;
      }
      final String changes = pendingConfigurationChanges();
      submitted.set(changes);
      if (appliedBeforeTheWaitGivesUp)
        applyConfigurationChanges(changes);
      throw new TransactionOutcomeUnknownException("the entry was dispatched, then the wait for its quorum gave up");
    }
  }

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    database = new UnknownOutcomeDatabase(DB_PATH);
    database.create();
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 3");
    database.unknownOutcome = true;
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /** The case the issue names: the apply ran before the caller handled the failure. The old code undid it. */
  @Test
  void aChangeAppliedBeforeTheWaitGaveUpStaysInForce() throws IOException {
    database.appliedBeforeTheWaitGivesUp = true;

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("the node keeps what it applied, as every other node does").isEqualTo(13);
    assertThat(savedSettings().getInt("typeDefaultBuckets")).isEqualTo(13);
    assertThat(database.pendingConfigurationChanges()).as("memory and file agree").isNull();
  }

  /** The per-database side effect follows the setting, not only the configuration map. */
  @Test
  void aDateImplementationAppliedBeforeTheWaitGaveUpStaysInForceOnTheSerializer() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.day DATE");
    database.transaction(() -> database.newDocument("D").set("day", LocalDate.of(2026, 10, 13)).save());
    assertThat(database.getSerializer().getDateImplementation()).as("precondition").isNotEqualTo(Date.class);
    database.appliedBeforeTheWaitGivesUp = true;

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    assertThat(database.getSerializer().getDateImplementation()).isEqualTo(Date.class);
    assertThat(database.query("sql", "SELECT day FROM D").next().<Object>getProperty("day")).isInstanceOf(Date.class);
  }

  /** Not applied yet: the previous value is back now, and the apply installs the new one when the entry commits. */
  @Test
  void aChangeCommittedAfterTheWaitGaveUpIsInstalledByItsApply() throws IOException {
    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("nothing installed yet").isEqualTo(3);

    // The entry commits afterwards: this node applies it like every other node
    database.applyConfigurationChanges(database.submitted.get());

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(13);
    assertThat(savedSettings().getInt("typeDefaultBuckets")).isEqualTo(13);
  }

  /**
   * Never committed: the node is back on the value every other node holds, and holds nothing its file does not, so the
   * next change elsewhere does not carry this one along.
   */
  @Test
  void aChangeThatNeverCommitsLeavesNothingBehind() throws IOException {
    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /** A setting the file has never held falls back to its default, as an undo to "not set" does. */
  @Test
  void aSettingTheFileDoesNotNameFallsBackToItsDefault() {
    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.txRetries` 7"))
        .isInstanceOf(TransactionOutcomeUnknownException.class);

    assertThat(database.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TX_RETRIES.getKey());
  }

  /**
   * A setting no {@link GlobalConfiguration} declares ({@code set database setting} stores one as given) is settled on
   * the file too. Reading it back through {@link ContextConfiguration#fromJSON}, which skips undeclared keys, reset it
   * although the file holds it, and the next change on this node then shipped it as a removal.
   */
  @Test
  void anUndeclaredSettingAppliedBeforeTheWaitGaveUpStaysInForce() throws IOException {
    final String key = GlobalConfiguration.PREFIX + "issue9904Custom";
    database.appliedBeforeTheWaitGivesUp = true;
    database.getConfiguration().setValue(key, "v1");

    assertThatThrownBy(database::saveConfiguration).isInstanceOf(TransactionOutcomeUnknownException.class);
    database.restoreSettingFromSavedConfiguration(key);

    final Object inForce = database.getConfiguration().getValue(key, null);
    assertThat(inForce).isEqualTo("v1");
    assertThat(savedSettings().getString("issue9904Custom")).isEqualTo("v1");
    assertThat(database.pendingConfigurationChanges()).as("memory and file agree, so nothing ships as a removal").isNull();
  }

  private JSONObject savedSettings() throws IOException {
    return new JSONObject(FileUtils.readFileAsString(new File(DB_PATH, LocalDatabase.CONFIGURATION_FILE_NAME)))
        .getJSONObject("configuration");
  }
}
