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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9763, the engine half: the settings {@link LocalDatabase#pendingConfigurationChanges()} computes and
 * {@link LocalDatabase#applyConfigurationChanges(String)} applies, which is what a Raft node replicates and takes after
 * an {@code ALTER DATABASE}, and the rollback {@code ALTER DATABASE} now performs when the configuration cannot be
 * saved - under HA that save is what replicates it, and a setting left in force on one node
 * only is the divergence the issue reports.
 */
class Issue9763AlterDatabaseConfigurationTest extends TestHelper {

  /** The rollback of a failed save: one setting back, side effect included, every other setting untouched. */
  @Test
  void restoringASettingPutsBackItsValueAndItsPerDatabaseEffect() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
    // In memory only, as changes whose save is under way hold them
    local.getConfiguration().setValue(GlobalConfiguration.DATE_IMPLEMENTATION, "java.util.Date");
    local.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 5);
    local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 6);

    local.restoreSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey(), 4);
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
    assertThat(local.getSerializer().getDateImplementation())
        .as("the per-database effects are re-derived from the configuration in force").isEqualTo(Date.class);

    local.restoreSetting(GlobalConfiguration.DATE_IMPLEMENTATION.getKey(), null);

    assertThat(local.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.DATE_IMPLEMENTATION.getKey());
    assertThat(local.getSerializer().getDateImplementation())
        .isEqualTo(GlobalConfiguration.DATE_IMPLEMENTATION.getDefValue());
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES)).isEqualTo(5);
  }

  /** A save that fails must not leave the setting, or its side effect, in force anyway. */
  @Test
  void aSettingWhoseSaveFailsIsRolledBack() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int bucketsBefore = db.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    final Class<?> dateImplementationBefore = db.getSerializer().getDateImplementation();
    assertThat(dateImplementationBefore).isNotEqualTo(Date.class);

    final File configuration = new File(db.getDatabasePath() + File.separator + LocalDatabase.CONFIGURATION_FILE_NAME);
    // A directory where the file belongs fails the save on every platform and for every user (see Issue7141DiagnosticsTest)
    assertThat(configuration.delete() || !configuration.exists()).isTrue();
    assertThat(configuration.mkdir()).isTrue();
    try {
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
          .isInstanceOf(CommandExecutionException.class);
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
          .isInstanceOf(CommandExecutionException.class);
    } finally {
      assertThat(configuration.delete()).isTrue();
    }

    assertThat(db.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(bucketsBefore);
    assertThat(db.getSerializer().getDateImplementation()).isEqualTo(dateImplementationBefore);
  }

  /** What under HA travels to every node: the settings memory holds that the file does not, and nothing else. */
  @Test
  void pendingChangesNameOnlyWhatDiffersFromTheFile() throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
    assertThat(local.pendingConfigurationChanges()).as("saved, so nothing pending").isNull();

    local.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 9);
    local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, null);

    final JSONObject changes = new JSONObject(local.pendingConfigurationChanges());
    assertThat(changes.getJSONObject("configuration").keySet()).containsExactly("txRetries");
    assertThat(changes.getJSONObject("configuration").getInt("txRetries")).isEqualTo(9);
    assertThat(changes.getJSONArray("removed").toList()).containsExactly("typeDefaultBuckets");
  }

  /**
   * Applying replicated changes touches only the settings they name, in memory and on disk, and writes the file from
   * its own content: a change still on its way through the replication on this node is neither lost nor written ahead.
   */
  @Test
  void applyingChangesKeepsEverySettingTheyDoNotName() throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
    database.command("sql", "ALTER DATABASE `arcadedb.txWalFlush` 2");
    // In memory only, as an ALTER DATABASE whose replication has not committed yet holds it
    local.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 9);

    local.applyConfigurationChanges(
        "{\"configuration\":{\"dateImplementation\":\"java.util.Date\"},\"removed\":[\"txWalFlush\"]}");

    assertThat(local.getConfiguration().getValueAsString(GlobalConfiguration.DATE_IMPLEMENTATION)).isEqualTo("java.util.Date");
    assertThat(local.getSerializer().getDateImplementation()).isEqualTo(Date.class);
    assertThat(local.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TX_WAL_FLUSH.getKey());
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES))
        .as("the local change in flight stays in force").isEqualTo(9);

    final JSONObject file = new JSONObject(FileUtils.readFileAsString(
        new File(local.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME))).getJSONObject("configuration");
    assertThat(file.getString("dateImplementation")).isEqualTo("java.util.Date");
    assertThat(file.has("txWalFlush")).isFalse();
    assertThat(file.getInt("typeDefaultBuckets")).isEqualTo(4);
    assertThat(file.has("txRetries")).as("not written ahead of its own replication").isFalse();
    assertThat(new JSONObject(local.pendingConfigurationChanges()).getJSONObject("configuration").keySet())
        .containsExactly("txRetries");

    local.applyConfigurationChanges("{\"configuration\":{},\"removed\":[\"dateImplementation\"]}");
    assertThat(local.getSerializer().getDateImplementation())
        .isEqualTo(GlobalConfiguration.DATE_IMPLEMENTATION.getDefValue());
  }

  /**
   * A replicated change whose file cannot be written is in force in memory anyway, and the failure is reported: the
   * HA apply turns it into a logged, non-halting entry failure rather than a crash loop on a full or read-only volume.
   */
  @Test
  void aChangeWhoseFileCannotBeWrittenIsInForceAndReported() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final File configuration = new File(local.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    assertThat(configuration.delete() || !configuration.exists()).isTrue();
    assertThat(configuration.mkdir()).isTrue();
    try {
      assertThatThrownBy(() -> local.applyConfigurationChanges("{\"configuration\":{\"txRetries\":8},\"removed\":[]}"))
          .isInstanceOf(IOException.class);
    } finally {
      assertThat(configuration.delete()).isTrue();
    }
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES)).isEqualTo(8);
  }

  /** A configuration.json that is not valid JSON does not block replicated changes: the next one writes a valid file. */
  @Test
  void aCorruptFileCountsAsEmpty() throws Exception {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final File configuration = new File(local.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    FileUtils.writeFile(configuration, "{\"configuration\":{\"txRetries\":");

    local.applyConfigurationChanges("{\"configuration\":{\"txRetries\":8},\"removed\":[]}");

    assertThat(new JSONObject(FileUtils.readFileAsString(configuration)).getJSONObject("configuration").getInt("txRetries"))
        .isEqualTo(8);
  }
}
