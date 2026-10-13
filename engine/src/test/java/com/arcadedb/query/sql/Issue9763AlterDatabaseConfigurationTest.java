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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9763, the engine half: {@link LocalDatabase#installConfiguration(String)}, which is how a Raft follower takes
 * the {@code configuration.json} the leader replicated, and the rollback {@code ALTER DATABASE} now performs when the
 * configuration cannot be saved - under HA that save is what replicates it, and a setting left in force on one node
 * only is the divergence the issue reports.
 */
class Issue9763AlterDatabaseConfigurationTest extends TestHelper {

  @Test
  void installingADocumentAppliesItsSettingsAndTheirPerDatabaseEffects() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
    final String before = local.getConfiguration().toJSON();

    local.installConfiguration(
        "{\"configuration\":{\"dateImplementation\":\"java.util.Date\",\"txWalFlush\":2}}");

    assertThat(local.getConfiguration().getValueAsString(GlobalConfiguration.DATE_IMPLEMENTATION))
        .isEqualTo("java.util.Date");
    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH)).isEqualTo(2);
    assertThat(local.getSerializer().getDateImplementation())
        .as("the serializer follows without a reopen").isEqualTo(Date.class);
    assertThat(local.getConfiguration().getContextKeys())
        .as("a setting the document does not carry goes back to its default, as a reopen would leave it")
        .doesNotContain(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey());

    local.installConfiguration(before);

    assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
    assertThat(local.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.DATE_IMPLEMENTATION.getKey());
    assertThat(local.getSerializer().getDateImplementation())
        .isEqualTo(GlobalConfiguration.DATE_IMPLEMENTATION.getDefValue());
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
}
