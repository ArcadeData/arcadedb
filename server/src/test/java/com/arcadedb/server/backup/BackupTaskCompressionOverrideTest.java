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
package com.arcadedb.server.backup;

import com.arcadedb.database.Database;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6087: a scheduled backup used to run with the server-wide {@code GlobalConfiguration} compression settings no
 * matter what the database asked for, because {@link DatabaseBackupConfig} carried no place to say otherwise. These
 * tests pin the wiring itself - that the values a per-database configuration holds actually reach the {@code Backup}
 * instance {@link BackupTask} builds behind its reflective boundary, and that an unset one leaves the instance alone so
 * the global default still decides.
 */
class BackupTaskCompressionOverrideTest {

  @Test
  void everyConfiguredOverrideReachesTheBackupInstance() throws Exception {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("archivedb");
    config.setCompressionLevel(9);
    config.setCompressionThreads(2);
    config.setMaxMBPerSecond(64);

    final Backup backup = newBackup();
    newTask(config).applyCompressionOverrides(backup);

    assertThat(backup.getSettings().compressionLevel).isEqualTo(9);
    assertThat(backup.getSettings().compressionThreads).isEqualTo(2);
    assertThat(backup.getSettings().maxMBPerSecond).isEqualTo(64);
  }

  /**
   * The half that makes the change backward compatible: a {@code backup.json} that never mentions compression must
   * leave every setting at {@code null}, which is what {@code BackupSettings} reads as "use the global configuration".
   * Push a resolved value here instead and every existing deployment silently freezes today's global default.
   */
  @Test
  void anUnsetOverrideLeavesTheBackupDeferringToTheGlobalConfiguration() throws Exception {
    final Backup backup = newBackup();
    newTask(new DatabaseBackupConfig("legacydb")).applyCompressionOverrides(backup);

    assertThat(backup.getSettings().compressionLevel).isNull();
    assertThat(backup.getSettings().compressionThreads).isNull();
    assertThat(backup.getSettings().maxMBPerSecond).isNull();
  }

  @Test
  void aPartiallyConfiguredDatabaseOverridesOnlyWhatItNamed() throws Exception {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("livedb");
    config.setCompressionLevel(1);

    final Backup backup = newBackup();
    newTask(config).applyCompressionOverrides(backup);

    assertThat(backup.getSettings().compressionLevel).isEqualTo(1);
    assertThat(backup.getSettings().compressionThreads).isNull();
    assertThat(backup.getSettings().maxMBPerSecond).isNull();
  }

  /**
   * Two databases in one fleet, opposite settings: the point of the whole change. Asserting them against each other
   * rules out a wiring that reads the value from somewhere shared rather than from the database's own configuration.
   */
  @Test
  void twoDatabasesInTheSameFleetGetTheirOwnSettings() throws Exception {
    final DatabaseBackupConfig nightly = new DatabaseBackupConfig("archivedb");
    nightly.setCompressionLevel(9);

    final DatabaseBackupConfig live = new DatabaseBackupConfig("livedb");
    live.setCompressionLevel(1);

    final Backup nightlyBackup = newBackup();
    newTask(nightly).applyCompressionOverrides(nightlyBackup);

    final Backup liveBackup = newBackup();
    newTask(live).applyCompressionOverrides(liveBackup);

    assertThat(nightlyBackup.getSettings().compressionLevel).isEqualTo(9);
    assertThat(liveBackup.getSettings().compressionLevel).isEqualTo(1);
  }

  /**
   * {@link DatabaseBackupConfig#validate()} refuses an out-of-range value when the file is read, but a value set
   * programmatically bypasses it. The {@code Backup} setter is still the last line of defence, and it must not be
   * possible to reach it with a value it silently accepts.
   */
  @Test
  void anOutOfRangeOverrideSetProgrammaticallyIsStillRefusedByTheBackup() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("mydb");
    config.setCompressionLevel(42);

    final Backup backup = newBackup();

    assertThatThrownBy(() -> newTask(config).applyCompressionOverrides(backup))
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
  }

  /**
   * The whole chain in one test: the JSON an operator writes into {@code config/backup.json}, through
   * {@link AutoBackupConfig#getEffectiveConfig(String)}, into the {@code Backup} the scheduled task builds. The two
   * databases here are the mixed fleet the issue describes - one tuned for the smallest archive, one tuned to stay out
   * of the live workload's way - and a third that names nothing and keeps following the server default.
   */
  @Test
  void aMixedFleetGetsItsPerDatabaseSettingsFromTheConfigFile() throws Exception {
    final AutoBackupConfig serverConfig = AutoBackupConfig.fromJSON(new JSONObject("""
        {
          "version": 1,
          "enabled": true,
          "backupDirectory": "./backups",
          "defaults": {
            "enabled": true,
            "runOnServer": "$leader"
          },
          "databases": {
            "archivedb": { "compressionLevel": 9, "compressionThreads": 8 },
            "livedb":    { "compressionLevel": 1, "maxMBPerSecond": 32 },
            "plaindb":   { "enabled": true }
          }
        }
        """));

    final Backup archive = newBackup();
    newTask(serverConfig.getEffectiveConfig("archivedb")).applyCompressionOverrides(archive);
    assertThat(archive.getSettings().compressionLevel).isEqualTo(9);
    assertThat(archive.getSettings().compressionThreads).isEqualTo(8);
    assertThat(archive.getSettings().maxMBPerSecond).isNull();

    final Backup live = newBackup();
    newTask(serverConfig.getEffectiveConfig("livedb")).applyCompressionOverrides(live);
    assertThat(live.getSettings().compressionLevel).isEqualTo(1);
    assertThat(live.getSettings().compressionThreads).isNull();
    assertThat(live.getSettings().maxMBPerSecond).isEqualTo(32);

    final Backup plain = newBackup();
    newTask(serverConfig.getEffectiveConfig("plaindb")).applyCompressionOverrides(plain);
    assertThat(plain.getSettings().compressionLevel).isNull();
    assertThat(plain.getSettings().compressionThreads).isNull();
    assertThat(plain.getSettings().maxMBPerSecond).isNull();
  }

  /**
   * A database with no entry of its own falls through {@code getEffectiveConfig}'s "copy the defaults" branch, which
   * builds a fresh {@link DatabaseBackupConfig} field by field - so a setting missing from that copy is dropped in
   * silence for exactly the databases that never asked for special treatment.
   */
  @Test
  void aDatabaseWithNoEntryOfItsOwnInheritsTheServerDefaults() throws Exception {
    final AutoBackupConfig serverConfig = AutoBackupConfig.fromJSON(new JSONObject("""
        {
          "version": 1,
          "enabled": true,
          "defaults": { "compressionLevel": 6, "compressionThreads": 4, "maxMBPerSecond": 16 }
        }
        """));

    final Backup backup = newBackup();
    newTask(serverConfig.getEffectiveConfig("unlisteddb")).applyCompressionOverrides(backup);

    assertThat(backup.getSettings().compressionLevel).isEqualTo(6);
    assertThat(backup.getSettings().compressionThreads).isEqualTo(4);
    assertThat(backup.getSettings().maxMBPerSecond).isEqualTo(16);
  }

  /**
   * A {@code Backup} that is never run: {@code backupDatabase()} is what touches the database and the filesystem, and
   * these tests only care about what the setters recorded.
   */
  private static Backup newBackup() {
    return new Backup((Database) null, "unit-test.zip");
  }

  private static BackupTask newTask(final DatabaseBackupConfig config) {
    return new BackupTask(null, config.getDatabaseName(), config, "target/backup-override-test", null);
  }
}
