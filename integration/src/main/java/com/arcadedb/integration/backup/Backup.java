/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.integration.backup;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.backup.format.AbstractBackupFormat;
import com.arcadedb.integration.backup.format.FullBackupFormat;
import com.arcadedb.integration.importer.ConsoleLogger;

import java.util.Locale;
import java.util.Timer;

public class Backup {
  protected BackupSettings       settings           = new BackupSettings();
  protected DatabaseInternal     database;
  protected Timer                timer;
  protected ConsoleLogger        logger;
  protected AbstractBackupFormat formatImplementation;
  protected boolean              closeDatabaseAtEnd = false;

  public Backup(final String[] args) {
    settings.parseParameters(args);
  }

  public Backup(final Database database) {
    this.database = (DatabaseInternal) database;
  }

  public Backup(final Database database, final String file) {
    this.database = (DatabaseInternal) database;
    settings.file = file;
  }

  public static void main(final String[] args) {
    new Backup(args).backupDatabase();
    System.exit(0);
  }

  public String backupDatabase() {
    try {
      if (logger == null)
        logger = new ConsoleLogger(settings.verboseLevel);

      openDatabase();

      settings.databaseName = database.getName();
      settings.validateSettings();

      if (logger.getVerboseLevel() != settings.verboseLevel)
        logger = new ConsoleLogger(settings.verboseLevel);

      formatImplementation = createFormatImplementation();
      formatImplementation.backupDatabase();

      return settings.file;

    } catch (final Exception e) {
      throw new BackupException(
          "Error during backup of database '" + (database != null ? database.getName() : settings.databaseURL) + "' to file '"
              + settings.file + "'", e);
    } finally {
      closeDatabase();
    }
  }

  public Backup setEncryptionAlgorithm(final String algorithm) {
    settings.encryptionAlgorithm = algorithm;
    return this;
  }

  public Backup setEncryptionKey(final String key) {
    settings.encryptionKey = key;
    return this;
  }

  public Backup setDirectory(final String directory) {
    settings.directory = directory;
    return this;
  }

  public Backup setVerboseLevel(final int verboseLevel) {
    settings.verboseLevel = verboseLevel;
    return this;
  }

  /**
   * Deflate level, 0 (store) to 9 (smallest). Overrides {@link com.arcadedb.GlobalConfiguration#BACKUP_COMPRESSION_LEVEL}.
   */
  public Backup setCompressionLevel(final int compressionLevel) {
    settings.compressionLevel = BackupSettings.checkIntSetting("compressionLevel", compressionLevel, 0, 9);
    return this;
  }

  /**
   * Compression threads: -1 automatic, 0 the legacy single-threaded writer, N a pool of N. Overrides
   * {@link com.arcadedb.GlobalConfiguration#BACKUP_COMPRESSION_THREADS}.
   */
  public Backup setCompressionThreads(final int compressionThreads) {
    settings.compressionThreads = BackupSettings.checkIntSetting("compressionThreads", compressionThreads, -1,
        BackupSettings.MAX_COMPRESSION_THREADS);
    return this;
  }

  /**
   * Read-side rate cap in MB/s, 0 for unlimited. Overrides
   * {@link com.arcadedb.GlobalConfiguration#BACKUP_MAX_MB_PER_SECOND}.
   */
  public Backup setMaxMBPerSecond(final int maxMBPerSecond) {
    settings.maxMBPerSecond = BackupSettings.checkIntSetting("maxMBPerSecond", maxMBPerSecond, 0, Integer.MAX_VALUE);
    return this;
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      return;

    final DatabaseFactory factory = new DatabaseFactory(settings.databaseURL);

    if (!factory.exists())
      throw new BackupException("Database '%s' not found".formatted(settings.databaseURL));

    logger.logLine(0, "Opening database '%s'...", settings.databaseURL);
    database = (DatabaseInternal) factory.open();
    closeDatabaseAtEnd = true;
  }

  protected void closeDatabase() {
    if (database != null && closeDatabaseAtEnd) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }

  protected AbstractBackupFormat createFormatImplementation() {
    switch (settings.format.toLowerCase(Locale.ENGLISH)) {
    case "full":
      return new FullBackupFormat(database, settings, logger);

    default:
      throw new BackupException("Format '" + settings.format + "' not supported");
    }
  }
}
