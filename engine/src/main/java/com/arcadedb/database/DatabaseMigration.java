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
package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.*;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles database migration between format versions.
 * Uses component file versioning (filename-based, not per-record).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseMigration {
  private static final Pattern VERSION_PATTERN = Pattern.compile("\\.v(\\d+)\\.");

  // Current version of the database format
  public static final int CURRENT_VERSION = 1;

  /**
   * Check if database needs migration by examining component file versions.
   *
   * @param databasePath Path to database directory
   * @return true if any component files need migration
   */
  public static boolean needsMigration(final String databasePath) throws IOException {
    final File dbDir = new File(databasePath);
    if (!dbDir.exists() || !dbDir.isDirectory()) {
      return false;
    }

    final File[] files = dbDir.listFiles((dir, name) -> name.endsWith(".bucket"));
    if (files == null || files.length == 0) {
      return false;
    }

    for (final File file : files) {
      final int version = getVersionFromFilename(file.getName());
      if (version < CURRENT_VERSION) {
        LogManager.instance().log(DatabaseMigration.class, Level.INFO, "Found v%d component file that needs migration: %s", version, file.getName());
        return true;
      }
    }

    return false;
  }

  /**
   * Migrate database from older versions to current version.
   *
   * @param databasePath Path to database directory
   */
  public static void migrate(final String databasePath) throws IOException {
    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Starting database migration to v%d for: %s", CURRENT_VERSION, databasePath);

    final File dbDir = new File(databasePath);
    final File[] files = dbDir.listFiles((dir, name) -> name.endsWith(".bucket"));

    if (files == null) {
      LogManager.instance().log(DatabaseMigration.class, Level.WARNING, "No bucket files found in: %s", databasePath);
      return;
    }

    final List<File> filesToMigrate = new ArrayList<>();
    for (final File file : files) {
      final int version = getVersionFromFilename(file.getName());
      if (version < CURRENT_VERSION) {
        filesToMigrate.add(file);
      }
    }

    if (filesToMigrate.isEmpty()) {
      LogManager.instance().log(DatabaseMigration.class, Level.INFO, "No files need migration");
      return;
    }

    // Create backup directory
    final File backupDir = new File(dbDir, "backup");
    if (!backupDir.exists() && !backupDir.mkdir()) {
      throw new IOException("Failed to create backup directory: " + backupDir);
    }

    // Phase 1: Rename files to mark as v1 (marks migration intent)
    for (final File file : filesToMigrate) {
      renameFileToV1(file, backupDir);
    }

    // Phase 2: Record data migration - convert actual v0 records to v1 format
    LogManager.instance().log(DatabaseMigration.class, Level.INFO, "Starting record data migration...");

    // Open database directly (files are already renamed to v1)
    final LocalDatabase db = new LocalDatabase(databasePath, ComponentFile.MODE.READ_WRITE,
        new com.arcadedb.ContextConfiguration(), null, new java.util.HashMap<>());
    try {
      db.open();

      // Run record-level migration (convert v0 data to v1 format)
      final RecordMigrationV0ToV1 recordMigration = new RecordMigrationV0ToV1(db);
      recordMigration.migrate();

    } finally {
      db.close();
    }

    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Migration completed. %d files migrated to v%d", filesToMigrate.size(), CURRENT_VERSION);
  }

  /**
   * Rename a component file from v0 to v1 format (marks migration intent).
   * Actual record data migration happens in Phase 2 after database open.
   */
  private static void renameFileToV1(final File file, final File backupDir) throws IOException {
    final int currentVersion = getVersionFromFilename(file.getName());

    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Migrating file: %s (v%d → v%d)", file.getName(), currentVersion, CURRENT_VERSION);

    // Backup the original file
    final File backupFile = new File(backupDir, file.getName() + ".v" + currentVersion + ".backup");
    Files.copy(file.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    LogManager.instance().log(DatabaseMigration.class, Level.INFO, "Backed up to: %s", backupFile.getName());

    // Rename to new version (marks intent, actual data migration TODO)
    final String newName = updateVersionInFilename(file.getName(), CURRENT_VERSION);
    final File newFile = new File(file.getParentFile(), newName);

    if (!file.renameTo(newFile)) {
      throw new IOException("Failed to rename " + file.getName() + " to " + newName);
    }

    LogManager.instance().log(DatabaseMigration.class, Level.INFO, "Renamed to: %s (data migration pending)", newName);
  }

  /**
   * Extract version number from component filename.
   * Format: component.id.size.vVERSION.bucket
   *
   * @param filename Component filename
   * @return Version number, or 0 if no version specified (v0)
   */
  public static int getVersionFromFilename(final String filename) {
    final Matcher matcher = VERSION_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }
    return 0; // Assume v0 if no version in filename
  }

  /**
   * Update version number in filename.
   *
   * @param filename Original filename
   * @param newVersion New version number
   * @return Updated filename
   */
  public static String updateVersionInFilename(final String filename, final int newVersion) {
    final Matcher matcher = VERSION_PATTERN.matcher(filename);
    if (matcher.find()) {
      // Replace existing version
      return matcher.replaceFirst(".v" + newVersion + ".");
    } else {
      // Add version before .bucket extension
      return filename.replace(".bucket", ".v" + newVersion + ".bucket");
    }
  }
}
