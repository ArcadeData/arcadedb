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
   * Migrate database from older versions to current version (legacy method, deprecated).
   * Use migrateOpenDatabase() instead for safer migration on an already-open database.
   *
   * @param databasePath Path to database directory
   */
  @Deprecated
  public static void migrate(final String databasePath) throws IOException {
    LogManager.instance().log(DatabaseMigration.class, Level.WARNING,
        "Using deprecated migrate() method. Consider using migrateOpenDatabase() instead.");

    final File dbDir = new File(databasePath);
    final File[] files = dbDir.listFiles((dir, name) ->
        name.endsWith(".bucket") || name.endsWith(".dict") || name.endsWith(".umtidx"));

    if (files == null) {
      LogManager.instance().log(DatabaseMigration.class, Level.WARNING, "No component files found in: %s", databasePath);
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

    final File backupDir = new File(dbDir, "backup");
    if (!backupDir.exists() && !backupDir.mkdir()) {
      throw new IOException("Failed to create backup directory: " + backupDir);
    }

    final Set<String> renamedBucketNames = new HashSet<>();
    for (final File file : filesToMigrate) {
      final String bucketName = extractBucketNameFromFilename(file.getName());
      renameFileToV1(file, backupDir);
      renamedBucketNames.add(bucketName);
    }

    if (renamedBucketNames.isEmpty()) {
      LogManager.instance().log(DatabaseMigration.class, Level.INFO, "No buckets to migrate");
      return;
    }

    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Starting record data migration for %d buckets...", renamedBucketNames.size());

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    final Database db = factory.open(ComponentFile.MODE.READ_WRITE, false);
    try {
      final RecordMigrationV0ToV1 recordMigration = new RecordMigrationV0ToV1((DatabaseInternal) db, renamedBucketNames);
      recordMigration.migrate();
    } finally {
      db.close();
    }

    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Migration completed. %d files migrated to v%d", renamedBucketNames.size(), CURRENT_VERSION);
  }

  /**
   * Migrate a closed database from v0 to v1 format.
   * Uses in-place migration: renames files to v1, opens database with backward-compatible reading,
   * rewrites all records in v1 format.
   *
   * @param databasePath Path to the database directory
   */
  public static void migrateClosedDatabase(final String databasePath) throws IOException {
    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Starting database migration to v%d for: %s", CURRENT_VERSION, databasePath);

    final File dbDir = new File(databasePath);
    final File[] v0Files = dbDir.listFiles((dir, name) ->
        (name.endsWith(".bucket") || name.endsWith(".dict") || name.endsWith(".umtidx")) &&
            getVersionFromFilename(name) < CURRENT_VERSION);

    if (v0Files == null || v0Files.length == 0) {
      LogManager.instance().log(DatabaseMigration.class, Level.INFO, "No files need migration");
      return;
    }

    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Found %d v0 component files to migrate", v0Files.length);

    // Create backup directory
    final File backupDir = new File(dbDir, "backup");
    if (!backupDir.exists() && !backupDir.mkdir()) {
      throw new IOException("Failed to create backup directory: " + backupDir);
    }

    // Phase 1: Backup and rename files to v1
    final Set<String> renamedBuckets = new HashSet<>();
    for (final File v0File : v0Files) {
      // Backup
      final File backupFile = new File(backupDir, v0File.getName() + ".backup");
      Files.copy(v0File.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      LogManager.instance().log(DatabaseMigration.class, Level.INFO,
          "Backed up: %s", v0File.getName());

      // Rename to v1
      final String v1FileName = updateVersionInFilename(v0File.getName(), CURRENT_VERSION);
      final File v1File = new File(dbDir, v1FileName);
      if (!v0File.renameTo(v1File)) {
        throw new IOException("Failed to rename " + v0File.getName() + " to " + v1FileName);
      }

      LogManager.instance().log(DatabaseMigration.class, Level.INFO,
          "Renamed: %s -> %s", v0File.getName(), v1FileName);

      // Track bucket names for record migration
      if (v0File.getName().endsWith(".bucket")) {
        renamedBuckets.add(extractBucketNameFromFilename(v0File.getName()));
      }
    }

    if (renamedBuckets.isEmpty()) {
      LogManager.instance().log(DatabaseMigration.class, Level.INFO,
          "No bucket files to migrate (only non-bucket files renamed)");
      return;
    }

    // Phase 2: Open database and migrate records in-place
    // The code will read v0 data (backward-compatible) and write v1 data
    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Opening database to migrate %d buckets...", renamedBuckets.size());

    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    Database db = null;
    try {
      // Open without migration check to avoid recursion
      db = factory.open(ComponentFile.MODE.READ_WRITE, false);

      // Migrate records from v0 to v1 format in-place
      final RecordMigrationV0ToV1 recordMigration = new RecordMigrationV0ToV1((DatabaseInternal) db, renamedBuckets);
      recordMigration.migrate();

    } finally {
      if (db != null) {
        db.close();
        // Remove from active instances
        DatabaseFactory.removeActiveDatabaseInstance(databasePath);
      }
    }

    LogManager.instance().log(DatabaseMigration.class, Level.INFO,
        "Migration completed. %d files migrated to v%d", v0Files.length, CURRENT_VERSION);
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

  /**
   * Extract bucket name from component filename.
   * Format: bucketName.id.size[.vVERSION].bucket
   * Example: "Person.0.65536.bucket" → "Person"
   * Example: "edge-segment.0.4096.v1.bucket" → "edge-segment"
   *
   * @param filename Component filename
   * @return Bucket name (first part before first dot)
   */
  private static String extractBucketNameFromFilename(final String filename) {
    final int firstDot = filename.indexOf('.');
    if (firstDot > 0) {
      return filename.substring(0, firstDot);
    }
    return filename; // Fallback
  }
}
