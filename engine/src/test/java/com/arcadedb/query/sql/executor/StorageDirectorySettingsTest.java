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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The two database settings that name a directory the engine creates files in, {@code arcadedb.externalPropertyBucketPath}
 * and {@code arcadedb.pageSnapshotSpillPath}, decide where the server writes on the host. A database may only point them
 * inside its own directory or inside the directory the server-level value of the same setting names, and only a user who
 * holds {@code updateSecurity} may change them. A value that got into a database some other way (a configuration file
 * written before these checks) is checked again where the files are created, so it cannot be used either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StorageDirectorySettingsTest {
  private static final String PATH = "target/databases/StorageDirectorySettingsTest";

  @TempDir
  Path tempDir;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.reset();
    if (database != null && database.isOpen())
      database.drop();
    factory.close();
  }

  @Test
  void storageDirectorySettingsNeedTheSecurityPermission() {
    final Path outside = tempDir.resolve("outside");
    bindUser(false);

    assertRefusedAsSecurity("ALTER DATABASE `arcadedb.externalPropertyBucketPath` '" + outside + "'");
    assertRefusedAsSecurity("ALTER DATABASE `arcadedb.pageSnapshotSpillPath` '" + outside + "'");

    assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH)).isEmpty();
    assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH)).isEmpty();
  }

  @Test
  void aDirectoryOutsideTheDatabaseIsRefusedWhenTheServerNamesNone() {
    final Path outside = tempDir.resolve("outside");
    bindUser(true);

    assertRefusedAsSecurity("ALTER DATABASE `arcadedb.externalPropertyBucketPath` '" + outside + "'");
    assertRefusedAsSecurity("ALTER DATABASE `arcadedb.pageSnapshotSpillPath` '" + outside + "'");

    // the external bucket is then created where it always was, next to the primary one
    database.command("sql", "CREATE DOCUMENT TYPE Leak").close();
    database.command("sql", "CREATE PROPERTY Leak.blob STRING (EXTERNAL true)").close();

    assertThat(outside).doesNotExist();
    assertThat(bucketFilesNamed(new File(database.getDatabasePath()), "Leak_0_ext")).hasSize(1);
  }

  @Test
  void aParentReferenceCannotLeaveTheServerDirectory() throws IOException {
    final Path root = Files.createDirectories(tempDir.resolve("root"));
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.setValue(root.toString());
    bindUser(true);

    final String escaping = root + File.separator + ".." + File.separator + "outside";
    assertRefusedAsSecurity("ALTER DATABASE `arcadedb.externalPropertyBucketPath` '" + escaping + "'");
    assertThat(tempDir.resolve("outside")).doesNotExist();
  }

  @Test
  void aSymbolicLinkCannotLeaveTheServerDirectory() throws IOException {
    final Path root = Files.createDirectories(tempDir.resolve("root"));
    final Path outside = Files.createDirectories(tempDir.resolve("outside"));
    final Path link = root.resolve("link");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (final IOException | UnsupportedOperationException e) {
      assumeTrue(false, "symbolic links are not available on this file system: " + e);
    }
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.setValue(root.toString());
    bindUser(true);

    assertRefusedAsSecurity("ALTER DATABASE `arcadedb.externalPropertyBucketPath` '" + link.resolve("tier") + "'");
    assertThat(outside.resolve("tier")).doesNotExist();
  }

  @Test
  void aSubDirectoryOfTheServerDirectoryIsAccepted() throws IOException {
    final Path root = Files.createDirectories(tempDir.resolve("root"));
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.setValue(root.toString());
    bindUser(true);

    final Path tier = root.resolve("tier");
    database.command("sql", "ALTER DATABASE `arcadedb.externalPropertyBucketPath` '" + tier + "'").close();
    unbindUser();

    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").set("blob", "tiered-payload");
      doc.save();
      saved[0] = doc.getIdentity();
    });

    assertThat(bucketFilesNamed(tier.resolve(database.getName()).toFile(), "Doc_0_ext")).hasSize(1);
    database.transaction(() -> assertThat(saved[0].asDocument().getString("blob")).isEqualTo("tiered-payload"));
  }

  @Test
  void aDirectoryInsideTheDatabaseIsAccepted() {
    bindUser(true);

    final String inside = database.getDatabasePath() + File.separator + "tier";
    database.command("sql", "ALTER DATABASE `arcadedb.externalPropertyBucketPath` '" + inside + "'").close();
    database.command("sql", "CREATE DOCUMENT TYPE Doc").close();
    database.command("sql", "CREATE PROPERTY Doc.blob STRING (EXTERNAL true)").close();

    assertThat(bucketFilesNamed(new File(inside, database.getName()), "Doc_0_ext")).hasSize(1);
  }

  @Test
  void theServerLevelDirectoryIsUsedAsConfigured() throws IOException {
    final Path root = Files.createDirectories(tempDir.resolve("root"));
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.setValue(root.toString());

    database.getSchema().createDocumentType("Doc").createProperty("blob", Type.STRING).setExternal(true);

    assertThat(bucketFilesNamed(root.resolve(database.getName()).toFile(), "Doc_0_ext")).hasSize(1);
  }

  /**
   * A value that never went through ALTER DATABASE, as a configuration file written before the check holds it, is
   * checked again where the bucket file is created.
   */
  @Test
  void aStoredValueOutsideTheServerDirectoryIsNotUsedToCreateBuckets() {
    final Path outside = tempDir.resolve("outside");
    database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH, outside.toString());

    final DocumentType type = database.getSchema().createDocumentType("Leak");
    assertThatThrownBy(() -> type.createProperty("blob", Type.STRING).setExternal(true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.getKey());

    assertThat(outside).doesNotExist();
  }

  @Test
  void aStoredSpillDirectoryOutsideTheServerDirectoryFallsBackToTheDatabase() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final Path outside = tempDir.resolve("outside");
    db.getConfiguration().setValue(GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH, outside.toString());

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
    }
    assertThat(outside).as("the directory must not have been created").doesNotExist();
  }

  @Test
  void aSpillDirectoryUnderTheServerDirectoryIsUsed() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final Path root = Files.createDirectories(tempDir.resolve("root"));
    GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.setValue(root.toString());

    final Path spill = root.resolve("spill");
    bindUser(true);
    database.command("sql", "ALTER DATABASE `arcadedb.pageSnapshotSpillPath` '" + spill + "'").close();
    unbindUser();

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
    }
    assertThat(spill).as("the spill directory is created on demand").isDirectory();
  }

  /** A new bucket gets a file name no other file has, so a file already on disk at that name is foreign: never taken over. */
  @Test
  void aNewBucketNeverTakesOverAFileAlreadyOnDisk() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int nextFileId = db.getFileManager().getFiles().size();
    final int pageSize = db.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
    final Path decoy = Path.of(db.getDatabasePath(), "Planted." + nextFileId + "." + pageSize + ".v0.bucket");
    Files.writeString(decoy, "foreign content", StandardCharsets.UTF_8);

    assertThatThrownBy(() -> database.getSchema().createBucket("Planted")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");

    assertThat(Files.readString(decoy, StandardCharsets.UTF_8)).isEqualTo("foreign content");
    assertThat(database.getSchema().existsBucket("Planted")).isFalse();
  }

  private void assertRefusedAsSecurity(final String sql) {
    assertThatThrownBy(() -> database.command("sql", sql).close()).as(sql).isInstanceOf(SecurityException.class);
  }

  private static File[] bucketFilesNamed(final File directory, final String bucketName) {
    final File[] files = directory.listFiles((dir, name) -> name.startsWith(bucketName + ".") && name.endsWith(".bucket"));
    return files != null ? files : new File[0];
  }

  private void bindUser(final boolean canUpdateSecurity) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "dbtuner";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return access == DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS || access == DATABASE_ACCESS.UPDATE_SCHEMA
            || (canUpdateSecurity && access == DATABASE_ACCESS.UPDATE_SECURITY);
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }

  private void unbindUser() {
    if (database != null && database.isOpen())
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }
}
