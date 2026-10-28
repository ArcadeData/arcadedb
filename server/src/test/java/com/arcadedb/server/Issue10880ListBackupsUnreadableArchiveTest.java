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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Issue #10880: {@code list backups} published an archive it could not stat as {@code "size":0,"lastModified":0}, a zero-byte
 * backup dated 1970, and logged nothing. The common cause is the retention sweep deleting the archive under the listing, which a
 * dangling symbolic link reproduces deterministically: the name is listed, the stat fails with {@code NoSuchFileException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10880ListBackupsUnreadableArchiveTest extends BaseGraphServerTest {
  private Path backupDir;

  @BeforeEach
  void prepareBackupDirectory() throws IOException {
    backupDir = new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY),
        getDatabaseName()).toPath();
    FileUtils.deleteRecursively(backupDir.toFile());
    Files.createDirectories(backupDir);
  }

  @AfterEach
  void removeBackupDirectory() throws IOException {
    if (backupDir != null) {
      // the backup root is shared with every other backup test of the module: give both directories back their access
      // before deleting, or what is left behind fails the next test
      if (backupDir.getFileSystem().supportedFileAttributeViews().contains("posix")) {
        for (final Path dir : new Path[] { backupDir.getParent(), backupDir })
          if (Files.exists(dir, LinkOption.NOFOLLOW_LINKS))
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      FileUtils.deleteRecursively(backupDir.toFile());
      assertThat(backupDir).as("the test leaves nothing behind").doesNotExist();
    }
  }

  @Test
  void onlyRegularFilesAreListedWithTheNumbersReadFromThem() throws IOException {
    final Path real = writeArchive("120000");

    // A directory with an archive's name is not an archive anybody can restore or delete
    Files.createDirectories(backupDir.resolve(getDatabaseName() + "-backup-20261028-140000.zip"));

    final JSONObject listing = listBackups();
    assertOnlyListed(listing, real);
  }

  @Test
  void anArchiveRemovedUnderTheListingIsNotListedWithMadeUpNumbers() throws IOException {
    final Path real = writeArchive("120000");

    // What an archive removed by the retention sweep under the listing looks like to the stat: listed, then gone
    final Path removed = backupDir.resolve(getDatabaseName() + "-backup-20261028-130000.zip");
    boolean linked;
    try {
      Files.createSymbolicLink(removed, backupDir.resolve("does-not-exist.zip"));
      linked = true;
    } catch (final UnsupportedOperationException | IOException | SecurityException e) {
      // Windows without the symbolic link privilege: the case cannot be built here
      linked = false;
    }
    assumeThat(linked).as("symbolic links can be created").isTrue();

    try {
      assertOnlyListed(listBackups(), real);
    } finally {
      // a recursive delete that asks File.exists() never sees a dangling link
      Files.deleteIfExists(removed);
    }
  }

  @Test
  void aDatabaseWithNoBackupDirectoryHasNoBackups() {
    FileUtils.deleteRecursively(backupDir.toFile());

    final JSONObject listing = listBackups();
    assertThat(listing.getJSONArray("backups").length()).isZero();
    assertThat(listing.getLong("totalCount")).isZero();
    assertThat(listing.getLong("totalSize")).isZero();
  }

  @Test
  void aDirectoryThatCannotBeListedIsATypedCommandFailure() throws IOException {
    writeArchive("120000");
    assumePosix();
    Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("---------"));
    // A superuser lists it anyway: nothing to test there
    assumeThat(Files.isReadable(backupDir)).isFalse();

    assertListingFails();
  }

  /** Files.isDirectory() answers false for a directory the server cannot reach, which used to read as "no backups". */
  @Test
  void aDirectoryThatCannotBeReachedIsATypedCommandFailureNotAnEmptyList() throws IOException {
    writeArchive("120000");
    assumePosix();
    Files.setPosixFilePermissions(backupDir.getParent(), PosixFilePermissions.fromString("---------"));
    assumeThat(Files.isDirectory(backupDir)).as("the directory is out of reach").isFalse();

    assertListingFails();
  }

  private Path writeArchive(final String time) throws IOException {
    final Path archive = backupDir.resolve(getDatabaseName() + "-backup-20261028-" + time + ".zip");
    Files.write(archive, new byte[4096]);
    return archive;
  }

  private JSONObject listBackups() {
    return new ServerControlPlane(getServer(0)).listBackups(getDatabaseName());
  }

  private void assertOnlyListed(final JSONObject listing, final Path archive) throws IOException {
    final JSONArray backups = listing.getJSONArray("backups");
    assertThat(backups.length()).as("only the archive whose attributes were read is listed").isEqualTo(1);
    final JSONObject backup = backups.getJSONObject(0);
    assertThat(backup.getString("fileName")).isEqualTo(archive.getFileName().toString());
    assertThat(backup.getLong("size")).isEqualTo(4096L);
    assertThat(backup.getLong("lastModified")).isEqualTo(Files.getLastModifiedTime(archive).toMillis());
    assertThat(backup.getString("timestamp")).isEqualTo("2026-10-28T12:00");

    assertThat(listing.getLong("totalCount")).isEqualTo(1L);
    assertThat(listing.getLong("totalSize")).isEqualTo(4096L);
  }

  private void assertListingFails() {
    assertThatThrownBy(this::listBackups)
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Error listing backups for database '" + getDatabaseName() + "'");
  }

  private void assumePosix() {
    assumeThat(backupDir.getFileSystem().supportedFileAttributeViews()).contains("posix");
  }
}
