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

import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7588, the audit pass that followed #7586: {@code LocalDatabase}'s constructor derived the database NAME
 * from the caller-supplied path with {@code path.lastIndexOf(File.separator)} - this JVM's own separator and no
 * other.
 * <p>
 * The path is caller-supplied through the embedded API, a configuration file or an environment variable, and is
 * routinely written with {@code '/'} even on Windows, where {@code File.separator} is {@code '\'}. A Windows user
 * passing {@code "C:/data/mydb"} therefore got the WHOLE PATH as the database name instead of {@code mydb} - the
 * name that then appears in every log line, every error message, the HTTP routes and the security ACL keys. A
 * trailing separator written the other way round was not stripped either, which made the name empty.
 * <p>
 * The tests mix both conventions in one path, as the real Windows defect did, so the gap is exercised on whichever
 * platform the suite runs on rather than only on Windows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7588DatabaseNameFromMixedSeparatorPathTest {

  /**
   * The defect end to end: the database is really created, and it knows its own name.
   * <p>
   * Only the LAST separator is written the other way round, so the directory this creates is the same directory on
   * either platform and the test does not depend on how the OS reads the rest of the path.
   */
  @Test
  void aDatabaseNamesItselfAfterTheLastPathSegmentWhicheverSeparatorPrecedesIt() {
    final String path = "target/databases/issue7588" + otherSeparator() + "mydb";

    FileUtils.deleteRecursively(new File(path));
    final DatabaseFactory factory = new DatabaseFactory(path);
    try (final Database database = factory.create()) {
      assertThat(database.getName())
          .as("the last segment, not the whole path")
          .isEqualTo("mydb");
      assertThat(database.getDatabasePath()).isEqualTo(path);
    } finally {
      final DatabaseFactory reopen = new DatabaseFactory(path);
      if (reopen.exists())
        reopen.open().drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * A trailing separator written in the OTHER convention is stripped, so the path the name is read off does not end
   * in one. With the {@code File.separator}-only check the separator stayed, and the name came out EMPTY.
   * <p>
   * Asserted on the two helpers rather than by creating a database: a trailing {@code '\'} on a POSIX file system is
   * part of the directory NAME, so a database created at such a path and one created at the stripped path are two
   * different directories, and only Windows can tell the end-to-end story.
   */
  @Test
  void aTrailingSeparatorOfEitherConventionIsStrippedBeforeTheNameIsRead() {
    assertThat(FileUtils.getFileNameFromPath(FileUtils.stripTrailingSeparator("/data/mydb/")))
        .isEqualTo("mydb");
    assertThat(FileUtils.getFileNameFromPath(FileUtils.stripTrailingSeparator("C:\\data\\mydb\\")))
        .isEqualTo("mydb");
    assertThat(FileUtils.getFileNameFromPath(FileUtils.stripTrailingSeparator("C:/data/mydb")))
        .isEqualTo("mydb");
  }

  /**
   * A path with no separator at all is the name: an embedded caller that opens {@code "mydb"} relative to the
   * working directory has always got {@code mydb}, and still does.
   */
  @Test
  void aPathWithNoSeparatorIsTheNameItself() {
    final String path = "target-issue7588-bare";

    FileUtils.deleteRecursively(new File(path));
    final DatabaseFactory factory = new DatabaseFactory(path);
    try (final Database database = factory.create()) {
      assertThat(database.getName()).isEqualTo(path);
    } finally {
      final DatabaseFactory reopen = new DatabaseFactory(path);
      if (reopen.exists())
        reopen.open().drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /** The separator convention this JVM does NOT use, which is the one the lookup used to miss. */
  private static String otherSeparator() {
    return "/".equals(File.separator) ? "\\" : "/";
  }
}
