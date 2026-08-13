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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.parser.Identifier;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A bucket name is used verbatim as the last segment of the component file path, so it must not be able to
 * address anything outside the database directory. Type names reach the same place already percent-encoded by
 * {@link com.arcadedb.utility.FileUtils#encode}, which escapes both separators; a bucket created directly is
 * the only unencoded route and therefore the one that needs the check.
 */
class BucketNameValidationTest extends TestHelper {

  /** Names that address a different directory, either by separator or by being a directory sentinel. */
  private static final List<String> ESCAPING_NAMES = List.of("../escaped", "../../escaped", "sub/nested", "sub\\nested",
      "/absolute", "\\absolute", "..", ".");

  @Test
  void createBucketRejectsNamesThatLeaveTheDatabaseDirectory() {
    final File databaseDirectory = new File(database.getDatabasePath());
    final File parent = databaseDirectory.getParentFile();
    final int filesInParentBefore = parent.listFiles().length;

    for (final String name : ESCAPING_NAMES)
      assertThatThrownBy(() -> database.getSchema().createBucket(name))
          .as("bucket name '%s' must be rejected", name)
          .isInstanceOf(SchemaException.class);

    // Nothing may have been created next to the database directory.
    assertThat(parent.listFiles().length).isEqualTo(filesInParentBefore);
  }

  @Test
  void createBucketViaSqlRejectsNamesThatLeaveTheDatabaseDirectory() {
    final File parent = new File(database.getDatabasePath()).getParentFile();
    final int filesInParentBefore = parent.listFiles().length;

    // Identifier.quote() backtick-quotes and escapes, so the name the engine receives is the one intended here.
    // Building the statement by hand would let the lexer eat a backslash as an escape and change the name.
    final List<String> accepted = new ArrayList<>();
    for (final String name : ESCAPING_NAMES) {
      try {
        database.command("sql", "create bucket " + Identifier.quote(name));
        accepted.add(name);
      } catch (final Exception e) {
        assertThat(e).as("bucket name '%s'", name).hasMessageContaining("Invalid bucket name");
      }
    }
    assertThat(accepted).as("bucket names accepted through SQL that address another directory").isEmpty();

    assertThat(parent.listFiles().length).isEqualTo(filesInParentBefore);
  }

  @Test
  void createBucketRejectsNullAndEmptyNames() {
    assertThatThrownBy(() -> database.getSchema().createBucket(null)).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.getSchema().createBucket("")).isInstanceOf(SchemaException.class);
  }

  /**
   * Dots are legal: the component-file name is parsed right-to-left, peeling the fixed
   * {@code .fileId.pageSize.vVersion.ext} tail, so a dot in the name survives the round trip. Only a name that
   * is exactly "." or ".." is a directory reference. This is what allows a Neo4j label such as {@code my.label}
   * to be imported without renaming it.
   */
  @Test
  void createBucketAcceptsDotsInsideTheName() {
    for (final String name : List.of("my.bucket", "acme.crm.Customer", "a..b", "....", "dash-label", "trailing."))
      assertThatCode(() -> database.getSchema().createBucket(name))
          .as("bucket name '%s' must be accepted", name)
          .doesNotThrowAnyException();

    for (final String name : List.of("my.bucket", "acme.crm.Customer", "a..b", "....", "dash-label", "trailing."))
      assertThat(database.getSchema().existsBucket(name)).as("bucket '%s' must exist", name).isTrue();
  }

  /**
   * A dotted type name must still be creatable, since that is the whole point of allowing dots through. The
   * bucket it derives is {@code <encodedTypeName>_<index>}.
   */
  @Test
  void createTypeWithDotsInNameDerivesADottedBucket() {
    database.getSchema().createVertexType("acme.Customer", 1);

    assertThat(database.getSchema().existsType("acme.Customer")).isTrue();
    assertThat(database.getSchema().getType("acme.Customer").getBuckets(false).getFirst().getName())
        .isEqualTo("acme.Customer_0");
  }

  /**
   * A bucket name is used verbatim as a file name, so any character NTFS/Windows refuses in a file name must be
   * rejected here too: {@code / \} are already covered as path separators, this covers the rest of the reserved
   * set (issue #6104).
   */
  @Test
  void createBucketRejectsWindowsIllegalCharacters() {
    for (final String name : List.of("a<b", "a>b", "a:b", "a\"b", "a|b", "a?b", "a\u0000b", "a\u001fb"))
      assertThatThrownBy(() -> database.getSchema().createBucket(name))
          .as("bucket name '%s' must be rejected", name)
          .isInstanceOf(SchemaException.class);
  }

  /**
   * {@code CON}, {@code PRN}, {@code AUX}, {@code NUL}, {@code COM1-9} and {@code LPT1-9} address a reserved
   * Windows device rather than a regular file, whether or not an extension follows: {@code CON.txt} is refused by
   * the OS exactly like bare {@code CON} is, because it looks only at the segment before the first dot (#6104).
   */
  @Test
  void createBucketRejectsWindowsReservedDeviceNames() {
    for (final String name : List.of("CON", "con", "PRN", "AUX", "NUL", "COM1", "com9", "LPT1", "LPT9", "CON.dat", "nul.backup"))
      assertThatThrownBy(() -> database.getSchema().createBucket(name))
          .as("bucket name '%s' must be rejected", name)
          .isInstanceOf(SchemaException.class);
  }

  /**
   * A reserved stem is only reserved as the exact segment before the first dot: a name that merely starts with
   * one, or that contains one as a later dot-separated segment, addresses an ordinary file and must be accepted.
   */
  @Test
  void createBucketAcceptsNamesThatOnlyResembleReservedDeviceNames() {
    for (final String name : List.of("CONFIG", "console", "NULL", "COM10", "COM0", "LPT10", "a.CON", "Concurrent"))
      assertThatCode(() -> database.getSchema().createBucket(name))
          .as("bucket name '%s' must be accepted", name)
          .doesNotThrowAnyException();
  }
}
