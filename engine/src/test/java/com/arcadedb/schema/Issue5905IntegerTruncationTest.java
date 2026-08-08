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
import com.arcadedb.graph.MutableVertex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5905: storing a numeric value that does not fit an INTEGER/SHORT/BYTE property used to silently truncate
 * it by two's-complement overflow ({@code 3000000000 -> -1294967296}), with no error and no warning. The write must
 * be rejected instead, both through SQL {@code INSERT}/{@code UPDATE} and through the typed record API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5905IntegerTruncationTest {

  @Test
  void insertOutOfRangeIntegerIsRejectedNotTruncated() throws Exception {
    TestHelper.executeInNewDatabase("issue-5905-insert", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n INTEGER");

      assertThatThrownBy(() -> db.command("sql", "INSERT INTO V SET n = 3000000000, id = 'big'"))
          .isInstanceOf(IllegalArgumentException.class);

      // THE ROW MUST NOT HAVE BEEN WRITTEN WITH A CORRUPTED VALUE
      try (final var rs = db.query("sql", "SELECT FROM V WHERE id = 'big'")) {
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void typedApiSetOutOfRangeIntegerIsRejectedNotTruncated() throws Exception {
    TestHelper.executeInNewDatabase("issue-5905-typed-api", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n INTEGER");

      final MutableVertex v = db.newVertex("V");
      assertThatThrownBy(() -> v.set("n", 3_000_000_000L))
          .isInstanceOf(IllegalArgumentException.class);
    });
  }

  @Test
  void updateOutOfRangeShortIsRejected() throws Exception {
    TestHelper.executeInNewDatabase("issue-5905-short", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n SHORT");
      db.command("sql", "INSERT INTO V SET id = 'a'");

      assertThatThrownBy(() -> db.command("sql", "UPDATE V SET n = 40000 WHERE id = 'a'"))
          .isInstanceOf(IllegalArgumentException.class);

      try (final var rs = db.query("sql", "SELECT n FROM V WHERE id = 'a'")) {
        assertThat(rs.next().<Object>getProperty("n")).isNull();
      }
    });
  }

  @Test
  void updateOutOfRangeByteIsRejected() throws Exception {
    TestHelper.executeInNewDatabase("issue-5905-byte", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n BYTE");
      db.command("sql", "INSERT INTO V SET id = 'a'");

      assertThatThrownBy(() -> db.command("sql", "UPDATE V SET n = 200 WHERE id = 'a'"))
          .isInstanceOf(IllegalArgumentException.class);
    });
  }

  @Test
  void inRangeIntegerStillWorks() throws Exception {
    TestHelper.executeInNewDatabase("issue-5905-inrange", db -> {
      db.command("sql", "CREATE VERTEX TYPE V");
      db.command("sql", "CREATE PROPERTY V.n INTEGER");
      db.command("sql", "INSERT INTO V SET n = 2000000000, id = 'ok'");

      try (final var rs = db.query("sql", "SELECT n FROM V WHERE id = 'ok'")) {
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(2_000_000_000);
      }
    });
  }
}
