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
package com.arcadedb.index;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Code review follow-up on PR #5967 (fix for #5905/#5906): {@code Type.convert()} now range-checks before narrowing
 * to BYTE/SHORT/INTEGER instead of silently wrapping, and {@code LSMTreeIndexAbstract.convertKeysToDeclaredTypes()}/
 * {@code LSMTreeIndex.convertKeys()}/{@code HashIndex.convertKeys()} call {@code Type.convert()} directly with no
 * catch guard - unlike {@code QueryOperatorEquals}/{@code FetchFromIndexStep}, which already defend against the new
 * exception. The reviewer asked to confirm that an index rebuild, which re-reads already-stored property values,
 * can never hand one of these paths an out-of-range raw value.
 * <p>
 * It can't: an INTEGER/SHORT/BYTE property's on-disk representation is a fixed-width primitive, so a value that
 * reached storage is by construction already in range for its declared type - there is no way to persist an
 * out-of-range value in the first place (that is exactly the bug #5905 fixed). This test locks that invariant in
 * for both index engines by round-tripping the boundary values through {@code REBUILD INDEX}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5967IndexRebuildBoundaryIntegerValueTest extends TestHelper {

  @Test
  void lsmTreeIndexRebuildSurvivesBoundaryIntegerValues() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.n INTEGER");
      database.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      database.command("sql", "INSERT INTO V SET n = " + Integer.MAX_VALUE);
      database.command("sql", "INSERT INTO V SET n = " + Integer.MIN_VALUE);
      database.command("sql", "INSERT INTO V SET n = 0");
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX `V[n]`"));

    database.transaction(() -> {
      try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MAX_VALUE)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MAX_VALUE);
      }
      try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MIN_VALUE)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MIN_VALUE);
      }
    });
  }

  @Test
  void hashIndexRebuildSurvivesBoundaryIntegerValues() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.n INTEGER");
      database.command("sql", "CREATE INDEX ON V (n) UNIQUE_HASH");
      database.command("sql", "INSERT INTO V SET n = " + Integer.MAX_VALUE);
      database.command("sql", "INSERT INTO V SET n = " + Integer.MIN_VALUE);
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX `V[n]`"));

    database.transaction(() -> {
      try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MAX_VALUE)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MAX_VALUE);
      }
      try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MIN_VALUE)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MIN_VALUE);
      }
    });
  }
}
