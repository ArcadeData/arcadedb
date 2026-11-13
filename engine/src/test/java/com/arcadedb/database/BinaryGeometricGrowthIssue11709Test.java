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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11709: Binary grew its buffer to the next multiple of its allocation chunk (512 bytes by default), so
 * writing N bytes in small pieces reallocated and copied the whole buffer about N / 512 times, and reading or writing a large
 * record cost about the square of its size.
 */
class BinaryGeometricGrowthIssue11709Test extends TestHelper {

  /** Reallocations while writing 4 MB in 100-byte pieces: logarithmic in the size, not 4 MB / 512. */
  @Test
  void writingInSmallPiecesReallocatesLogarithmically() {
    final Binary binary = new Binary();
    final byte[] piece = new byte[100];
    Arrays.fill(piece, (byte) 7);
    byte[] content = binary.getContent();
    int reallocations = 0;
    for (int i = 0; i < 4 * 1024 * 1024 / piece.length; i++) {
      binary.putByteArray(piece);
      if (binary.getContent() != content) {
        content = binary.getContent();
        ++reallocations;
      }
    }
    assertThat(reallocations).isLessThan(64);
    assertThat(binary.size()).isGreaterThanOrEqualTo(4 * 1024 * 1024 / piece.length * piece.length);
  }

  /** The same with append, as a multi-page record is assembled one page chunk at a time. */
  @Test
  void appendingChunksReallocatesLogarithmically() {
    final Binary chunk = new Binary(new byte[60_000]);
    final Binary record = new Binary();
    byte[] content = record.getContent();
    int reallocations = 0;
    for (int i = 0; i < 200; i++) {
      record.append(chunk);
      if (record.getContent() != content) {
        content = record.getContent();
        ++reallocations;
      }
    }
    assertThat(reallocations).isLessThan(32);
    assertThat(record.size()).isEqualTo(200 * 60_000);
  }

  /** A size the caller asked for in advance is still what the first growth gives it. */
  @Test
  void aPresizedChunkIsKept() {
    final Binary binary = new Binary(16);
    binary.setAllocationChunkSize(100_000);
    binary.putByteArray(new byte[1_000]);
    assertThat(binary.capacity()).isGreaterThanOrEqualTo(100_000);
  }

  /** Large records keep their content through a write and a read. */
  @Test
  void largeRecordsRoundTrip() {
    final String big = "x".repeat(3 * 1024 * 1024) + "end";
    final List<String> list = new ArrayList<>();
    for (int i = 0; i < 50_000; i++)
      list.add("tok" + i);
    database.transaction(() -> {
      database.getSchema().createDocumentType("D");
      database.newDocument("D").set("k", 1, "big", big, "list", list).save();
    });
    database.transaction(() -> database.command("sql", "UPDATE D SET small = 1 WHERE k = 1"));
    try (final ResultSet rs = database.query("sql", "SELECT big, list, small FROM D WHERE k = 1")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("big")).isEqualTo(big);
      assertThat(row.<List<String>>getProperty("list")).containsExactlyElementsOf(list);
      assertThat(row.<Integer>getProperty("small")).isEqualTo(1);
    }
  }
}
