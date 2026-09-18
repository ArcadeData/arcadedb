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

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers what {@code UnwindStep} considers a sequence. Since #7910 that question is answered by
 * {@code MultiValue.isSequenceArray()}, which {@code ExpandStep} shares, so the two steps cannot drift apart on it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UnwindStepTest extends TestHelper {

  @Test
  void unwindOfANativeArrayProducesOneRowPerElement() {
    final DocumentType type = database.getSchema().createDocumentType("ArrayUnwind");
    type.createProperty("arrf", Type.ARRAY_OF_FLOATS);

    database.transaction(() -> database.newDocument("ArrayUnwind").set("arrf", new float[] { 1, 2, 3 }).save());

    final List<Object> values = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT arrf FROM ArrayUnwind UNWIND arrf")) {
      while (result.hasNext())
        values.add(result.next().getProperty("arrf"));
    }

    assertThat(values).containsExactly(1F, 2F, 3F);
  }

  /**
   * Regression test for the sibling half of #7910: a {@code BINARY} property is a {@code byte[]}, i.e. an opaque blob
   * rather than a sequence, and {@code UNWIND} used to route every array through
   * {@code MultiValue.getMultiValueIterator()} - turning a megabyte into a million rows, and disagreeing with the
   * single row {@code expand()} answers for the very same property.
   */
  @Test
  void unwindOfABinaryPropertyProducesOneRowHoldingTheWholeBlob() {
    final DocumentType type = database.getSchema().createDocumentType("BinaryUnwind");
    type.createProperty("blob", Type.BINARY);

    database.transaction(() -> database.newDocument("BinaryUnwind").set("blob", new byte[] { 1, 2, 3 }).save());

    try (final ResultSet result = database.query("sql", "SELECT blob FROM BinaryUnwind UNWIND blob")) {
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<byte[]>getProperty("blob")).containsExactly(1, 2, 3);
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void unwindOfABinaryPropertyAgreesWithExpand() {
    final DocumentType type = database.getSchema().createDocumentType("BinaryBoth");
    type.createProperty("blob", Type.BINARY);

    database.transaction(() -> database.newDocument("BinaryBoth").set("blob", new byte[] { 9, 8 }).save());

    int unwound = 0;
    try (final ResultSet result = database.query("sql", "SELECT blob FROM BinaryBoth UNWIND blob")) {
      while (result.hasNext()) {
        result.next();
        unwound++;
      }
    }

    int expanded = 0;
    try (final ResultSet result = database.query("sql", "SELECT expand(blob) FROM BinaryBoth")) {
      while (result.hasNext()) {
        result.next();
        expanded++;
      }
    }

    assertThat(unwound).isEqualTo(expanded);
    assertThat(unwound).isEqualTo(1);
  }
}
