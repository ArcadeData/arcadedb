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
package com.arcadedb;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Code review follow-up on <a href="https://github.com/ArcadeData/arcadedb/issues/6061">issue #6061</a>:
 * {@link Type#convert} had narrowing branches from {@link java.util.Collection} to {@code float[]},
 * {@code double[]}, {@code int[]}, {@code long[]} and {@code short[]}, but not to {@code byte[]}. A
 * JSON array received over the wire (e.g. through {@code RemoteGraphBatch}, or any JSON-based write
 * path) for a BINARY property therefore parsed into a {@code List<Number>} that was silently stored
 * as an untyped {@link List} instead of being converted to {@code byte[]}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6061BinaryPropertyListConversionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Blob");
      database.command("sql", "CREATE PROPERTY Blob.data BINARY");
    });
  }

  @Test
  void setListOnBinaryPropertyConvertsToByteArray() {
    database.transaction(() -> {
      final MutableDocument doc = database.newVertex("Blob");
      doc.set("data", List.of(1, 2, 3, 4));

      final Object data = doc.get("data");
      assertThat(data).isInstanceOf(byte[].class);
      assertThat((byte[]) data).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4);
    });
  }

  @Test
  void typeConvertListToByteArray() {
    final Object converted = Type.convert(database, List.of(1, 2, 3, 4), byte[].class);
    assertThat(converted).isInstanceOf(byte[].class);
    assertThat((byte[]) converted).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4);
  }

  @Test
  void typeConvertEmptyListToEmptyByteArray() {
    final Object converted = Type.convert(database, List.of(), byte[].class);
    assertThat(converted).isInstanceOf(byte[].class);
    assertThat((byte[]) converted).isEmpty();
  }

  // Code review follow-up: a null element in the source List/Collection previously NPE'd deep
  // inside narrowToIntegral()/the float/double narrowing branches instead of raising a clean
  // validation error. Not specific to byte[] - the same null guard now covers every
  // Collection -> primitive-array narrowing branch in Type.convert() (byte[]/short[]/int[]/long[]
  // via narrowToIntegral(), float[]/double[] via the new requireNonNullNumber() helper).
  @Test
  void typeConvertListWithNullElementToByteArrayThrowsCleanly() {
    final List<Integer> withNull = Arrays.asList(1, null, 3);
    assertThatThrownBy(() -> Type.convert(database, withNull, byte[].class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null element");
  }

  @Test
  void typeConvertListWithNullElementToFloatArrayThrowsCleanly() {
    final List<Double> withNull = Arrays.asList(1.0, null, 3.0);
    assertThatThrownBy(() -> Type.convert(database, withNull, float[].class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null element");
  }
}
