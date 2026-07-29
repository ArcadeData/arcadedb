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
package com.arcadedb.server.http.handler.batch;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which characters split a JSONL payload into records, asked by the reporter of issue #5470 after finding U+2028
 * (LINE SEPARATOR) and U+2029 (PARAGRAPH SEPARATOR) inside exported data.
 * <p>
 * The answer is the contract of {@code BufferedReader.readLine()} and nothing else - LF, CR and CRLF - which
 * matters because it is NOT the contract of the alternatives: {@code Scanner} and several JavaScript line
 * splitters also break on U+2028, U+2029 and U+0085, so the same file can be read as a different number of records
 * by the producer and by the loader. Both halves are pinned here: the separators that do split, and the characters
 * that must be carried through into the value untouched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JsonlBatchRecordStreamLineEndingTest {

  private static final String LINE_SEPARATOR      = " ";
  private static final String PARAGRAPH_SEPARATOR = " ";
  private static final String NEXT_LINE           = "";

  /**
   * These are legal, unescaped characters inside a JSON string. They must reach the property value as they are,
   * not cut the record in half.
   */
  @Test
  void unicodeSeparatorsInsideAValueAreOrdinaryCharacters() {
    final String name = "Rue de la Paix" + LINE_SEPARATOR + "Apt " + PARAGRAPH_SEPARATOR + "3" + NEXT_LINE + "b";
    final List<Object[]> records = read(
        "{\"@type\":\"vertex\",\"@class\":\"Address\",\"name\":\"" + name + "\"}\n"
            + "{\"@type\":\"vertex\",\"@class\":\"Address\",\"name\":\"second\"}\n");

    assertThat(records).hasSize(2);
    assertThat(propertyOf(records.get(0), "name")).isEqualTo(name);
    assertThat(propertyOf(records.get(1), "name")).isEqualTo("second");
  }

  @Test
  void lfCrAndCrLfAllSeparateRecords() {
    assertThat(read("{\"@type\":\"vertex\",\"@class\":\"V\"}\n{\"@type\":\"vertex\",\"@class\":\"V\"}\n")).hasSize(2);
    assertThat(read("{\"@type\":\"vertex\",\"@class\":\"V\"}\r\n{\"@type\":\"vertex\",\"@class\":\"V\"}\r\n")).hasSize(2);
    assertThat(read("{\"@type\":\"vertex\",\"@class\":\"V\"}\r{\"@type\":\"vertex\",\"@class\":\"V\"}\r")).hasSize(2);
  }

  /**
   * A record is not required to be followed by a terminator: the last line of a file that does not end with a
   * newline is a record like any other, so a payload never loses its tail.
   */
  @Test
  void aLastLineWithoutATerminatorIsStillARecord() {
    assertThat(read("{\"@type\":\"vertex\",\"@class\":\"V\"}\n{\"@type\":\"vertex\",\"@class\":\"V\"}")).hasSize(2);
  }

  @Test
  void blankLinesAreSkippedAndDoNotCount() {
    assertThat(read("{\"@type\":\"vertex\",\"@class\":\"V\"}\n\n   \n{\"@type\":\"vertex\",\"@class\":\"V\"}\n")).hasSize(2);
  }

  /**
   * The stream reuses one {@link BatchRecord} across calls, so each record's properties are copied out as they are
   * read: [name, value, name, value, ...].
   */
  private static List<Object[]> read(final String body) {
    final List<Object[]> records = new ArrayList<>();
    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(
        new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)))) {
      while (stream.hasNext())
        records.add(stream.next().copyProperties());
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
    return records;
  }

  private static Object propertyOf(final Object[] properties, final String name) {
    for (int i = 0; i < properties.length; i += 2)
      if (name.equals(properties[i]))
        return properties[i + 1];
    return null;
  }
}
