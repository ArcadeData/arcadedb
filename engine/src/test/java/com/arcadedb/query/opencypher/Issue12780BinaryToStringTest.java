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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12780: {@code toString()}, {@code toStringOrNull()} and {@code apoc.convert.toJson()} answered the Java identity
 * string {@code [B@...} for a byte[] (a BINARY property or a bytes parameter). {@code toString()} must refuse it like any
 * other value it cannot convert, {@code toStringOrNull()} must then answer null, and {@code toJson()} must serialize it as
 * the JSON array of signed bytes it already gives for a byte[] nested in a map. Indexing into a byte[] stays refused
 * (#8098), with a message that names the byte[] rather than calling it a list.
 */
class Issue12780BinaryToStringTest extends TestHelper {
  private static final byte[] BYTES = { 1, -1 };

  @BeforeEach
  void createBinaryProperty() {
    database.command("sql", "CREATE VERTEX TYPE Blob IF NOT EXISTS");
    database.transaction(() -> database.command("sql", "INSERT INTO Blob SET bin = decode('Af8=', 'base64')"));
  }

  @Test
  void toStringRefusesABinaryProperty() {
    assertThatThrownBy(() -> evaluate("MATCH (n:Blob) RETURN toString(n.bin) AS v"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toString() cannot convert byte[]");
  }

  @Test
  void toStringRefusesABytesParameter() {
    assertThatThrownBy(() -> evaluate("RETURN toString($b) AS v"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toString() cannot convert byte[]");
  }

  @Test
  void toStringOrNullAnswersNullForABinaryValue() {
    assertThat(evaluate("MATCH (n:Blob) RETURN toStringOrNull(n.bin) AS v")).isNull();
    assertThat(evaluate("RETURN toStringOrNull($b) AS v")).isNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  void toStringListAnswersNullForABinaryElement() {
    // toStringList() converts each element through toString() and answers null for one it cannot convert
    assertThat((List<Object>) evaluate("MATCH (n:Blob) RETURN toStringList([n.bin, 7]) AS v")).containsExactly(null, "7");
    assertThat((List<Object>) evaluate("RETURN toStringList([$b]) AS v")).containsExactly((Object) null);
  }

  @Test
  void toJsonSerializesABinaryValueAsTheArrayOfItsSignedBytes() {
    assertThat(evaluate("MATCH (n:Blob) RETURN apoc.convert.toJson(n.bin) AS v")).isEqualTo("[1,-1]");
    assertThat(evaluate("RETURN apoc.convert.toJson($b) AS v")).isEqualTo("[1,-1]");
  }

  @Test
  void nestedToJsonIsUnchanged() {
    assertThat(evaluate("RETURN apoc.convert.toJson({b: $b}) AS v")).isEqualTo("{\"b\":[1,-1]}");
  }

  @Test
  void indexingIntoABinaryValueIsStillRefusedWithoutCallingItAList() {
    assertThatThrownBy(() -> evaluate("MATCH (n:Blob) RETURN n.bin[0] AS v"))
        .hasMessageContaining("Cannot index into type: byte[]")
        .hasMessageNotContaining("LIST");
    assertThatThrownBy(() -> evaluate("RETURN $b[0] AS v"))
        .hasMessageContaining("Cannot index into type: byte[]")
        .hasMessageNotContaining("LIST");
  }

  private Object evaluate(final String query) {
    try (final ResultSet result = database.query("opencypher", query, Map.of("b", BYTES))) {
      return result.next().getProperty("v");
    }
  }
}
