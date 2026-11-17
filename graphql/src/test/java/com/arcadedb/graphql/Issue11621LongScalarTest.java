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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11621: the {@code Long} scalar was checked for its width only, so it took a float literal or a floating-point variable
 * value, bound it to a filter on a LONG property and matched nothing without saying why, where the same mistake on an {@code Int}
 * argument was refused. A {@code Long} takes an integral value in 64-bit range, or a string holding one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11621LongScalarTest extends AbstractGraphQLTest {
  private static final long TS = 1_700_000_000_000L;

  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.getSchema().getOrCreateVertexType("Event");
    database.newVertex("Event").set("id", "e1").set("ts", TS).save();
    database.newVertex("Event").set("id", "e2").set("ts", 3L).save();
    database.command("graphql", """
        type Event {
          id: String
          ts: Long
        }

        type Query {
          byTs(ts: Long): [Event]
          byTsId(ts: ID): [Event]
          byTsDirective(ts: Long): [Event] @sql(statement: "select from Event where ts = :ts order by id")
        }""");
  }

  @ParameterizedTest
  @ValueSource(strings = { "byTs", "byTsDirective" })
  void floatLiteralIsRefusedForALongArgument(final String field) {
    executeTest(database -> {
      defineTypes(database);
      for (final String literal : new String[] { "3.14", "3.0", "1e3", "1700000000000.0" })
        assertThatThrownBy(() -> ids(database, "{ " + field + "(ts: " + literal + ") { id } }", Map.of()))//
            .as(literal).isInstanceOf(CommandParsingException.class).hasMessageContaining("Long").hasMessageContaining("64-bit");
      assertThatThrownBy(() -> ids(database, "{ " + field + "(ts: true) { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class);
      assertThatThrownBy(() -> ids(database, "{ " + field + "(ts: \"abc\") { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class);
      // integer literals are still taken, whatever their width up to 64 bits
      assertThat(ids(database, "{ " + field + "(ts: 3) { id } }", Map.of())).containsExactly("e2");
      assertThat(ids(database, "{ " + field + "(ts: 1700000000000) { id } }", Map.of())).containsExactly("e1");
      assertThat(ids(database, "{ " + field + "(ts: 9223372036854775807) { id } }", Map.of())).isEmpty();
      return null;
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "byTs", "byTsDirective" })
  void floatingPointVariableIsRefusedForALongVariable(final String field) {
    executeTest(database -> {
      defineTypes(database);
      final String query = "query q($t: Long) { " + field + "(ts: $t) { id } }";
      for (final Object value : new Object[] { 3.14d, 3.0d, 3.0f, new BigDecimal("3.14"), true, "abc", "3.0", "", "-",
          "9223372036854775808", new BigInteger("9223372036854775808") })
        assertThatThrownBy(() -> ids(database, query, Map.of("t", value))).as("%s %s", value.getClass().getSimpleName(), value)
            .isInstanceOf(CommandParsingException.class).hasMessageContaining("Long");

      assertThat(ids(database, query, Map.of("t", 3))).containsExactly("e2");
      assertThat(ids(database, query, Map.of("t", TS))).containsExactly("e1");
      assertThat(ids(database, query, Map.of("t", BigInteger.valueOf(TS)))).containsExactly("e1");
      // a JSON client sends a value wider than 53 bits as a string
      assertThat(ids(database, query, Map.of("t", String.valueOf(TS)))).containsExactly("e1");
      assertThat(ids(database, query, Map.of("t", "9223372036854775807"))).isEmpty();
      assertThat(ids(database, query, Map.of("t", "-9223372036854775808"))).isEmpty();
      return null;
    });
  }

  @Test
  void variablesDecodedFromJsonAreCheckedLikeTheLiterals() {
    executeTest(database -> {
      defineTypes(database);
      // the JSON decoder hands an integer token beyond the long range over as a BigDecimal of scale 0
      final Map<String, Object> wide = new JSONObject("{\"t\": 12345678901234567890}").toMap();
      final Map<String, Object> decimal = new JSONObject("{\"t\": 3.14}").toMap();
      final Map<String, Object> exact = new JSONObject("{\"t\": 1700000000000}").toMap();

      assertThatThrownBy(() -> ids(database, "query q($t: Long) { byTs(ts: $t) { id } }", wide)).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("Long");
      assertThatThrownBy(() -> ids(database, "query q($t: Long) { byTs(ts: $t) { id } }", decimal)).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("Long");
      assertThat(ids(database, "query q($t: Long) { byTs(ts: $t) { id } }", exact)).containsExactly("e1");

      // an ID takes an integral value of any width, the one the JSON decoder could not fit in a long included
      assertThat(ids(database, "query q($t: ID) { byTsId(ts: $t) { id } }", wide)).isEmpty();
      assertThatThrownBy(() -> ids(database, "query q($t: ID) { byTsId(ts: $t) { id } }", decimal)).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("ID");
      return null;
    });
  }

  private static List<String> ids(final Database database, final String query, final Map<String, Object> variables) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("graphql", query, variables)) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().getProperty("id"));
    }
    return ids;
  }
}
