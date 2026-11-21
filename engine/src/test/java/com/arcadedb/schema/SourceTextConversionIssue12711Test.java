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
import com.arcadedb.query.sql.executor.QueryOperatorEquals;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #12711: a value a protocol read as a list from text whose type the client left unspecified (a Postgres
 * parameter of OID 0 that is a well-formed array literal) is the text it was sent as wherever it becomes a STRING: stored into a
 * STRING property, or compared with a string. Where a list is wanted it stays the list.
 */
class SourceTextConversionIssue12711Test extends TestHelper {

  private static final class ListFromText extends ArrayList<Object> implements SourceText {
    private final String text;

    private ListFromText(final String text, final List<Object> elements) {
      super(elements);
      this.text = text;
    }

    @Override
    public String sourceText() {
      return text;
    }
  }

  @Test
  void convertedToAStringItIsTheTextItWasReadFrom() {
    final DocumentType type = database.getSchema().createDocumentType("T12711");
    final Property property = type.createProperty("s", Type.STRING);

    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    assertThat(Type.convert(database, value, String.class, property)).isEqualTo("{a,b}");
    // without a property: the conversion a comparison with a string makes
    assertThat(Type.convert(database, value, String.class)).isEqualTo("{a,b}");
    assertThat(Type.convert(database, new ListFromText("{}", List.of()), String.class, property)).isEqualTo("{}");
    // a plain list keeps its own rendering
    assertThat(Type.convert(database, List.of("a", "b"), String.class, property)).isEqualTo("[a, b]");
  }

  @Test
  void comparedWithAStringItIsTheTextItWasReadFrom() {
    final ListFromText value = new ListFromText("{a,b}", List.of("a", "b"));
    assertThat(QueryOperatorEquals.equals("{a,b}", value)).isTrue();
    assertThat(QueryOperatorEquals.equals("[a, b]", value)).isFalse();
    // against a list it is still the list it was read as
    assertThat(QueryOperatorEquals.equals(List.of("a", "b"), value)).isTrue();
  }

  @Test
  void aStringPropertyStoresTheTextAndAnUndeclaredOneTheList() {
    database.getSchema().createDocumentType("T12711").createProperty("s", Type.STRING);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T12711 SET k = 1, s = ?", new ListFromText("{name}", List.of("name")));
      database.command("sql", "INSERT INTO T12711 SET k = 2, u = ?", new ListFromText("{a,b}", List.of("a", "b")));
    });

    try (final ResultSet rs = database.query("sql", "SELECT k FROM T12711 WHERE s = ?", new ListFromText("{name}", List.of("name")))) {
      assertThat(rs.next().<Integer>getProperty("k")).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT s, u, u.type() AS t FROM T12711 ORDER BY k")) {
      assertThat(rs.next().<Object>getProperty("s")).isEqualTo("{name}");
      final Result second = rs.next();
      assertThat(second.<String>getProperty("t")).isEqualTo("LIST");
      assertThat(second.<List<Object>>getProperty("u")).containsExactly("a", "b");
    }
  }
}
