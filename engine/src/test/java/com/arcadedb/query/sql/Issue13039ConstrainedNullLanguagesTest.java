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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #13039: a null written to a constrained property was judged differently by the two languages. SQL stores an explicit
 * null in the property, openCypher removes the property, and the existence constraints are read from the presence of the
 * property, so the same write was a violation in one language and not in the other.
 * <ul>
 *   <li>a MANDATORY property must have a value: a null written to it is refused in both languages;</li>
 *   <li>a NOTNULL property must not hold a null: a null written to it is refused in both languages;</li>
 *   <li>a record that never had the property stays legal, as the declaration over such a record is accepted.</li>
 * </ul>
 */
class Issue13039ConstrainedNullLanguagesTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.transaction(() -> database.command("sql", "INSERT INTO V SET k = 1, m = 'x', nn = 'y'"));
    database.command("sql", "CREATE PROPERTY V.m STRING (mandatory true)");
    database.command("sql", "CREATE PROPERTY V.nn STRING (notnull true)");
  }

  @Test
  void sqlRefusesANullOnAMandatoryProperty() {
    assertRefused("sql", "UPDATE V SET m = null WHERE k = 1", "The property 'V.m' is mandatory");
    assertThat(value("V", "m")).isEqualTo("x");
  }

  @Test
  void openCypherRefusesANullOnAMandatoryProperty() {
    assertRefused("opencypher", "MATCH (v:V) SET v.m = null", "The property 'V.m' is mandatory");
    assertThat(value("V", "m")).isEqualTo("x");
  }

  @Test
  void sqlRefusesANullOnANotNullProperty() {
    assertRefused("sql", "UPDATE V SET nn = null WHERE k = 1", "The property 'V.nn' cannot be null");
    assertThat(value("V", "nn")).isEqualTo("y");
  }

  @Test
  void openCypherRefusesANullOnANotNullProperty() {
    assertRefused("opencypher", "MATCH (v:V) SET v.nn = null", "The property 'V.nn' cannot be null");
    assertThat(value("V", "nn")).isEqualTo("y");
  }

  @Test
  void aRecordWithoutThePropertyStaysLegal() {
    // the declaration of a NOTNULL property over a record that never had it is accepted
    database.command("sql", "CREATE VERTEX TYPE W");
    database.transaction(() -> database.command("sql", "INSERT INTO W SET k = 1"));
    database.command("sql", "CREATE PROPERTY W.nn STRING (notnull true)");

    // and so is a record that does not set it
    database.transaction(() -> database.command("sql", "INSERT INTO W SET k = 2"));
    assertThat(count("SELECT count(*) AS c FROM W")).isEqualTo(2);
  }

  @Test
  void theTwoLanguagesAnswerTheSame() {
    assertRefused("sql", "UPDATE V SET m = null WHERE k = 1", "The property 'V.m' is mandatory");
    assertRefused("opencypher", "MATCH (v:V) SET v.m = null", "The property 'V.m' is mandatory");
    assertRefused("sql", "UPDATE V SET nn = null WHERE k = 1", "The property 'V.nn' cannot be null");
    assertRefused("opencypher", "MATCH (v:V) SET v.nn = null", "The property 'V.nn' cannot be null");
  }

  /**
   * The two languages wrap the refusal differently: SQL raises the validation exception itself, openCypher wraps it in the
   * error of the command. The refusal itself is what has to be the same.
   */
  private void assertRefused(final String language, final String query, final String expectedMessage) {
    final Throwable thrown = catchThrowable(() -> database.command(language, query));
    assertThat(thrown).as(query).isNotNull();
    Throwable cause = thrown;
    while (cause != null && !(cause instanceof ValidationException))
      cause = cause.getCause();
    assertThat(cause).as(query).isInstanceOf(ValidationException.class);
    assertThat(cause.getMessage()).as(query).startsWith(expectedMessage);
  }

  private Object value(final String type, final String property) {
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " FROM " + type)) {
      final List<Map<String, Object>> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next().toMap());
      return rows.isEmpty() ? null : rows.get(0).get(property);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
