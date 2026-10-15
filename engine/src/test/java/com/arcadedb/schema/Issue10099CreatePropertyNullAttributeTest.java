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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.parser.CreatePropertyAttributeStatement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10099: {@code CREATE PROPERTY T.a STRING (regexp null)} stored the four-character pattern {@code null}, so from then on
 * the only value the property could hold was the text {@code null}, and the state survived a reopen without a warning, while
 * {@code ALTER PROPERTY T.a REGEXP null} cleared the pattern. The two statements set the same attributes and disagreed on what
 * the word {@code null} means.
 * <p>
 * The guarantee is the family, not the reported member: for every attribute {@code CREATE PROPERTY ... ( ... )} accepts, the SQL
 * literal {@code null} produces what {@code ALTER PROPERTY ... <attribute> null} produces. The attribute list is the one the
 * statement itself publishes, so an attribute added later is under this check by construction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10099CreatePropertyNullAttributeTest extends TestHelper {

  @Test
  void createPropertyRegexpNullDoesNotStoreTheTextNull() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a STRING (regexp null)");

    assertThat(database.getSchema().getType("T").getProperty("a").getRegexp()).isNull();
    database.transaction(() -> database.command("sql", "INSERT INTO T SET a = 'hello'"));

    reopenDatabase();

    assertThat(database.getSchema().getType("T").getProperty("a").getRegexp()).as("the pattern survives no reopen, there is none")
        .isNull();
    database.transaction(() -> database.command("sql", "INSERT INTO T SET a = 'again'"));
    assertThat(database.countType("T", false)).isEqualTo(2);
  }

  @Test
  void aQuotedNullIsAPatternThatTheCallerAskedFor() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a STRING (regexp 'null')");
    assertThat(database.getSchema().getType("T").getProperty("a").getRegexp()).isEqualTo("null");

    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.a STRING");
    database.command("sql", "ALTER PROPERTY U.a REGEXP 'null'");
    assertThat(database.getSchema().getType("U").getProperty("a").getRegexp()).isEqualTo("null");
  }

  @Test
  void minAndMaxNullAreAcceptedAndLeaveNoBound() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.b LONG (min null, max null)");

    final Property property = database.getSchema().getType("T").getProperty("b");
    assertThat(property.getMin()).isNull();
    assertThat(property.getMax()).isNull();
    database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 123456789"));
  }

  @Test
  void compressionNullIsNoCompression() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a STRING (compression null)");
    assertThat(database.getSchema().getType("T").getProperty("a").getCompression()).isIn(null, "none");

    database.command("sql", "ALTER PROPERTY T.a COMPRESSION 'fast'");
    assertThat(database.getSchema().getType("T").getProperty("a").getCompression()).isEqualTo("fast");
    database.command("sql", "ALTER PROPERTY T.a COMPRESSION null");
    assertThat(database.getSchema().getType("T").getProperty("a").getCompression()).isIn(null, "none");
  }

  @Test
  void aBooleanAttributeRefusesNullWithAMessageFromBothStatements() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.a STRING");

    for (final String attribute : List.of("readonly", "mandatory", "notnull", "hidden", "external")) {
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.b" + attribute + " STRING (" + attribute + " null)")).as(
          "CREATE " + attribute).isInstanceOf(CommandExecutionException.class).hasMessageContaining("true or false");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.a " + attribute + " null")).as("ALTER " + attribute)
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("true or false");
      assertThat(database.getSchema().getType("T").existsProperty("b" + attribute)).as("the refused CREATE leaves nothing")
          .isFalse();
    }
  }

  @Test
  void aBareWordIsAFieldReferenceNotANullAndIsRefusedByBothStatements() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.s STRING (regexp '[a-z]+')");
    database.command("sql", "CREATE PROPERTY T.n LONG (min 1, max 9)");

    // ALTER used to read the unresolved word as null, which silently cleared the pattern or the bound it was meant to set
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s REGEXP abc")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("field reference").hasMessageContaining("'abc'");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.n MAX nine")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("field reference");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.n MIN one")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("field reference");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s COMPRESSION fast")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("field reference");

    final Property s = database.getSchema().getType("T").getProperty("s");
    final Property n = database.getSchema().getType("T").getProperty("n");
    assertThat(s.getRegexp()).isEqualTo("[a-z]+");
    assertThat(n.getMax()).isEqualTo("9");
    assertThat(n.getMin()).isEqualTo("1");

    // and CREATE used to store the unresolved word as the text null
    for (final String attribute : List.of("regexp abc", "compression fast"))
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.x STRING (" + attribute + ")")).as(attribute)
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("field reference");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.y LONG (max nine)")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("field reference");
    assertThat(database.getSchema().getType("T").existsProperty("x")).isFalse();
    assertThat(database.getSchema().getType("T").existsProperty("y")).isFalse();
  }

  @Test
  void nullMeansTheSameForCreateAndAlterOnEveryAttributeCreateAccepts() {
    final List<String> disagreements = new ArrayList<>();
    int index = 0;
    for (final String attribute : CreatePropertyAttributeStatement.ATTRIBUTES) {
      final String propertyType = attribute.equals("min") || attribute.equals("max") ? "LONG" : "STRING";
      final String created = "C" + index;
      final String altered = "A" + index++;

      database.command("sql", "CREATE DOCUMENT TYPE " + created);
      database.command("sql", "CREATE DOCUMENT TYPE " + altered);
      database.command("sql", "CREATE PROPERTY " + altered + ".p " + propertyType);

      final String fromCreate = outcome(created, "CREATE PROPERTY " + created + ".p " + propertyType + " (" + attribute + " null)");
      final String fromAlter = outcome(altered, "ALTER PROPERTY " + altered + ".p " + attribute + " null");

      if (!fromCreate.equals(fromAlter))
        disagreements.add(attribute + ": CREATE " + fromCreate + " but ALTER " + fromAlter);
    }
    assertThat(disagreements).as("CREATE PROPERTY and ALTER PROPERTY disagree on the SQL literal null").isEmpty();
  }

  @Test
  void theListedAttributesAreTheOnesCreateAccepts() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.a STRING (nosuchattribute 1)")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("Invalid attribute definition");

    // every listed attribute is accepted by CREATE with a value of its own kind
    int index = 0;
    for (final String attribute : CreatePropertyAttributeStatement.ATTRIBUTES) {
      final String propertyType = attribute.equals("min") || attribute.equals("max") ? "LONG" : "STRING";
      final String value = switch (attribute) {
        case "compression" -> "'fast'";
        case "min", "max" -> "5";
        case "default", "regexp" -> "'x'";
        default -> "true";
      };
      final String name = "p" + index++;
      database.command("sql", "CREATE PROPERTY T." + name + " " + propertyType + " (" + attribute + " " + value + ")");
      assertThat(database.getSchema().getType("T").existsProperty(name)).as(attribute).isTrue();
    }
  }

  /** What the statement left behind: either that it was refused, or every attribute the property carries. */
  private String outcome(final String typeName, final String sql) {
    try {
      database.command("sql", sql);
    } catch (final RuntimeException e) {
      return "REFUSED";
    }
    final Property p = database.getSchema().getType(typeName).getProperty("p");
    return "readonly=" + p.isReadonly() + " mandatory=" + p.isMandatory() + " notnull=" + p.isNotNull() + " hidden=" + p.isHidden()
        + " external=" + p.isExternal() + " compression=" + bracket(p.getCompression()) + " max=" + bracket(p.getMax()) + " min="
        + bracket(p.getMin()) + " default=" + bracket(p.getDefaultValueDefinition()) + " regexp=" + bracket(p.getRegexp());
  }

  /** The text {@code null} and no value at all must not look alike. */
  private static String bracket(final Object value) {
    return value == null ? "<none>" : "[" + value + "]";
  }
}
