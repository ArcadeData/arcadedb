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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentValidator.RegexpUsability;
import com.arcadedb.database.DocumentValidator;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: regexp constraint.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class RegexpConstraintTest {
  /**
   * Regression for #9808: regexp on non text property.
   *
   * Issue #9808: a REGEXP is matched against the text form of a value, and was accepted on a property whose values have no
   * text form a pattern can describe, or none the pattern could ever match: the declaration succeeded and refused every later
   * write, while MIN and MAX are refused at declaration time when the type cannot use them. A pattern over the text of a
   * number or a temporal keeps working ({@code '[0-9]{3}'} on a LONG).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RegexpOnNonTextProperty extends TestHelper {

    @Test
    void aPatternIsRefusedOnATypeWithoutATextFormThroughEveryDoor() {
      database.command("sql", "CREATE DOCUMENT TYPE E");
      database.command("sql", "CREATE DOCUMENT TYPE T");
      // a LINK has a text form since #9992, the RID it points to, so '[A-Z]+' is refused on it as a pattern no RID matches
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.plink LINK (regexp '[A-Z]+')"))
          .hasMessageContaining("matches no text a value of LINK");
      for (final String declared : List.of("LIST", "MAP", "BINARY", "EMBEDDED", "ARRAY_OF_FLOATS", "ARRAY_OF_LONGS")) {
        final String name = "p" + declared.toLowerCase();
        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T." + name + " " + declared + " (regexp '[A-Z]+')"))
            .as(declared).hasMessageContaining("is not applicable to type " + declared);
        assertThat(database.getSchema().getType("T").existsProperty(name)).as(declared).isFalse();

        database.command("sql", "CREATE PROPERTY T." + name + " " + declared);
        assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T." + name + " REGEXP '[A-Z]+'")).as(declared)
            .hasMessageContaining("is not applicable to type " + declared);
        final Property property = database.getSchema().getType("T").getProperty(name);
        assertThatThrownBy(() -> property.setRegexp("[A-Z]+")).as(declared).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("T." + name);
        assertThat(property.getRegexp()).as(declared).isNull();
      }
    }

    @Test
    void aBooleanPatternMustMatchTrueOrFalse() {
      database.command("sql", "CREATE DOCUMENT TYPE B");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY B.flag BOOLEAN (regexp '[A-Z]+')")).hasMessageContaining(
          "matches neither 'true' nor 'false'");

      // a pattern that admits one of the two still constrains the property, as the OrientDB importer carries 'true|false'
      database.command("sql", "CREATE PROPERTY B.flag BOOLEAN (regexp 'true')");
      database.transaction(() -> database.newDocument("B").set("flag", true).save());
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("B").set("flag", false).save())).isInstanceOf(
          ValidationException.class);

      database.command("sql", "ALTER PROPERTY B.flag REGEXP 'true|false'");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY B.flag REGEXP 'yes|no'")).hasMessageContaining(
          "matches neither 'true' nor 'false'");
      assertThat(database.getSchema().getType("B").getProperty("flag").getRegexp()).isEqualTo("true|false");
    }

    @Test
    void aPatternOverTheTextOfANumberStaysLegal() {
      database.command("sql", "CREATE DOCUMENT TYPE Z");
      database.command("sql", "CREATE PROPERTY Z.zip LONG (regexp '[0-9]{5}')");
      database.transaction(() -> database.command("sql", "INSERT INTO Z SET zip = 12345").close());
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Z SET zip = 123").close()))
          .hasMessageContaining("regular expression");
    }

    @Test
    void anInvalidPatternIsRefusedByTheJavaApiToo() {
      final Property property = database.getSchema().createDocumentType("S").createProperty("s", Type.STRING);
      assertThatThrownBy(() -> property.setRegexp("[a-")).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("is not a valid regular expression");
      assertThat(property.getRegexp()).isNull();
    }

    @Test
    void aSchemaWrittenBeforeTheRuleStillOpensAndCanBeRepaired() throws IOException {
      database.command("sql", "CREATE DOCUMENT TYPE L");
      database.command("sql", "CREATE PROPERTY L.tags LIST");
      database.close();

      final File schemaFile = new File(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      root.getJSONObject("types").getJSONObject("L").getJSONObject("properties").getJSONObject("tags").put("regexp", "[A-Z]+");
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      assertThat(database.getSchema().getType("L").getProperty("tags").getRegexp()).isEqualTo("[A-Z]+");

      database.command("sql", "ALTER PROPERTY L.tags REGEXP null");
      assertThat(database.getSchema().getType("L").getProperty("tags").getRegexp()).isNull();
    }
  }

  /**
   * Regression for #9853: regexp no value can match.
   *
   * Issue #9853: a REGEXP on a number or a temporal no value of the type can match (a text pattern like {@code '[A-Z]+'} on a
   * LONG or a DATE) was accepted at declaration time and then refused every write of the property. Every text form of such a
   * value is written with a small, fixed set of characters, so a pattern no string over that set matches is refused, through
   * every door a REGEXP is declared by; one some value matches stays legal and lets that value through.
   */
  @Nested
  class RegexpNoValueCanMatch extends TestHelper {

    /** one row per declared type: a pattern no value can match, one a value can, and that value */
    private record Row(String type, String unmatchable, String matchable, Object value) {
    }

    private static final List<Row> ROWS = List.of(//
        new Row("BYTE", "[A-Z]+", "[0-9]", (byte) 7),//
        new Row("SHORT", "x+", "-?[0-9]+", (short) -12),//
        new Row("INTEGER", ",[0-9]+", "[0-9]{3}", 123),//
        new Row("LONG", "[A-Z]+", "[0-9]{5}", 12345L),//
        new Row("FLOAT", "[0-9],[0-9]", "[0-9]+\\.[0-9]+", 1.5f),//
        new Row("DOUBLE", "[a-z]{2}x", "[0-9]+\\.[0-9]+", 2.25d),//
        new Row("DECIMAL", "\\$[0-9]+", "[0-9]+\\.[0-9]{2}", new BigDecimal("3.14")),//
        new Row("DATE", "[A-Z]+", "[0-9]{4}-01-[0-9]{2}", LocalDate.of(2024, 1, 15)),//
        new Row("DATETIME", "[0-9]{2}/[0-9]{2}/[0-9]{4}", "2024-01-15T.*", LocalDateTime.of(2024, 1, 15, 10, 30)),//
        new Row("DATETIME_SECOND", "Mon.*", "2024-01-15T.*", LocalDateTime.of(2024, 1, 15, 10, 30, 5)),//
        new Row("DATETIME_MICROS", "[a-z]+", "2024-.*", LocalDateTime.of(2024, 1, 15, 10, 30, 5, 123_000)),//
        new Row("DATETIME_NANOS", "[a-z]+", "2024-.*", LocalDateTime.of(2024, 1, 15, 10, 30, 5, 123_456_789)),//
        new Row("LOCAL_TIME", "(AM|PM) [0-9]+", "[0-9]{2}:[0-9]{2}", LocalTime.of(10, 30)),//
        new Row("OFFSET_TIME", "UTC[0-9:]+", "[0-9:]+Z", OffsetTime.of(10, 30, 0, 0, ZoneOffset.UTC)),//
        new Row("ZONED_DATETIME", "#.*", "2024-.*",
            ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("Europe/Rome"))),//
        new Row("DURATION", "hours=[0-9]+", "PT[0-9]+H", "PT2H"));

    @Test
    void aPatternNoValueCanMatchIsRefusedThroughEveryDoor() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      for (final Row row : ROWS) {
        final String name = "p" + row.type().toLowerCase();
        final String what = row.type() + " " + row.unmatchable();

        assertThatThrownBy(() -> database.command("sql",
            "CREATE PROPERTY T." + name + " " + row.type() + " (regexp '" + sqlEscape(row.unmatchable()) + "')")).as(what)
            .hasMessageContaining("would refuse every value");
        assertThat(database.getSchema().getType("T").existsProperty(name)).as(what).isFalse();

        database.command("sql", "CREATE PROPERTY T." + name + " " + row.type());
        assertThatThrownBy(() -> database.command("sql",
            "ALTER PROPERTY T." + name + " REGEXP '" + sqlEscape(row.unmatchable()) + "'")).as(what)
            .hasMessageContaining("T." + name).hasMessageContaining("would refuse every value");

        final Property property = database.getSchema().getType("T").getProperty(name);
        assertThatThrownBy(() -> property.setRegexp(row.unmatchable())).as(what).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("T." + name).hasMessageContaining(row.type());
        assertThat(property.getRegexp()).as(what).isNull();
      }
    }

    @Test
    void aPatternSomeValueMatchesIsAcceptedAndLetsThatValueThrough() {
      database.command("sql", "CREATE DOCUMENT TYPE A");
      for (final Row row : ROWS) {
        final String name = "p" + row.type().toLowerCase();
        final String what = row.type() + " " + row.matchable();
        database.command("sql", "CREATE PROPERTY A." + name + " " + row.type() + " (regexp '" + sqlEscape(row.matchable()) + "')");
        assertThat(database.getSchema().getType("A").getProperty(name).getRegexp()).as(what).isEqualTo(row.matchable());

        database.transaction(() -> database.newDocument("A").set(name, row.value()).save());
      }
      assertThat(database.countType("A", false)).isEqualTo(ROWS.size());
    }

    @Test
    void theReportedDeclarationsAreRefused() {
      database.command("sql", "CREATE VERTEX TYPE R1");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY R1.n LONG (regexp '[A-Z]+')")).hasMessageContaining(
          "The REGEXP '[A-Z]+' of property 'R1.n' matches no text a value of LONG can be written as");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '[A-Z]+')")).hasMessageContaining(
          "DATE (held as LocalDate)");

      // letters a FLOAT is written with ('E', 'N', 'I') never make up one on their own; no BYTE has five digits
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY R1.f FLOAT (regexp '[A-Z]+')")).hasMessageContaining(
          "would refuse every value");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY R1.b BYTE (regexp '[0-9]{5}')")).hasMessageContaining(
          "would refuse every value");

      database.command("sql", "CREATE PROPERTY R1.k LONG");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY R1.k REGEXP '[A-Z]+'")).hasMessageContaining(
          "would refuse every value");
      assertThat(database.getSchema().getType("R1").getProperty("k").getRegexp()).isNull();

      // the declaration that stays legal keeps constraining the values
      database.command("sql", "ALTER PROPERTY R1.k REGEXP '[0-9]{5}'");
      database.transaction(() -> database.command("sql", "INSERT INTO R1 SET k = 12345").close());
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO R1 SET k = 1").close()))
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void aDateIsJudgedByTheClassTheDatabaseHoldsItAs() {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Class<?> previous = serializer.getDateImplementation();
      database.command("sql", "CREATE DOCUMENT TYPE D");
      // held as LocalDate: written 2024-01-15, never with a weekday
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY D.a DATE (regexp '[A-Z][a-z]{2} .*')"))
          .hasMessageContaining("DATE (held as LocalDate)");

      serializer.setDateImplementation(Date.class);
      try {
        // held as java.util.Date: written 'Mon Jan 15 00:00:00 CET 2024', so the weekday pattern is the legal one...
        database.command("sql", "CREATE PROPERTY D.a DATE (regexp '[A-Z][a-z]{2} .*')");
        database.transaction(() -> database.newDocument("D").set("a", LocalDate.of(2024, 1, 15)).save());
        // ... and the ISO one is refused, naming the class
        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY D.b DATE (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')"))
            .hasMessageContaining("DATE (held as Date)").hasMessageContaining("would refuse every value");
      } finally {
        serializer.setDateImplementation(previous);
      }
    }

    @Test
    void aPatternTheSearchCannotDecideIsAcceptedWithAWarning() {
      final List<String> warnings = new ArrayList<>();
      final Logger previous = LogManager.instance().getLogger();
      LogManager.instance().setLogger(new CapturingLogger(warnings));
      try {
        database.command("sql", "CREATE DOCUMENT TYPE W");
        // a look-ahead no automaton expresses, asking for more digits than a LONG has: every string of digits might still
        // grow into a match, so the search can neither find one nor rule all of them out. A regular pattern is decided since
        // #9990, and this is what is left undecided
        database.command("sql", "CREATE PROPERTY W.n LONG (regexp '(?=[0-9]{20})[0-9]+')");
        assertThat(database.getSchema().getType("W").getProperty("n").getRegexp()).isEqualTo("(?=[0-9]{20})[0-9]+");
        assertThat(warnings).anySatisfy(w -> assertThat(w).contains("W.n").contains("LONG")
            .contains("ALTER PROPERTY `W`.`n` REGEXP null"));

        // the regular patterns this test used to leave undecided are now refused at the declaration (#9990)
        for (final String regexp : List.of("[0-9]*[A-Z]", "[0-9]+,[0-9]+"))
          assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY W.c LONG (regexp '" + regexp + "')")).as(regexp)
              .hasMessageContaining("would refuse every value");

        // a pattern the search decides costs no warning
        warnings.clear();
        database.command("sql", "CREATE PROPERTY W.z LONG (regexp '[0-9]{5}')");
        assertThat(warnings).noneSatisfy(w -> assertThat(w).contains("W.z"));
      } finally {
        LogManager.instance().setLogger(previous);
      }
    }

    @Test
    void aSchemaWrittenBeforeTheRuleStillOpensAndCanBeRepaired() throws IOException {
      database.command("sql", "CREATE DOCUMENT TYPE L");
      database.command("sql", "CREATE PROPERTY L.n LONG");
      database.close();

      final File schemaFile = new File(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      root.getJSONObject("types").getJSONObject("L").getJSONObject("properties").getJSONObject("n").put("regexp", "[A-Z]+");
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      assertThat(database.getSchema().getType("L").getProperty("n").getRegexp()).isEqualTo("[A-Z]+");

      database.command("sql", "ALTER PROPERTY L.n REGEXP null");
      assertThat(database.getSchema().getType("L").getProperty("n").getRegexp()).isNull();
    }

    private static String sqlEscape(final String pattern) {
      return pattern.replace("\\", "\\\\").replace("'", "\\'");
    }

    private static final class CapturingLogger implements Logger {
      private final List<String> warnings;

      private CapturingLogger(final List<String> warnings) {
        this.warnings = warnings;
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
          final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
          final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        log(requester, level, message, exception, context,
            new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17 });
      }

      @Override
      public synchronized void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (level == Level.WARNING && message != null)
          warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
      }

      @Override
      public void flush() {
      }
    }
  }

  /**
   * Regression for #9990: regexp decided on every type.
   *
   * Issue #9990: an impossible REGEXP was refused at declaration time only on the types whose texts are few enough to
   * enumerate (a BYTE), because the proof was a budgeted search: a LONG accepted {@code '[0-9]{25}'} with a doubt and then
   * refused every write. The check now decides every regular pattern on every type, whatever its width.
   * <p>
   * The guarantee is the table below: for EVERY type a pattern can be declared on and checked against a text form, a pattern no
   * value can match is refused, and one some value matches is accepted without a doubt. A type added to {@link Type} without a
   * row fails {@link #theTableCoversEveryTypeWithATextForm}, so the width of a type cannot decide the outcome again.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RegexpDecidedOnEveryType extends TestHelper {

    /** per type: patterns no value can match, then patterns some value matches */
    private static final Map<Type, List<List<String>>> TABLE = new EnumMap<>(Type.class);

    static {
      row(Type.BYTE, List.of("[0-9]{4}", "-[0-9]{4,}", "~"), List.of("[0-9]{3}", "-1[0-9]{2}"));
      row(Type.SHORT, List.of("[0-9]{6}", "[0-9]+\\.[0-9]+"), List.of("[0-9]{5}", "-?[0-9]+"));
      row(Type.INTEGER, List.of("[0-9]{11}", "[0-9]*[a-z][0-9]*"), List.of("[0-9]{10}", "[1-9][0-9]{9}"));
      row(Type.LONG, List.of("[0-9]{20}", "[0-9]{25}", "-?[0-9]+-"), List.of("[0-9]{19}", "-[0-9]{19}", "[0-9]{5}"));
      row(Type.FLOAT, List.of("[0-9]+", "-?[0-9]+E[0-9]+"), List.of("[0-9]+\\.[0-9]+", "NaN|Infinity"));
      row(Type.DOUBLE, List.of("[0-9]+", "Inf"), List.of("[0-9]+\\.[0-9]+E-?[0-9]+", "-Infinity"));
      row(Type.DECIMAL, List.of("[0-9]+\\.[0-9]+\\.[0-9]+", "[0-9]+E[0-9]+"), List.of("[0-9]{40}", "[0-9]+\\.[0-9]{2}"));
      row(Type.DATE, List.of("[0-9]{8}", "[0-9]{4}/[0-9]{2}/[0-9]{2}"), List.of("[0-9]{4}-[0-9]{2}-[0-9]{2}", "2024-.*"));
      for (final Type datetime : List.of(Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS))
        row(datetime, List.of("[0-9]{4}-[0-9]{2}-[0-9]{2}", "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}"),
            List.of("[0-9]{4}-[0-9]{2}-[0-9]{2}T.*", ".*T[0-9]{2}:[0-9]{2}"));
      row(Type.ZONED_DATETIME, List.of("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}", "[^\\[]*\\[UTC\\]X"),
          List.of(".*\\[Europe/Rome\\]", ".*Z"));
      row(Type.LOCAL_TIME, List.of("[0-9]{2}", "[0-9]{2}:[0-9]{2}Z"), List.of("[0-9]{2}:[0-9]{2}", "10:30:00\\.[0-9]{3}"));
      row(Type.OFFSET_TIME, List.of("[0-9]{2}:[0-9]{2}", "[0-9]{2}:[0-9]{2}[A-Y]"), List.of("[0-9]{2}:[0-9]{2}Z", ".*\\+01:00"));
      row(Type.DURATION, List.of("[0-9]+", "PT[0-9]+X"), List.of("P.*", "PT[0-9]+S"));
      row(Type.LINK, List.of("[0-9]+", "#[0-9]+", "#[0-9]+:[a-z]+"), List.of("#12:.*", "#[0-9]+:[0-9]+"));
    }

    private static void row(final Type type, final List<String> impossible, final List<String> possible) {
      TABLE.put(type, List.of(impossible, possible));
    }

    @Test
    void theTableCoversEveryTypeWithATextForm() {
      // the types a pattern is not checked against a text form on: anything goes on a STRING, a BOOLEAN has two texts, and the
      // rest have no text form at all and are refused outright
      final Set<Type> notSearched = EnumSet.of(Type.STRING, Type.BOOLEAN, Type.BINARY, Type.LIST, Type.MAP, Type.EMBEDDED,
          Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_LONGS, Type.ARRAY_OF_FLOATS, Type.ARRAY_OF_DOUBLES);
      for (final Type type : Type.values())
        assertThat(TABLE.containsKey(type) || notSearched.contains(type)).as(
            "%s has no row: add the patterns no value of it can match, and some that one does", type).isTrue();
    }

    @Test
    void anImpossiblePatternIsRefusedOnEveryTypeAndAPossibleOneAcceptedWithoutADoubt() {
      for (final Map.Entry<Type, List<List<String>>> row : TABLE.entrySet()) {
        final Type type = row.getKey();
        for (final String impossible : row.getValue().get(0)) {
          final RegexpUsability usability = DocumentValidator.regexpUsability(database, type, impossible);
          assertThat(usability.refusal()).as("'%s' on %s", impossible, type).isNotNull().contains("matches no text");
        }
        for (final String possible : List.of(row.getValue().get(1), List.of(".*", "(.*)", ".+")).stream().flatMap(List::stream)
            .toList()) {
          final RegexpUsability usability = DocumentValidator.regexpUsability(database, type, possible);
          assertThat(usability.refusal()).as("'%s' on %s", possible, type).isNull();
          assertThat(usability.doubt()).as("'%s' on %s", possible, type).isNull();
        }
      }
    }

    @Test
    void theReproductionOfTheIssueIsRefusedAtDeclaration() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      for (final String declaration : List.of("SHORT (regexp '[0-9]{6}')", "INTEGER (regexp '[0-9]{11}')", "LONG (regexp '[0-9]{20}')",
          "LONG (regexp '[0-9]{25}')", "DOUBLE (regexp '[0-9]+')", "FLOAT (regexp '[0-9]+')", "DATE (regexp '[0-9]{8}')",
          "DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')"))
        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY D.a " + declaration)).as(declaration)
            .hasMessageContaining("matches no text");
      assertThat(database.getSchema().getType("D").existsProperty("a")).isFalse();

      // the satisfiable ones still constrain the writes
      database.command("sql", "CREATE PROPERTY D.a LONG (regexp '[0-9]{19}')");
      database.transaction(() -> database.command("sql", "INSERT INTO D SET a = 1234567890123456789").close());
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO D SET a = 7").close()))
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void aPatternTooLargeForAnAutomatonDegradesToADoubtNeverToARefusal() {
      // a million states: past RegexAutomaton.MAX_STATES, so the check falls back to the samples and the budgeted search,
      // which cannot rule it out either. Accepted with a doubt, and a write tells the truth about it
      final RegexpUsability tooLarge = DocumentValidator.regexpUsability(database, Type.LONG, "[0-9]{1000000}");
      assertThat(tooLarge.refusal()).isNull();
      assertThat(tooLarge.doubt()).isNotNull();
      // the same size, satisfiable: a sample settles it
      final RegexpUsability satisfiable = DocumentValidator.regexpUsability(database, Type.LONG, "[0-9]{0,1000000}");
      assertThat(satisfiable.refusal()).isNull();
      assertThat(satisfiable.doubt()).isNull();
    }

    @Test
    void aPathologicalPatternGivesUpInsteadOfHoldingTheDeclaration() {
      // nested counted repetitions: far past the automaton's size cap, and a search the budget has to end
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final RegexpUsability usability = DocumentValidator.regexpUsability(database, Type.LONG, "((0{1,100}){1,100}){1,100}9{20}");
      stopwatch.assertGaveUpWithin(10_000, "a declaration-time check bounded by its deadline, from one that is not");
      assertThat(usability.refusal()).isNull();
      assertThat(usability.doubt()).isNotNull();
    }

    @Test
    void aPatternNoAutomatonExpressesIsStillDecidedWhenASampleValueMatchesIt() {
      // a back reference: settled by the sample values, the edge values of the type, before any search
      final RegexpUsability backReference = DocumentValidator.regexpUsability(database, Type.LONG, "(0)\\1*");
      assertThat(backReference.refusal()).isNull();
      assertThat(backReference.doubt()).isNull();
      // and a look-ahead no value of a wide type matches is still searched for, and only doubted
      final RegexpUsability lookahead = DocumentValidator.regexpUsability(database, Type.LONG, "(?=[0-9]{20})[0-9]+");
      assertThat(lookahead.refusal()).isNull();
      assertThat(lookahead.doubt()).isNotNull();
    }
  }

  /**
   * Regression for #9992: link regexp text form.
   *
   * Issue #9992: the REGEXP gate refused a pattern on a LINK while the write path matched one anyway, against the
   * {@code toString()} of whatever object the link arrived as, so the same link passed as a RID ({@code #1:0}) and failed as
   * the record it points to ({@code #1:0[n=0]}). A LINK is now matched as the RID it points to, by the write path, the check of
   * stored records and the declaration alike, so they cannot disagree. A type whose values have no text at all (a LIST) refuses
   * the write of a pattern a schema written before the rule still carries, saying what the declaration would have said,
   * instead of matching it against a bracketed dump.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LinkRegexpTextForm extends TestHelper {

    @Test
    void aLinkIsMatchedAsItsRidWhateverObjectItArrivedAs() {
      database.command("sql", "CREATE VERTEX TYPE V9992");
      database.command("sql", "CREATE DOCUMENT TYPE T9992");

      final Vertex[] targets = new Vertex[2];
      database.transaction(() -> {
        targets[0] = database.newVertex("V9992").set("n", 0).save();
        targets[1] = database.newVertex("V9992").set("n", 1).save();
      });
      final RID first = targets[0].getIdentity();
      // the pattern names the first link exactly: '#3:0' matches it, its own record writes itself as '#3:0[n=0]'
      database.command("sql", "CREATE PROPERTY T9992.l LINK (regexp '" + first + "')");

      // the same link, as a RID and as the record: both pass
      database.transaction(() -> database.newDocument("T9992").set("l", first).save());
      database.transaction(() -> database.newDocument("T9992").set("l", targets[0]).save());
      database.transaction(() -> database.newDocument("T9992").set("l", first.asVertex()).save());
      // and as the text a JSON or HTTP client sends
      database.transaction(() -> database.newDocument("T9992").set("l", first.toString()).save());

      // the other link, both ways: both fail
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T9992").set("l", targets[1].getIdentity()).save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T9992").set("l", targets[1]).save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");

      // SQL writes the link as the record a subquery selects
      database.transaction(() -> database.command("sql", "INSERT INTO T9992 SET l = (SELECT FROM V9992 WHERE n = 0)").close());
      assertThat(database.countType("T9992", false)).isEqualTo(5);
    }

    @Test
    void aPatternOnALinkIsJudgedAgainstTheShapeOfARid() {
      database.command("sql", "CREATE DOCUMENT TYPE L9992");
      // what the issue's legacy schema matched against: the text of a record, which a link is never written as any more
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY L9992.l LINK (regexp '#[0-9]+:[0-9]+\\\\[.*\\\\]')"))
          .hasMessageContaining("matches no text a value of LINK");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY L9992.l LINK (regexp '[A-Z]+')"))
          .hasMessageContaining("matches no text a value of LINK");
      database.command("sql", "CREATE PROPERTY L9992.l LINK (regexp '#12:.*')");
      assertThat(database.getSchema().getType("L9992").getProperty("l").getRegexp()).isEqualTo("#12:.*");
    }

    @Test
    void aPatternFromASchemaWrittenBeforeTheRuleIsMatchedTheSameWayOnALink() throws IOException {
      database.command("sql", "CREATE VERTEX TYPE W9992");
      database.command("sql", "CREATE DOCUMENT TYPE S9992");
      database.command("sql", "CREATE PROPERTY S9992.l LINK");
      database.command("sql", "CREATE PROPERTY S9992.tags LIST");
      database.command("sql", "CREATE PROPERTY S9992.s STRING");
      database.close();

      // patterns no declaration accepts today: the text of a selected record on the LINK, and one on a type with no text
      final File schemaFile = new File(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      final JSONObject properties = root.getJSONObject("types").getJSONObject("S9992").getJSONObject("properties");
      properties.getJSONObject("l").put("regexp", "#[0-9]+:[0-9]+\\[.*\\]");
      properties.getJSONObject("tags").put("regexp", "\\[.*\\]");
      // and one that does not even compile
      properties.getJSONObject("s").put("regexp", "[a-");
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      final Vertex[] saved = new Vertex[1];
      database.transaction(() -> saved[0] = database.newVertex("W9992").set("n", 0).save());
      final Vertex target = saved[0];

      // the same link fails the same way, as a RID and as the record whose toString() the pattern was written for
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("S9992").set("l", target.getIdentity()).save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("S9992").set("l", target).save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("does not match the regular expression");

      // a LIST has no text: the write says so and names the remedy, rather than matching the pattern against '[a, b]'
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument document = database.newDocument("S9992");
        document.set("tags", List.of("a", "b"));
        document.save();
      })).isInstanceOf(ValidationException.class).hasMessageContaining("is not applicable to type LIST")
          .hasMessageContaining("REGEXP null");

      // a pattern that does not compile says so, with the remedy, rather than escaping as a PatternSyntaxException
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("S9992").set("s", "abc").save()))
          .isInstanceOf(ValidationException.class).hasMessageContaining("is not a valid regular expression")
          .hasMessageContaining("REGEXP null");

      // the remedy works, and so does a write without the property
      database.transaction(() -> database.newDocument("S9992").save());
      database.command("sql", "ALTER PROPERTY S9992.tags REGEXP null");
      database.command("sql", "ALTER PROPERTY S9992.l REGEXP null");
      database.transaction(() -> database.newDocument("S9992").set("l", target).set("tags", List.of("a")).save());
    }
  }
}
