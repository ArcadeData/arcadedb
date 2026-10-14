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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
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
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9853: a REGEXP on a number or a temporal no value of the type can match (a text pattern like {@code '[A-Z]+'} on a
 * LONG or a DATE) was accepted at declaration time and then refused every write of the property. Every text form of such a
 * value is written with a small, fixed set of characters, so a pattern no string over that set matches is refused, through
 * every door a REGEXP is declared by; one some value matches stays legal and lets that value through.
 */
class Issue9853RegexpNoValueCanMatchTest extends TestHelper {

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
