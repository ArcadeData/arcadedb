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
import com.arcadedb.database.DocumentValidator;
import com.arcadedb.database.DocumentValidator.RegexpUsability;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue9990RegexpDecidedOnEveryTypeTest extends TestHelper {

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
