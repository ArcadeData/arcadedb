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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The interval arithmetic behind {@code COMMAND_WARNINGS_EVERY}, which two copies had wrong in the same way:
 * {@code counter % every == 1} never holds at {@code every = 1}, so the setting's MOST verbose value - documented
 * as "every occurrence" - silenced the warning completely (found while fixing issue #7477).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CommandWarningsTest {
  private int saved;

  @BeforeEach
  void setUp() {
    saved = GlobalConfiguration.COMMAND_WARNINGS_EVERY.getValueAsInteger();
    CommandWarnings.resetForTests();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.COMMAND_WARNINGS_EVERY.setValue(saved);
    CommandWarnings.resetForTests();
  }

  @Test
  void anIntervalOfOneReportsEveryOccurrence() {
    GlobalConfiguration.COMMAND_WARNINGS_EVERY.setValue(1);

    assertThat(due("Person.scan", 5)).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  void anIntervalReportsTheFirstOccurrenceAndEveryNthAfterIt() {
    GlobalConfiguration.COMMAND_WARNINGS_EVERY.setValue(3);

    // the number handed back is the occurrence count, so the message can say how many times it has happened
    assertThat(due("Person.scan", 7)).containsExactly(1, 4, 7);
  }

  @Test
  void zeroDisablesTheWarningEntirely() {
    GlobalConfiguration.COMMAND_WARNINGS_EVERY.setValue(0);

    assertThat(due("Person.scan", 5)).isEmpty();
  }

  @Test
  void aNegativeIntervalIsTreatedAsDisabledRatherThanAsAnInterval() {
    GlobalConfiguration.COMMAND_WARNINGS_EVERY.setValue(-1);

    assertThat(due("Person.scan", 5)).isEmpty();
  }

  @Test
  void eachKeyIsCountedOnItsOwn() {
    GlobalConfiguration.COMMAND_WARNINGS_EVERY.setValue(2);

    assertThat(CommandWarnings.occurrencesWhenDue("Person.scan")).isEqualTo(1);
    assertThat(CommandWarnings.occurrencesWhenDue("Invoice.scan")).as("a different situation starts its own count")
        .isEqualTo(1);
    assertThat(CommandWarnings.occurrencesWhenDue("Person.scan")).as("second of two: not due").isZero();
    assertThat(CommandWarnings.occurrencesWhenDue("Person.scan")).isEqualTo(3);
  }

  /** The occurrence counts reported as due over {@code times} occurrences of {@code key}. */
  private static List<Integer> due(final String key, final int times) {
    final List<Integer> reported = new ArrayList<>();
    for (int i = 0; i < times; i++) {
      final int occurrences = CommandWarnings.occurrencesWhenDue(key);
      if (occurrences > 0)
        reported.add(occurrences);
    }
    return reported;
  }
}
