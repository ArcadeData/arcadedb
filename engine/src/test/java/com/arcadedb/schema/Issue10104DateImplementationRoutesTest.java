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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.serializer.BinarySerializer;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10104: #9976 guarded the one route the report named, {@code ALTER DATABASE}, at its call site, and the failure it
 * reported reproduced verbatim by the other routes that change the class a DATE or DATETIME value is held as. The held class is
 * decided by {@code arcadedb.dateImplementation} and {@code arcadedb.dateTimeImplementation}, which the context configuration
 * an open reads sets as well, and by the public setters of the serializer.
 * <p>
 * The guarantee is that every route either refuses or warns, naming the property: the statement refuses (a statement can be
 * refused), while the open and the setter, which cannot refuse (an open must not fail over a constraint), warn.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10104DateImplementationRoutesTest extends TestHelper {
  private static final String ISO_DATE = "[0-9]{4}-[0-9]{2}-[0-9]{2}";

  @Test
  void theStatementRouteRefuses() {
    declareIsoPattern();

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'")).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("R1.d");
    assertThat(serializer().getDateImplementation()).isEqualTo(LocalDate.class);
  }

  @Test
  void theContextConfigurationAtOpenWarnsNamingTheProperty() {
    declareIsoPattern();
    database.close();

    // what a -Darcadedb.dateImplementation=java.util.Date on the command line does: open() resets the context configuration
    // and reads the saved one, so the value that reaches an open is the one the setting itself holds
    GlobalConfiguration.DATE_IMPLEMENTATION.setValue(Date.class);
    try {
      final List<String> warnings = WarningCapture.captureWarnings(() -> database = factory.open());

      assertThat(database.isOpen()).as("an open must not fail over a constraint").isTrue();
      assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
      assertThat(warnings).as("the warnings the open logged").anyMatch(
          w -> w.contains("R1.d") && w.contains(ISO_DATE) && w.contains("REGEXP null"));

      // the symptom the warning announces: the property can no longer be written
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')")
          .close())).isInstanceOf(ValidationException.class).hasMessageContaining("R1.d");

      // and its remedy works
      database.command("sql", "ALTER PROPERTY R1.d REGEXP null");
      database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')").close());
    } finally {
      GlobalConfiguration.DATE_IMPLEMENTATION.reset();
    }
  }

  @Test
  void theDatetimeImplementationAtOpenWarnsToo() {
    database.command("sql", "CREATE VERTEX TYPE R2");
    database.command("sql", "CREATE PROPERTY R2.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
    database.close();

    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(Date.class);
    try {
      final List<String> warnings = WarningCapture.captureWarnings(() -> database = factory.open());
      assertThat(warnings).anyMatch(w -> w.contains("R2.t"));
    } finally {
      GlobalConfiguration.DATE_TIME_IMPLEMENTATION.reset();
    }
  }

  @Test
  void thePublicSetterWarnsNamingTheProperty() {
    declareIsoPattern();

    final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateImplementation(Date.class));
    try {
      assertThat(warnings).anyMatch(w -> w.contains("R1.d") && w.contains(ISO_DATE));
    } finally {
      serializer().setDateImplementation(LocalDate.class);
    }

    // given by name, as the configuration holds it
    final List<String> byName = WarningCapture.captureWarnings(() -> serializer().setDateImplementation("java.util.Date"));
    try {
      assertThat(byName).anyMatch(w -> w.contains("R1.d"));
    } finally {
      serializer().setDateImplementation(LocalDate.class);
    }
  }

  @Test
  void theDatetimeSetterWarnsForTheTypesItDecidesOnly() {
    database.command("sql", "CREATE VERTEX TYPE R3");
    database.command("sql", "CREATE PROPERTY R3.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
    // a microsecond datetime stays a LocalDateTime when DATETIME becomes a java.util.Date, so its pattern is not in question
    database.command("sql", "CREATE PROPERTY R3.micros DATETIME_MICROS (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");

    final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateTimeImplementation(Date.class));
    try {
      assertThat(warnings).anyMatch(w -> w.contains("R3.t")).noneMatch(w -> w.contains("R3.micros"));
    } finally {
      serializer().setDateTimeImplementation(LocalDateTime.class);
    }
  }

  @Test
  void aPatternTheNewClassSatisfiesWarnsNothing() {
    database.command("sql", "CREATE VERTEX TYPE R4");
    database.command("sql", "CREATE PROPERTY R4.d DATE (regexp '.*20[0-9]{2}.*')");

    final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateImplementation(Date.class));
    try {
      assertThat(warnings).noneMatch(w -> w.contains("R4.d"));
    } finally {
      serializer().setDateImplementation(LocalDate.class);
    }
  }

  @Test
  void aSchemaWithoutADatePatternWarnsNothingWhateverTheClass() {
    database.command("sql", "CREATE VERTEX TYPE R5");
    database.command("sql", "CREATE PROPERTY R5.d DATE");
    database.command("sql", "CREATE PROPERTY R5.s STRING (regexp '[a-z]+')");

    final List<String> warnings = WarningCapture.captureWarnings(() -> {
      serializer().setDateImplementation(Date.class);
      serializer().setDateTimeImplementation(Date.class);
    });
    try {
      assertThat(warnings).noneMatch(w -> w.contains("R5."));
    } finally {
      serializer().setDateImplementation(LocalDate.class);
      serializer().setDateTimeImplementation(LocalDateTime.class);
    }
  }

  @Test
  void settingTheClassAlreadyInForceChangesNothingAndWarnsNothing() {
    declareIsoPattern();
    final List<String> warnings = WarningCapture.captureWarnings(() -> serializer().setDateImplementation(LocalDate.class));
    assertThat(warnings).noneMatch(w -> w.contains("R1.d"));
  }

  /** The class has already changed when the listener runs, so a listener that fails must not reach the caller of the setter. */
  @Test
  void aListenerThatFailsDoesNotBreakTheSetter() {
    final List<String> warnings = WarningCapture.captureWarnings(() -> {
      serializer().setImplementationChangeListener(() -> {
        throw new IllegalStateException("a listener that fails");
      });
      try {
        serializer().setDateImplementation(Date.class);
        assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
      } finally {
        serializer().setImplementationChangeListener(null);
        serializer().setDateImplementation(LocalDate.class);
      }
    });

    assertThat(warnings).as("the failure is reported, not swallowed silently").anyMatch(w -> w.contains("date implementation"));
  }

  private void declareIsoPattern() {
    database.command("sql", "CREATE VERTEX TYPE R1");
    database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '" + ISO_DATE + "')");
    database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-01-15','yyyy-MM-dd')").close());
  }

  private BinarySerializer serializer() {
    return ((DatabaseInternal) database).getSerializer();
  }
}
