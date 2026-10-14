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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9976: a REGEXP on a DATE or DATETIME property is matched against the text of the class the database holds the value
 * as, which {@code arcadedb.dateImplementation} and {@code arcadedb.dateTimeImplementation} decide for the whole database.
 * Changing them re-checked nothing, so a pattern accepted for one class matched no value of the next and every later write of
 * the property failed. The change is now refused naming the property, and so is a change of {@code arcadedb.dateFormat} or
 * {@code arcadedb.dateTimeFormat} that would leave a MIN or MAX of a date property unreadable, which every write reads
 * through them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9976DateImplementationChangeKeepsConstraintsTest extends TestHelper {

  @Test
  void theReproductionOfTheIssueIsRefusedAndLeavesTheSettingAsItWas() {
    database.command("sql", "CREATE VERTEX TYPE R1");
    database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')");
    database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-01-15','yyyy-MM-dd')").close());

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R1.d")
        .hasMessageContaining("'[0-9]{4}-[0-9]{2}-[0-9]{2}'").hasMessageContaining("held as Date");

    assertThat(serializer().getDateImplementation()).isEqualTo(LocalDate.class);
    database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')").close());

    // once the pattern is gone, the change goes through
    database.command("sql", "ALTER PROPERTY R1.d REGEXP null");
    database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'");
    assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
  }

  @Test
  void aPatternSomeValueOfTheNewClassMatchesDoesNotBlockTheChange() {
    database.command("sql", "CREATE VERTEX TYPE R2");
    database.command("sql", "CREATE PROPERTY R2.d DATE (regexp '.*20[0-9]{2}.*')");
    database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'");
    assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
    database.transaction(() -> database.command("sql", "INSERT INTO R2 SET d = date('2024-02-20','yyyy-MM-dd')").close());
  }

  @Test
  void theDatetimeImplementationIsCheckedForTheTypesItDecides() {
    database.command("sql", "CREATE VERTEX TYPE R3");
    database.command("sql", "CREATE PROPERTY R3.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
    // a microsecond datetime stays a LocalDateTime when DATETIME becomes a java.util.Date, so its pattern does not block
    database.command("sql", "CREATE PROPERTY R3.micros DATETIME_MICROS (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` 'java.util.Date'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R3.t").hasMessageNotContaining("R3.micros");
    assertThat(serializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);

    database.command("sql", "ALTER PROPERTY R3.t REGEXP null");
    database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` 'java.util.Date'");
    assertThat(serializer().getDateTimeImplementation()).isEqualTo(Date.class);
  }

  @Test
  void aCalendarHeldDateIsCheckedToo() {
    database.command("sql", "CREATE VERTEX TYPE R4");
    database.command("sql", "CREATE PROPERTY R4.d DATE (regexp '[0-9]{4}-.*')");
    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Calendar'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R4.d").hasMessageContaining("held as");
    assertThat(serializer().getDateImplementation()).isEqualTo(LocalDate.class);
  }

  @Test
  void aDateFormatChangeThatLeavesABoundUnreadableIsRefused() {
    database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd/MM/yyyy'");
    database.command("sql", "CREATE VERTEX TYPE R5");
    database.command("sql", "CREATE PROPERTY R5.d DATE (min '15/01/2024')");
    database.transaction(() -> database.command("sql", "INSERT INTO R5 SET d = '20/01/2024'").close());

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy-MM-dd'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R5.d")
        .hasMessageContaining("MIN '15/01/2024'");
    assertThat(database.getSchema().getDateFormat()).isEqualTo("dd/MM/yyyy");
    database.transaction(() -> database.command("sql", "INSERT INTO R5 SET d = '21/01/2024'").close());

    // a bound written in a form every format reads does not block it
    database.command("sql", "ALTER PROPERTY R5.d MIN '2024-01-15'");
    database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy-MM-dd'");
    assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MM-dd");
  }

  @Test
  void aSchemaWhoseFormatAndBoundsAlreadyDisagreeStillOpens() throws IOException {
    database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd/MM/yyyy'");
    database.command("sql", "CREATE VERTEX TYPE R6");
    database.command("sql", "CREATE PROPERTY R6.d DATE (min '15/01/2024')");
    database.close();

    // the format is changed behind the check's back: the load assigns it, it does not go through the setter that refuses
    final File schemaFile = new File(getDatabasePath(), "schema.json");
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    root.getJSONObject("settings").put("dateFormat", "yyyy-MM-dd");
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

    database = factory.open();
    assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MM-dd");
    assertThat(database.getSchema().getType("R6").getProperty("d").getMin()).isEqualTo("15/01/2024");
    // the remedy works, and the setter refuses nothing a bound it cannot read either predates
    database.command("sql", "ALTER PROPERTY R6.d MIN '2024-01-15'");
    database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy/MM/dd'");
  }

  private BinarySerializer serializer() {
    return ((DatabaseInternal) database).getSerializer();
  }
}
