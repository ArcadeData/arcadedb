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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11523: {@code EXPORT DATABASE ... WITH overwrite = false} overwrote an existing export file, exactly like
 * {@code overwrite = true}: the exporter set the flag for the name and never read the value. The same statement with no
 * {@code WITH} clause refused the file, so the one explicit way to say "do not overwrite" was the one that overwrote.
 * <p>
 * Every other setting the exporter did not know was kept in a map nothing read, so a misspelled one ({@code exludeTypes})
 * exported everything and answered OK. The command-line {@code -o} flag was dropped when it was the last argument.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11523ExportOverwriteSettingTest {
  private static final String DATABASE_PATH = "target/databases/Issue11523ExportOverwriteSettingTest";

  private String exportName;
  private File   exportFile;

  @BeforeEach
  void setUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    exportName = "issue11523-" + UUID.randomUUID() + ".jsonl.tgz";
    exportFile = new File("exports", exportName);
  }

  @AfterEach
  void tearDown() {
    exportFile.delete();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    TestHelper.checkActiveDatabases();
  }

  @Test
  void overwriteFalseRefusesAnExistingFileLikeNoSetting() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createPeople(database, "Ann", "Bob", "Cid");
      database.command("sql", "EXPORT DATABASE file://" + exportName);
      createPeople(database, "Dee");

      final byte[] firstExport = Files.readAllBytes(exportFile.toPath());
      assertThat(exportedContent()).contains("Cid").doesNotContain("Dee");

      for (final String with : new String[] { "", " WITH overwrite = false", " WITH `overwrite` = false", " WITH overwrite = 'false'",
          " WITH overwrite = FALSE", " WITH format = 'jsonl', overwrite = false" }) {
        assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportName + with))//
            .as("EXPORT DATABASE ...%s", with)//
            .hasStackTraceContaining("already exist");
        assertThat(Files.readAllBytes(exportFile.toPath())).as("file after EXPORT DATABASE ...%s", with).isEqualTo(firstExport);
      }

      database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH overwrite = true");
      assertThat(exportedContent()).contains("Cid").contains("Dee");
    }
  }

  @Test
  void overwriteFalseOnANewFileWritesIt() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createPeople(database, "Ann", "Bob", "Cid");
      database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH overwrite = false");
      assertThat(exportedContent()).contains("Ann").contains("Cid");
    }
  }

  @Test
  void overwriteValueThatIsNotABooleanIsRefused() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createPeople(database, "Ann");
      database.command("sql", "EXPORT DATABASE file://" + exportName);
      final byte[] firstExport = Files.readAllBytes(exportFile.toPath());
      createPeople(database, "Dee");

      for (final String value : new String[] { "'yes'", "1", "'no'", "''" })
        assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH overwrite = " + value))//
            .as("overwrite = %s", value)//
            .hasStackTraceContaining("Invalid value")//
            .hasStackTraceContaining("overwrite");

      assertThat(Files.readAllBytes(exportFile.toPath())).isEqualTo(firstExport);
    }
  }

  @Test
  void unknownSettingIsRefusedInsteadOfIgnored() {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createPeople(database, "Ann");

      assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH overwite = true"))//
          .hasStackTraceContaining("Unknown export setting 'overwite'")//
          .hasStackTraceContaining("Did you mean 'overwrite'?");
      assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH exludeTypes = 'Person'"))//
          .hasStackTraceContaining("Did you mean 'excludeTypes'?");
      // 'o' IS THE COMMAND-LINE FLAG: IN A WITH CLAUSE IT TOOK NO VALUE INTO ACCOUNT EITHER, SO IT IS NOT A SETTING THERE
      assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH o = false"))//
          .hasStackTraceContaining("Unknown export setting 'o'");

      assertThat(exportFile.exists()).isFalse();
    }
  }

  /**
   * The Python bindings drive the {@code Exporter} API directly and put {@code verboseLevel} in the settings map on every call:
   * an allow-list without it would refuse every default {@code export_database}. It is the level the console logger is built
   * from, and it now applies instead of being dropped.
   */
  @Test
  void theSettingsTheBindingsSendAreAcceptedAndApplied() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createPeople(database, "Ann", "Bob");

      final Exporter exporter = new Exporter(database, "exports/" + exportName);
      exporter.setFormat("jsonl");
      exporter.setOverwrite(true);
      exporter.setSettings(Map.of("includeTypes", "Person", "verboseLevel", "1"));
      assertThat(exporter.settings.verboseLevel).isEqualTo(1);

      exporter.exportDatabase();
      assertThat(exportedContent()).contains("Ann").contains("Bob");

      for (final String level : new String[] { "-1", "4", "loud", "" })
        assertThatThrownBy(() -> new Exporter(database, "exports/" + exportName).setSettings(Map.of("verboseLevel", level)))//
            .as("verboseLevel = '%s'", level)//
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid value '" + level + "' for -verboseLevel");
    }
  }

  @Test
  void theTargetCannotBeReplacedFromTheWithClause() {
    final File elsewhere = new File("target", "issue11523-" + UUID.randomUUID() + ".jsonl.tgz");
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createPeople(database, "Ann");

      for (final String name : new String[] { "file", "f" })
        assertThatThrownBy(
            () -> database.command("sql", "EXPORT DATABASE file://" + exportName + " WITH " + name + " = '" + elsewhere.getPath() + "'"))//
            .as("WITH %s = ...", name)//
            .hasStackTraceContaining("Unknown export setting '" + name + "'");

      assertThat(elsewhere.exists()).isFalse();
      assertThat(exportFile.exists()).isFalse();
    } finally {
      elsewhere.delete();
    }
  }

  @Test
  void commandLineOverwriteFlagAndSetting() {
    // A TRAILING -o WAS DROPPED: THE LOOP STOPPED ONE ARGUMENT BEFORE THE END, EXPECTING EVERY NAME TO HAVE A VALUE
    assertThat(parse("-format", "jsonl", "-f", "x.jsonl.tgz", "-o").overwriteFile).isTrue();
    assertThat(parse("-o", "-format", "jsonl", "-f", "x.jsonl.tgz").overwriteFile).isTrue();
    assertThat(parse("-format", "jsonl", "-f", "x.jsonl.tgz").overwriteFile).isFalse();
    assertThat(parse("-format", "jsonl", "-overwrite", "true").overwriteFile).isTrue();
    assertThat(parse("-format", "jsonl", "-overwrite", "false").overwriteFile).isFalse();

    final ExporterSettings settings = parse("-format", "JSONL", "-file", "y.jsonl.tgz", "-d", "target/db", "-includeTypes", "A,B");
    assertThat(settings.format).isEqualTo("jsonl");
    assertThat(settings.file).isEqualTo("y.jsonl.tgz");
    assertThat(settings.databaseURL).isEqualTo("target/db");
    assertThat(settings.includeTypes).containsExactlyInAnyOrder("A", "B");

    // A REPEATED ENTRY USED TO THROW "duplicate element", AND THE BLANK AFTER A COMMA WAS KEPT AS PART OF THE NAME
    assertThat(parse("-format", "jsonl", "-includeTypes", "A, B,A,").includeTypes).containsExactlyInAnyOrder("A", "B");
    assertThat(parse("-format", "jsonl", "-includeRecords", "#1:0 , #1:1").includeRecords).containsExactlyInAnyOrder("#1:0", "#1:1");

    assertThatThrownBy(() -> parse("-format", "jsonl", "-overwrite", "yes"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid value 'yes' for -overwrite");
    assertThatThrownBy(() -> parse("-format", "jsonl", "-f"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Missing value for -f");
    // AN OPTION IN THE VALUE POSITION IS THE NEXT OPTION, NOT A FILE NAMED '-o' OR A TYPE NAMED '-o'
    assertThatThrownBy(() -> parse("-format", "jsonl", "-f", "-o"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Missing value for -f");
    assertThatThrownBy(() -> parse("-format", "jsonl", "-includeTypes", "-o"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Missing value for -includeTypes");
    assertThatThrownBy(() -> parse("-f", "-format", "jsonl"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Missing value for -f");
    // A VALUE THAT MERELY BEGINS WITH '-' IS STILL A VALUE
    assertThat(parse("-format", "jsonl", "-f", "-backup.jsonl.tgz").file).isEqualTo("-backup.jsonl.tgz");

    assertThatThrownBy(() -> parse("-format", "jsonl", "-exludeTypes", "A"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Did you mean 'excludeTypes'?");
    assertThatThrownBy(() -> parse("-format", "jsonl", "x.jsonl.tgz"))//
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must begin with '-'");
  }

  private static ExporterSettings parse(final String... args) {
    final ExporterSettings settings = new ExporterSettings();
    settings.parseParameters(args);
    return settings;
  }

  private static void createPeople(final Database database, final String... names) {
    if (!database.getSchema().existsType("Person"))
      database.getSchema().createDocumentType("Person");
    database.transaction(() -> {
      for (final String name : names)
        database.newDocument("Person").set("name", name).save();
    });
  }

  private String exportedContent() throws IOException {
    try (final InputStream in = new GZIPInputStream(new FileInputStream(exportFile))) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
