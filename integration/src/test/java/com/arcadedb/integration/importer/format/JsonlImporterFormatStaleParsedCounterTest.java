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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7313, finding 1: {@code JsonlImporterFormat.load()} took its periodic commit boundary from
 * {@code context.parsed.get() % COMMIT_EVERY}, and - unlike the seven sibling formats that zero the counter on
 * entry - never reset it. {@code ImporterContext} is created once per {@code Importer.load()} and shared by its
 * four {@code loadFromSource()} phases, so a jsonl phase that ran after another one entered with a stale offset
 * {@code k} and committed after {@code COMMIT_EVERY - (k % COMMIT_EVERY)} records instead of after
 * {@code COMMIT_EVERY}.
 * <p>
 * The reset itself has since moved OUT of the format and into {@code Importer.loadFromSource()}, where a format
 * cannot forget it (issue #7342), so the stale-offset case below drives two real phases of one
 * {@code Importer.load()} rather than handing a format a pre-loaded counter - which no longer expresses anything
 * the production path does.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class JsonlImporterFormatStaleParsedCounterTest {

  private static final String DB_PATH = "target/databases/jsonl-importer-stale-parsed-test";

  /**
   * Mirrors {@code JsonlImporterFormat.COMMIT_EVERY}, which is private: the boundary the tests below drive.
   */
  private static final int COMMIT_EVERY = 1_000;

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> database.getSchema().createDocumentType("Doc"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private static Parser jsonlParser(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("test.jsonl", new ByteArrayInputStream(bytes), bytes.length, false, null, null);
    return new Parser(source, 0);
  }

  private static String documentLine(final String typeName, final int bucketOffset) {
    return "{\"t\":\"d\",\"c\":{\"t\":\"" + typeName + "\",\"r\":\"#1:" + bucketOffset + "\",\"p\":{\"n\":\"v" + bucketOffset
        + "\"}}}";
  }

  /**
   * The row that makes {@code loadDocument} throw - the target type does not exist, so {@code newDocument()}
   * fails - which in the default "abort" mode rolls the in-flight batch back. Only records an intermediate
   * commit already made durable survive it, so the count afterwards is a direct read-out of where the boundary
   * fired.
   */
  private static String rowThatAbortsTheImport(final int bucketOffset) {
    return documentLine("TypeThatDoesNotExist", bucketOffset);
  }

  private long countOfDoc() {
    return database.query("sql", "select count(*) as c from Doc").next().<Long>getProperty("c");
  }

  private ImporterSettings settings() {
    final ImporterSettings settings = new ImporterSettings();
    settings.documentTypeName = "Doc";
    return settings;
  }

  /**
   * The reported case, driven through the live CLI path: a first phase of {@code COMMIT_EVERY - 1} records leaves
   * 999 in the counter, so before the fix the very first record of the jsonl phase pushed it to 1,000 and tripped
   * the boundary. Two valid records then a failing one must leave nothing of the second phase durable, because it
   * never reached a thousand records of its own.
   */
  @Test
  void aCounterLeftBehindByAnEarlierPhaseDoesNotShiftTheCommitBoundary() throws Exception {
    final Path firstPhase = Path.of("target", "jsonl-stale-boundary-phase1.jsonl").toAbsolutePath();
    final Path secondPhase = Path.of("target", "jsonl-stale-boundary-phase2.jsonl").toAbsolutePath();
    Files.createDirectories(firstPhase.getParent());

    final StringBuilder first = new StringBuilder();
    for (int i = 0; i < COMMIT_EVERY - 1; i++)
      first.append(documentLine("Doc", i)).append('\n');
    Files.writeString(firstPhase, first.toString(), StandardCharsets.UTF_8);
    Files.writeString(secondPhase,
        documentLine("Doc", COMMIT_EVERY) + "\n" + documentLine("Doc", COMMIT_EVERY + 1) + "\n" + rowThatAbortsTheImport(
            COMMIT_EVERY + 2) + "\n", StandardCharsets.UTF_8);

    final String cliDbPath = "target/databases/jsonl-stale-boundary-cli";
    FileUtils.deleteRecursively(new File(cliDbPath));
    try (final Database cliDatabase = new DatabaseFactory(cliDbPath).create()) {
      cliDatabase.transaction(() -> cliDatabase.getSchema().createDocumentType("Doc"));
    }

    try {
      assertThatThrownBy(() -> new Importer(("-url file://" + firstPhase + " -documents file://" + secondPhase
          + " -database " + cliDbPath).split(" ")).load()).isInstanceOf(ImportException.class);

      try (final Database cliDatabase = new DatabaseFactory(cliDbPath).open()) {
        assertThat(cliDatabase.countType("Doc", true))
            .as("the jsonl phase's commit boundary is measured from its own first record, not from the 999 the "
                + "previous phase left behind: two records is far short of COMMIT_EVERY, so the rollback takes both "
                + "and only the first phase's records survive")
            .isEqualTo(COMMIT_EVERY - 1);
      }
    } finally {
      FileUtils.deleteRecursively(new File(cliDbPath));
      Files.deleteIfExists(firstPhase);
      Files.deleteIfExists(secondPhase);
    }
  }

  /**
   * The other half of the same assertion, so the fix cannot be "never commit mid-file": with no stale offset the
   * boundary must still fire at exactly COMMIT_EVERY records, making the first thousand durable before the
   * failing record rolls the rest back.
   */
  @Test
  void theBoundaryStillFiresAtCommitEveryRecords() throws Exception {
    final JsonlImporterFormat format = new JsonlImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    final StringBuilder jsonl = new StringBuilder();
    for (int i = 0; i < COMMIT_EVERY + 1; i++)
      jsonl.append(documentLine("Doc", i)).append('\n');
    jsonl.append(rowThatAbortsTheImport(COMMIT_EVERY + 1)).append('\n');

    assertThatThrownBy(() -> format.load(null, null, jsonlParser(jsonl.toString()), (DatabaseInternal) database, context, settings()))
        .isInstanceOf(ImportException.class);

    assertThat(countOfDoc())
        .as("the single boundary that this phase reached made exactly its first COMMIT_EVERY records durable; "
            + "the 1,001st was still in flight when the failing record rolled the batch back")
        .isEqualTo(COMMIT_EVERY);
  }

  /**
   * The live CLI path: two {@code loadFromSource()} phases of one {@code Importer.load()}, the second of which
   * is the jsonl source under test. {@code parsedRecords} in the returned report is the counter the commit
   * boundary is taken from, so a run that reports only the second phase's own rows is the observable proof the
   * reset happens where a user actually drives it.
   */
  @Test
  void theTwoPhaseCliRouteScopesTheCounterToTheJsonlPhase() throws Exception {
    final Path firstPhase = Path.of("target", "jsonl-stale-parsed-phase1.jsonl").toAbsolutePath();
    final Path secondPhase = Path.of("target", "jsonl-stale-parsed-phase2.jsonl").toAbsolutePath();
    Files.createDirectories(firstPhase.getParent());
    Files.writeString(firstPhase, documentLine("Doc", 0) + "\n" + documentLine("Doc", 1) + "\n" + documentLine("Doc", 2) + "\n",
        StandardCharsets.UTF_8);
    Files.writeString(secondPhase, documentLine("Doc", 3) + "\n" + documentLine("Doc", 4) + "\n", StandardCharsets.UTF_8);

    final String cliDbPath = "target/databases/jsonl-stale-parsed-cli";
    FileUtils.deleteRecursively(new File(cliDbPath));
    try (final Database cliDatabase = new DatabaseFactory(cliDbPath).create()) {
      cliDatabase.transaction(() -> cliDatabase.getSchema().createDocumentType("Doc"));
    }

    try {
      final Map<String, Object> report = new Importer(("-url file://" + firstPhase + " -documents file://" + secondPhase
          + " -database " + cliDbPath).split(" ")).load();

      assertThat(report.get("createdDocuments"))
          .as("the import must actually have run: three documents from the first phase and two from the second")
          .isEqualTo(5L);
      assertThat(report.get("parsedRecords"))
          .as("parsedRecords is what the IMPORT parsed: the first phase's three rows plus the jsonl phase's two "
              + "(issue #7342), while the per-phase counter the commit boundary is taken from starts from zero")
          .isEqualTo(5L);
    } finally {
      FileUtils.deleteRecursively(new File(cliDbPath));
      Files.deleteIfExists(firstPhase);
      Files.deleteIfExists(secondPhase);
    }
  }
}
