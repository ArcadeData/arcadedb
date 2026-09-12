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
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.univocity.parsers.common.TextParsingException;

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
 * Issue #7288: {@code RDFImporterFormat.load()} took its mid-loop commit boundary from
 * {@code context.parsed}, an import-wide progress counter it incremented twice per row and never reset on
 * entry. The boundary was therefore always odd (so an even {@code -commitEvery} - 5000 is the default -
 * never matched it and the whole file imported in one transaction), an odd one matched at roughly half the
 * requested cadence, and an earlier phase of the same import shifted it by an arbitrary offset.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RDFImporterFormatCommitCadenceTest {

  private static final String DB_PATH = "target/databases/rdf-importer-commit-cadence-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
      database.getSchema().createEdgeType("Related");
      database.getSchema().createDocumentType("Marker");
    });
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

  private static Parser rdfParser(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("test.rdf", new ByteArrayInputStream(bytes), bytes.length, false, null, null);
    return new Parser(source, 0);
  }

  /**
   * The value that makes {@code csvParser.parseNext()} itself throw a {@link TextParsingException} on the row
   * that carries it, which is what induces the mid-file failure the durability assertions need: only edges an
   * intermediate commit already made durable can survive it.
   */
  private static final String ROW_THAT_ABORTS_THE_PARSE = "v99,rel,TOOLONGVALUEHERE";

  private ImporterSettings settingsWithCommitEvery(final int commitEvery) {
    final ImporterSettings settings = new ImporterSettings();
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Related";
    settings.typeIdProperty = "id";
    settings.commitEvery = commitEvery;
    settings.options.put("maxPropertySize", 5);
    return settings;
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  /**
   * Finding 1, the reported case: {@code commitEvery} is even, so the always-odd boundary never matched and
   * the mid-loop commit never ran. With six valid rows and a boundary of two, three intermediate commits owe
   * six durable edges before the seventh row aborts the parse.
   */
  @Test
  void anEvenCommitEveryStillCommitsMidFile() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel,v3
        v3,rel,v4
        v4,rel,v5
        v5,rel,v6
        v6,rel,v7
        """ + ROW_THAT_ABORTS_THE_PARSE + "\n");

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithCommitEvery(2)))
        .isInstanceOf(TextParsingException.class);

    assertThat(countOf("Related"))
        .as("an even -commitEvery must still commit mid-file: the three batches of two that completed are durable")
        .isEqualTo(6);
    assertThat(context.createdEdges.get())
        .as("the report must credit the import with exactly the edges the intermediate commits made durable")
        .isEqualTo(6);
  }

  /**
   * Finding 1, the other half: an odd {@code commitEvery} did match the odd boundary, but at roughly half the
   * requested row count - {@code parsed} reaching 3 on the FIRST data row, not the third. The cadence must be
   * measured in rows, so a boundary of three owes three durable edges, not one.
   */
  @Test
  void anOddCommitEveryCommitsOnTheRequestedCadenceNotHalfOfIt() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel,v3
        v3,rel,v4
        """ + ROW_THAT_ABORTS_THE_PARSE + "\n");

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithCommitEvery(3)))
        .isInstanceOf(TextParsingException.class);

    assertThat(countOf("Related"))
        .as("-commitEvery 3 must commit once every three rows, not once every one-and-a-half")
        .isEqualTo(3);
  }

  /**
   * {@code -commitEvery} is parsed with a bare {@code Integer.parseInt} and never range-checked
   * ({@code ImporterSettings:177}), so a non-positive value reaches the loop. It used to reach a
   * {@code % commitEvery}, which threw {@link ArithmeticException} on the first row; against the row count it
   * now means "commit every row", which is what {@code CSVImporterFormat.loadEdges()} has always done with the
   * same value and the same {@code txCount >= settings.commitEvery} shape. Pinned here because the change is a
   * consequence of the fix rather than its subject, and because a degenerate setting turning a crash into a
   * conservative cadence is the direction worth locking in.
   */
  @Test
  void aNonPositiveCommitEveryCommitsEveryRowInsteadOfThrowing() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel,v3
        v3,rel,v4
        """ + ROW_THAT_ABORTS_THE_PARSE + "\n");

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithCommitEvery(0)))
        .as("the failure that propagates is the malformed row, not an ArithmeticException from the boundary")
        .isInstanceOf(TextParsingException.class);

    assertThat(countOf("Related"))
        .as("-commitEvery 0 commits after every row, so all three rows before the malformed one are durable")
        .isEqualTo(3);
  }

  /**
   * Finding 2: the counter the cadence was taken from was incremented TWICE per row, so the value reaching the
   * modulo was never the row count the setting asks for.
   * <p>
   * The stale-offset half of that finding - {@code context.parsed} arriving carrying whatever the previous
   * {@code loadFromSource()} phase left in it - is no longer the format's to answer: the reset has moved into
   * {@code Importer.loadFromSource()}, where a format cannot forget it (issue #7342), and the two CLI-route tests
   * below drive it where a user does.
   */
  @Test
  void eachSourceRowIsCountedExactlyOnce() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel,v3
        v3,rel,v4
        v4,rel,v5
        v5,rel,v6
        v6,rel,v7
        """ + ROW_THAT_ABORTS_THE_PARSE + "\n");

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithCommitEvery(2)))
        .isInstanceOf(TextParsingException.class);

    assertThat(countOf("Related"))
        .as("the boundary is measured in rows: three batches of two completed before the malformed row")
        .isEqualTo(6);
    assertThat(context.parsed.get())
        .as("each source row is counted exactly once: the six data rows - the seventh throws inside parseNext(), "
            + "in the loop condition, so the loop body never counts it")
        .isEqualTo(6);
  }

  /**
   * The issue's "Related" bullet: the trailing {@code database.commit()} was unconditional, so a transaction
   * that predates the import - the one {@code callerTransactionActiveOnEntry} records - was committed on the
   * success path as a side effect of the import finishing. The mid-loop commit was already gated on
   * ownership; the trailing one was not. {@code CSVImporterFormat.loadDocuments()} is the sibling that gets
   * this right: rows stay staged in the caller's still-open transaction, and whether they become durable is
   * the caller's own decision.
   */
  @Test
  void aCallerOwnedTransactionIsNeverCommittedByTheImport() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    // Every row is valid, so the loop runs to its own trailing commit - the path under test.
    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel,v3
        v3,rel,v4
        v4,rel,v5
        """);

    format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithCommitEvery(2));

    assertThat(database.isTransactionActive())
        .as("a transaction that predates the import is the caller's to resolve, on the success path too")
        .isTrue();

    // The caller's decision, which the import must not have pre-empted.
    database.rollback();

    assertThat(countOf("Marker"))
        .as("the caller's own pending work must still be theirs to discard")
        .isZero();
    assertThat(countOf("Related"))
        .as("edges accumulated into a caller-owned transaction go with it when the caller rolls back")
        .isZero();
  }

  /**
   * The caller-owned path taken far enough to cross several commit boundaries and then fail, which is the
   * combination neither of the other caller-owned tests reaches:
   * {@code aCallerOwnedTransactionIsNeverCommittedByTheImport} runs to the end, and
   * {@code RDFImporterFormatTransactionLeakTest#aFailedRowLeavesTheCallersOwnTransactionUntouched} fails on the
   * second row, before any boundary. With {@code -commitEvery 2} and six rows there are three boundaries to get
   * wrong, and the transaction is not the import's: nothing may become durable, so a rollback by the caller has
   * to take every edge with it.
   */
  @Test
  void noCallerOwnedRowIsMadeDurableByABoundaryBeforeAMidFileFailure() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    database.begin();

    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel,v3
        v3,rel,v4
        v4,rel,v5
        v5,rel,v6
        v6,rel,v7
        """ + ROW_THAT_ABORTS_THE_PARSE + "\n");

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithCommitEvery(2)))
        .isInstanceOf(TextParsingException.class);

    assertThat(database.isTransactionActive())
        .as("six rows and three boundaries later, the caller's transaction is still theirs and still open")
        .isTrue();

    database.rollback();

    assertThat(countOf("Related"))
        .as("no boundary may have committed a caller-owned row behind its owner's back: the rollback takes all six")
        .isZero();
  }

  /**
   * The live CLI path, end to end, on the {@code -edges} route: {@code Importer.load()} -> {@code loadFromSource()} ->
   * {@code SourceDiscovery} sniffing the {@code <a>,<b>,<c>} triples back to {@code RDFImporterFormat} ->
   * {@code load()}. What the run reports as {@code parsedRecords} is the counter the commit boundary was
   * taken from, so a run that counts each row once is the observable proof the double increment is gone on
   * the path a user actually drives.
   * <p>
   * The triples are comma-delimited rather than in the canonical space-delimited N-Triples form because the
   * RDF branch of {@code SourceDiscovery} drops the delimiter it detects and the parse then falls back to a
   * comma, which makes a space-delimited source unimportable without an explicit {@code -delimiter} (issue
   * #7315). That is a separate defect from the commit cadence under test here.
   */
  @Test
  void theEdgesCliRouteCountsEachSourceRowExactlyOnce() throws Exception {
    assertCliRouteCountsEachSourceRowExactlyOnce("edges", "-vertexType Node -edgeType Related");
  }

  /**
   * The other of the two {@code loadFromSource()} calls that can dispatch to this format. {@code -url} is the
   * first of the four, and {@code Importer.load()} routes it by entity type: with {@code edgeType} among the
   * options and {@code settings.edges} unset it resolves to EDGE, which is the form an RDF source arrives in.
   * It reaches the same {@code load()}, so it is the same defect - but it is a different call site, and the
   * {@code -edges} test above does not drive it.
   */
  @Test
  void theUrlCliRouteCountsEachSourceRowExactlyOnce() throws Exception {
    assertCliRouteCountsEachSourceRowExactlyOnce("url", "-edgeType Related");
  }

  private void assertCliRouteCountsEachSourceRowExactlyOnce(final String sourceOption, final String typeOptions)
      throws Exception {
    final Path rdfFile = Path.of("target", "rdf-commit-cadence-cli-" + sourceOption + ".nt").toAbsolutePath();
    Files.createDirectories(rdfFile.getParent());
    Files.writeString(rdfFile, """
        <http://a/s1>,<http://a/rel>,<http://a/o1>
        <http://a/s2>,<http://a/rel>,<http://a/o2>
        <http://a/s3>,<http://a/rel>,<http://a/o3>
        <http://a/s4>,<http://a/rel>,<http://a/o4>
        <http://a/s5>,<http://a/rel>,<http://a/o5>
        """, StandardCharsets.UTF_8);

    final String cliDbPath = "target/databases/rdf-commit-cadence-cli-" + sourceOption;
    FileUtils.deleteRecursively(new File(cliDbPath));
    try (final Database cliDatabase = new DatabaseFactory(cliDbPath).create()) {
      cliDatabase.transaction(() -> {
        cliDatabase.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
        cliDatabase.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
        cliDatabase.getSchema().createEdgeType("Related");
      });
    }

    try {
      final Map<String, Object> report = new Importer(("-" + sourceOption + " file://" + rdfFile
          + " -database " + cliDbPath + " " + typeOptions + " -commitEvery 2").split(" ")).load();

      assertThat(report.get("createdEdges"))
          .as("the import must actually have run: all five triples become edges, because an RDF source has no "
              + "header row for the first one to be dropped as (issue #7345)")
          .isEqualTo(5L);
      assertThat(report.get("parsedRecords"))
          .as("the five source rows are counted once each, not twice")
          .isEqualTo(5L);
    } finally {
      FileUtils.deleteRecursively(new File(cliDbPath));
      Files.deleteIfExists(rdfFile);
    }
  }
}
