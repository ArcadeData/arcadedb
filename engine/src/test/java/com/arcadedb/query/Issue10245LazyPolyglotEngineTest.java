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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.polyglot.PolyglotEngineManager;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10245: the first SQL or openCypher statement of an embedded JVM built the GraalVM polyglot Engine (Truffle and
 * every GraalVM language on the classpath), because the registry of the query engines listed the polyglot languages in its
 * constructor. They are now listed the first time a language no other engine answers to is looked up.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10245LazyPolyglotEngineTest extends TestHelper {

  @Test
  void builtInLanguagesNeverListThePolyglotOnes() {
    final AtomicInteger listings = new AtomicInteger();
    final QueryEngineManager manager = new QueryEngineManager(() -> {
      listings.incrementAndGet();
      return List.of("js", "sql");
    });
    try {
      assertThat(manager.getEngine("sql", (DatabaseInternal) database)).isInstanceOf(SQLQueryEngine.class);
      assertThat(manager.getEngine("OpenCypher", (DatabaseInternal) database)).isNotNull();
      assertThat(manager.isLanguageRegistered("sqlscript")).isTrue();
      assertThat(listings.get()).as("no polyglot language was asked for").isZero();

      // the per-query check of the metrics never lists them: an invented language must not build the engine there
      assertThat(manager.isLanguageRegistered("lang12345")).isFalse();
      assertThat(manager.isLanguageRegistered("js")).isFalse();
      assertThat(listings.get()).isZero();

      // a lookup of a language no engine answers to lists them, once
      assertThatThrownBy(() -> manager.getEngine("lang12345", (DatabaseInternal) database)).isInstanceOf(IllegalArgumentException.class);
      assertThat(manager.isLanguageRegistered("js")).isTrue();
      assertThat(manager.isLanguageRegistered("lang12345")).isFalse();
      assertThat(listings.get()).isEqualTo(1);

      // a polyglot language named like a built-in engine does not take its place
      assertThat(manager.getEngine("sql", (DatabaseInternal) database)).isInstanceOf(SQLQueryEngine.class);
      assertThat(manager.getAvailableLanguages()).contains("sql", "sqlscript", "java", "opencypher", "cypher", "js")
          .doesNotHaveDuplicates();
      assertThat(listings.get()).isEqualTo(1);
    } finally {
      manager.close();
    }
  }

  @Test
  void aFailingListingLeavesTheBuiltInEnginesAndIsNotRetried() {
    final AtomicInteger listings = new AtomicInteger();
    final QueryEngineManager manager = new QueryEngineManager(() -> {
      listings.incrementAndGet();
      throw new IllegalStateException("no engine");
    });
    try {
      assertThatThrownBy(() -> manager.getEngine("js", (DatabaseInternal) database)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> manager.getEngine("js", (DatabaseInternal) database)).isInstanceOf(IllegalArgumentException.class);
      assertThat(manager.getEngine("sql", (DatabaseInternal) database)).isInstanceOf(SQLQueryEngine.class);
      assertThat(listings.get()).isEqualTo(1);
    } finally {
      manager.close();
    }
  }

  @Test
  void availableLanguagesListThePolyglotOnes() {
    final AtomicInteger listings = new AtomicInteger();
    final QueryEngineManager manager = new QueryEngineManager(() -> {
      listings.incrementAndGet();
      return List.of("js", "python");
    });
    try {
      assertThat(manager.isLanguageRegistered("PYTHON")).isFalse();
      assertThat(manager.getAvailableLanguages()).contains("js", "python", "sql");
      assertThat(manager.isLanguageRegistered("PYTHON")).isTrue();
      assertThat(listings.get()).isEqualTo(1);
    } finally {
      manager.close();
    }
  }

  /**
   * The real registry in a JVM of its own: in this one some other test may have built the Engine already. The child runs a
   * SQL and an openCypher statement and reports whether the Engine exists, then runs a script and reports again.
   */
  @Test
  void firstStatementsOfAFreshJvmDoNotBuildTheEngine() throws Exception {
    final String classpath = System.getProperty("java.class.path");
    assertThat(classpath).as("the forked JVM needs this JVM's classpath").isNotBlank();

    final Path directory = Files.createTempDirectory("issue10245");
    final List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    command.add("-cp");
    command.add(classpath);
    command.add(FirstStatementsProbe.class.getName());
    command.add(directory.resolve("db").toString());

    // stdout to a file, so a child that hangs without closing it cannot block the read past the wait below
    final File stdout = directory.resolve("stdout.txt").toFile();
    final Process process = new ProcessBuilder(command).redirectOutput(stdout).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    try {
      // A hang detector, not a latency bound
      final boolean exited = process.waitFor(120, TimeUnit.SECONDS);
      final String output = Files.readString(stdout.toPath(), StandardCharsets.US_ASCII).trim();
      assertThat(exited).as("the forked JVM must terminate; it printed: %s", output).isTrue();
      assertThat(process.exitValue()).as("the forked JVM failed; it printed: %s", output).isZero();
      assertThat(output).endsWith("afterStatements=false afterScript=true");
    } finally {
      process.destroyForcibly();
      FileUtils.deleteRecursively(directory.toFile());
    }
  }

  /** Child-JVM entry point: prints whether the shared polyglot Engine exists after the statements, and after a script. */
  public static class FirstStatementsProbe {
    public static void main(final String[] args) {
      final DatabaseFactory factory = new DatabaseFactory(args[0]);
      final Database db = factory.create();
      try {
        db.getSchema().createDocumentType("Doc");
        db.transaction(() -> db.newDocument("Doc").set("id", 1).save());
        try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM Doc")) {
          rs.next();
        }
        try (final ResultSet rs = db.query("opencypher", "MATCH (n) RETURN count(n) AS n")) {
          rs.next();
        }
        final boolean afterStatements = PolyglotEngineManager.getInstance().isSharedEngineCreated();
        try (final ResultSet rs = db.command("js", "1 + 1")) {
          rs.hasNext();
        }
        final boolean afterScript = PolyglotEngineManager.getInstance().isSharedEngineCreated();
        System.out.println("afterStatements=" + afterStatements + " afterScript=" + afterScript);
      } finally {
        db.drop();
        PolyglotEngineManager.getInstance().closeSharedEngine();
      }
      System.exit(0);
    }
  }
}
