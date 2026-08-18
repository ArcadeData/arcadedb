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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the degree guard of issue #6335. A Cypher label write maps to an ArcadeDB type change, and a record's type
 * comes from the bucket it lives in, so there is no in-place retype: the vertex is rewritten under the new type and
 * every incident edge is re-created, which makes {@code SET n:Label} a write proportional to degree that also
 * changes the RID of the vertex and of all of its edges.
 * <p>
 * That cannot be fixed while a label is a type, so it is made visible instead: it is counted and logged above
 * {@code arcadedb.opencypher.labelWriteDegreeWarning}, and refused above
 * {@code arcadedb.opencypher.labelWriteDegreeLimit} - which is off by default, because the rewrite is slow rather
 * than wrong and refusing it has to be the operator's choice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelWriteDegreeGuardIssue6335Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-write-degree-6335");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {k:'h'})");
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "CREATE (:Leaf {k:'l" + i + "'})");
      database.command("opencypher", "MATCH (h:Hub), (l:Leaf) CREATE (h)-[:LINK]->(l)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void theLimitIsOffByDefaultSoALabelWriteOnAHubStillRuns() {
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT))
        .isZero();
    database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
    assertThat(labelsOfHub()).contains("Hub", "Popular");
    assertThat(degreeOfHub()).isEqualTo(5);
  }

  @Test
  void aLabelWriteAboveTheLimitIsRefusedBeforeAnythingMoves() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 4);
    try {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (h:Hub) SET h:Popular"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("labelWriteDegreeLimit");

      // Refused before any record moved: the hub still has its original type and all of its edges.
      assertThat(labelsOfHub()).containsExactly("Hub");
      assertThat(degreeOfHub()).isEqualTo(5);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }
  }

  @Test
  void aRemoveOfALabelIsGuardedTheSameWay() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 4);
    try {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (h:Hub) REMOVE h:Hub"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("labelWriteDegreeLimit");
      assertThat(labelsOfHub()).containsExactly("Hub");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }
  }

  @Test
  void aLabelWriteAtOrBelowTheLimitIsAllowed() {
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 5);
    try {
      database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
      assertThat(labelsOfHub()).contains("Hub", "Popular");
      assertThat(degreeOfHub()).isEqualTo(5);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }
  }

  @Test
  void crossingTheWarningThresholdReportsWithoutRefusing() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previous = LogManager.instance().getLogger();
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING, 1);
    LogManager.instance().setLogger(captured);
    try {
      database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
      assertThat(labelsOfHub()).contains("Hub", "Popular");
      assertThat(degreeOfHub()).isEqualTo(5);
    } finally {
      LogManager.instance().setLogger(previous);
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING, 10_000);
    }

    // "Silent" is the failure mode the whole issue is about, so the warning is asserted rather than assumed: without
    // this, deleting the log call would leave every other assertion here green.
    assertThat(captured.warnings)
        .as("a label write past the threshold has to say so")
        .anySatisfy(record -> {
          assertThat(record.message).contains("Cypher label write on");
          assertThat(record.args).contains("Hub", "Hub~Popular", 5L);
        });
  }

  @Test
  void aSelfLoopIsCountedOnceBecauseItIsMigratedOnce() {
    // countEdges(BOTH) sums the two edge lists, so a self-loop - present in both - counts twice, while the migration
    // re-creates it once. The guard has to refuse on the number it would report, or the limit and the warning
    // disagree about the same vertex: here 6 edges are migrated, and countEdges(BOTH) would have said 7.
    database.transaction(() -> database.command("opencypher", "MATCH (h:Hub) CREATE (h)-[:LINK]->(h)"));

    final CapturingLogger captured = new CapturingLogger();
    final Logger previous = LogManager.instance().getLogger();
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 6);
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING, 1);
    LogManager.instance().setLogger(captured);
    try {
      database.command("opencypher", "MATCH (h:Hub) SET h:Popular");
      assertThat(labelsOfHub()).contains("Hub", "Popular");
      assertThat(degreeOfHub()).isEqualTo(6); // 5 leaves + the self-loop, which relationship uniqueness yields once
    } finally {
      LogManager.instance().setLogger(previous);
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_WARNING, 10_000);
      database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT, 0);
    }

    assertThat(captured.warnings)
        .as("the warning counts the edges the write re-created, the self-loop once")
        .anySatisfy(record -> assertThat(record.args).contains(6L));
  }

  /** A log record as {@link Logger} receives it, kept only for what these assertions read. */
  private record LogRecord(Level level, String message, List<Object> args) {
  }

  /**
   * Captures WARNING records so a test can assert that something was reported. {@link LogManager#setLogger} documents
   * this as its intended use; the previous logger is always put back.
   * <p>
   * The logger is process-wide, as are the {@code GlobalConfiguration} values these tests set, so this class assumes
   * the sequential execution the module runs under - {@code forkCount=1}, {@code reuseForks=true}, and no JUnit
   * parallel configuration. If class-level parallelism is ever turned on for {@code engine}, this capture has to
   * move to a per-test logger rather than a swap of the global one.
   */
  private static final class CapturingLogger implements Logger {
    private final List<LogRecord> warnings = Collections.synchronizedList(new ArrayList<>());

    // Both overloads record directly: delegating from the fixed-arity one to the varargs one would resolve back to
    // itself (the fixed arity is the exact match) and recurse until the stack gives out.
    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      record(level, message, new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12,
          arg13, arg14, arg15, arg16, arg17 });
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      record(level, message, args);
    }

    /** Keeps the non-null arguments, with a RID as its string so an assertion can name it without a live record. */
    private void record(final Level level, final String message, final Object[] args) {
      if (level != Level.WARNING)
        return;
      final List<Object> present = new ArrayList<>();
      if (args != null)
        for (final Object arg : args)
          if (arg != null)
            present.add(arg instanceof RID rid ? rid.toString() : arg);
      warnings.add(new LogRecord(level, message, present));
    }

    @Override
    public void flush() {
    }
  }

  private List<String> labelsOfHub() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (h {k:'h'}) RETURN labels(h) AS l")) {
      assertThat(rs.hasNext()).isTrue();
      final List<String> labels = new ArrayList<>(rs.next().getProperty("l"));
      Collections.sort(labels);
      return labels;
    }
  }

  private long degreeOfHub() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (h {k:'h'})-[r]-() RETURN count(r) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
