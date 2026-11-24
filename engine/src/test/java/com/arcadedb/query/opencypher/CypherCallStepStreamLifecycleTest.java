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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@code CallStep} does with the stream a procedure answered when something goes wrong or nothing needs reading: a count that
 * never reads a row, a stream whose size cannot be taken (the step closes it, and a failing close does not hide the failure), the
 * rows of a procedure that are not {@code ResultInternal}, a procedure that declares no yield fields, a function that answers null,
 * and the PROFILE cost of a chained call. A probe procedure makes the rows as they are read, so a row that is never read never fails.
 */
class CypherCallStepStreamLifecycleTest extends TestHelper {
  private static final Pattern CALL_COST = Pattern.compile("\\+ CALL test\\.probe[^\\n]*\\(([\\d,]+)μs\\)");
  private static final Pattern EXECUTION_TIME = Pattern.compile("Execution Time: ([\\d,.]+) ms");

  private Probe read;
  private Probe write;

  @BeforeEach
  void registerProbes() {
    read = new Probe("test.probeRead", false);
    write = new Probe("test.probeWrite", true);
    CypherProcedureRegistry.registerOrReplace(read);
    CypherProcedureRegistry.registerOrReplace(write);
  }

  @AfterEach
  void unregisterProbes() {
    CypherProcedureRegistry.unregister("test.probeRead");
    CypherProcedureRegistry.unregister("test.probeWrite");
  }

  // ---- a count never reads the rows, every other consumer does -------------------------------------------------------------------

  @Test
  void aCountOfAReadProcedureTakesTheSizeOfTheStreamAndNeverReadsARow() {
    read.rows = 4;
    read.failAtRow = 2;

    assertThat(count("CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(4);
    assertThat(count("UNWIND [1, 2, 3] AS x CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(12);
    assertThat(read.closed.get()).isEqualTo(4);
  }

  @Test
  void whenTheRowsAreReadTheFailureOfARowSurfaces() {
    read.rows = 4;
    read.failAtRow = 2;

    // a YIELD WHERE needs the rows (the size would count the ones it drops)
    assertThatThrownBy(() -> count("CALL test.probeRead() YIELD a WHERE a >= 0 RETURN count(*) AS c")).hasMessageContaining("row 2 refused");
    // a stream of unknown size has no count to take
    read.sized = false;
    assertThatThrownBy(() -> count("CALL test.probeRead() YIELD a RETURN count(*) AS c")).hasMessageContaining("row 2 refused");
    // a column read is the rows
    read.sized = true;
    assertThatThrownBy(() -> count("CALL test.probeRead() YIELD a RETURN count(a) AS c")).hasMessageContaining("row 2 refused");

    // a write procedure runs for its effect: its rows are made
    write.rows = 4;
    write.failAtRow = 2;
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "CALL test.probeWrite() YIELD a RETURN count(*) AS c")) {
        rs.next();
      }
    }).hasStackTraceContaining("row 2 refused");
    assertThat(write.closed.get()).isEqualTo(1);
  }

  @Test
  void aStreamWithNoRowsIsNotCountedAsAKnownSizeOfZero() {
    read.rows = 0;
    read.failAtRow = 0;
    assertThat(count("CALL test.probeRead() YIELD a RETURN count(*) AS c")).isZero();
    assertThat(count("OPTIONAL CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(1);
  }

  // ---- a stream whose size cannot be taken ---------------------------------------------------------------------------------------

  @Test
  void aFailureOfTheSizeClosesTheStreamAndTheFailureOfTheCloseIsSuppressedIntoIt() {
    final SizeFails probe = new SizeFails("test.probeSize", new IllegalStateException("no size"));
    CypherProcedureRegistry.registerOrReplace(probe);
    try {
      assertThatThrownBy(() -> count("CALL test.probeSize() YIELD a RETURN count(*) AS c")).isInstanceOf(CommandExecutionException.class)
          .hasMessage("Error executing procedure: test.probeSize: no size")
          .satisfies(e -> assertThat(e.getCause().getSuppressed()).extracting(Throwable::getMessage).containsExactly("close failed"));
      assertThat(probe.closed.get()).isEqualTo(1);

      // OPTIONAL turns the failure into its one row of nulls, and the stream is closed all the same
      probe.closed.set(0);
      assertThat(count("OPTIONAL CALL test.probeSize() YIELD a RETURN count(*) AS c")).isEqualTo(1);
      assertThat(probe.closed.get()).isEqualTo(1);
    } finally {
      CypherProcedureRegistry.unregister("test.probeSize");
    }
  }

  @Test
  void aConflictOfTheSizeKeepsItsTypeAndClosesTheStreamEvenUnderOptional() {
    final SizeFails probe = new SizeFails("test.probeSize", new ConcurrentModificationException("conflict"));
    CypherProcedureRegistry.registerOrReplace(probe);
    try {
      assertThatThrownBy(() -> count("CALL test.probeSize() YIELD a RETURN count(*) AS c")).isInstanceOf(ConcurrentModificationException.class)
          .hasMessage("conflict").satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage).containsExactly("close failed"));
      assertThat(probe.closed.get()).isEqualTo(1);

      assertThatThrownBy(() -> count("OPTIONAL CALL test.probeSize() YIELD a RETURN count(*) AS c")).isInstanceOf(ConcurrentModificationException.class);
      assertThat(probe.closed.get()).isEqualTo(2);
    } finally {
      CypherProcedureRegistry.unregister("test.probeSize");
    }
  }

  @Test
  void aMalformedCallOfTheSizeIsNotHiddenByOptionalAndClosesTheStream() {
    final SizeFails probe = new SizeFails("test.probeSize", new CommandParsingException("bad argument"));
    CypherProcedureRegistry.registerOrReplace(probe);
    try {
      assertThatThrownBy(() -> count("OPTIONAL CALL test.probeSize() YIELD a RETURN count(*) AS c")).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("bad argument")
          .satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage).containsExactly("close failed"));
      assertThat(probe.closed.get()).isEqualTo(1);
    } finally {
      CypherProcedureRegistry.unregister("test.probeSize");
    }
  }

  @Test
  void aProcedureThatFailsBeforeItAnswersAStreamHasNothingToCloseNorToSuppress() {
    read.failOnExecute = new IllegalStateException("boom");
    assertThatThrownBy(() -> run("CALL test.probeRead() YIELD a RETURN a")).isInstanceOf(CommandExecutionException.class)
        .satisfies(e -> {
          assertThat(e.getCause()).isSameAs(read.failOnExecute);
          assertThat(e.getCause().getSuppressed()).isEmpty();
        });
    assertThat(read.closed.get()).isZero();
  }

  @Test
  void aStreamIsClosedAsSoonAsItsRowsRunOutNotWhenTheResultIsClosed() {
    read.rows = 2;
    final ResultSet rs = database.query("opencypher", "UNWIND [1, 2, 3] AS x CALL test.probeRead() YIELD a RETURN x, a");
    try {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).isEqualTo(6);
      assertThat(read.closed.get()).as("closed by the rows running out, the result is still open").isEqualTo(3);
      // exhausted: a further read is refused, it does not answer a null row
      assertThatThrownBy(rs::next).isInstanceOf(NoSuchElementException.class);
    } finally {
      rs.close();
    }
    assertThat(read.closed.get()).as("closing the result closes a stream once").isEqualTo(3);
  }

  @Test
  void theStreamsOfAnAbandonedResultAreClosedAndEveryFailureOfTheClosesIsReported() {
    read.rows = 3;
    read.failOnClose = true;
    final ResultSet rs = database.query("opencypher", "UNWIND [1, 2, 3] AS x CALL test.probeRead() YIELD a RETURN x, a LIMIT 1");
    assertThat(rs.hasNext()).isTrue();
    rs.next();

    // three calls ran, none read to the end: the first close that fails is the error, the other two are suppressed into it
    assertThatThrownBy(rs::close).isInstanceOf(IllegalStateException.class).hasMessageStartingWith("close failed")
        .satisfies(e -> assertThat(e.getSuppressed()).hasSize(2));
    assertThat(read.closed.get()).isEqualTo(3);
  }

  // ---- the rows and the fields a procedure answers ------------------------------------------------------------------------------

  @Test
  void aRowThatIsNotAResultInternalIsServedWithItsProperties() {
    final Probe plain = new Probe("test.probePlain", false);
    plain.plainRows = true;
    CypherProcedureRegistry.registerOrReplace(plain);
    try {
      final List<Result> rows = run("CALL test.probePlain() YIELD a, b RETURN a, b ORDER BY a");
      assertThat(rows).hasSize(3);
      assertThat(rows).extracting(r -> ((Number) r.getProperty("a")).intValue()).containsExactly(0, 1, 2);
      assertThat(rows).extracting(r -> (String) r.getProperty("b")).containsExactly("b0", "b1", "b2");
    } finally {
      CypherProcedureRegistry.unregister("test.probePlain");
    }
  }

  @Test
  void aProcedureThatDeclaresNoYieldFieldsIsToldToProduceNone() {
    for (final List<String> declared : new List[] { null, List.of() }) {
      final Probe open = new Probe("test.probeOpen", false);
      open.declared = declared;
      CypherProcedureRegistry.registerOrReplace(open);
      try {
        assertThat(run("CALL test.probeOpen()")).hasSize(3);
        assertThat(run("UNWIND [1, 2] AS x CALL test.probeOpen()")).hasSize(6);
        assertThat(open.requested).as("declared %s", declared).containsExactly(Set.of(), Set.of(), Set.of());
      } finally {
        CypherProcedureRegistry.unregister("test.probeOpen");
      }
    }
  }

  @Test
  void aFunctionThatAnswersNullAnswersNoRowAndOptionalAnswersItsRowOfNulls() {
    assertThat(run("CALL abs(null) YIELD value RETURN value")).isEmpty();

    final List<Result> optional = run("OPTIONAL CALL abs(null) YIELD value RETURN value");
    assertThat(optional).hasSize(1);
    assertThat((Object) optional.getFirst().getProperty("value")).isNull();

    final List<Result> chained = run("UNWIND [1, null, -3] AS x CALL abs(x) YIELD value RETURN x, value");
    assertThat(chained).hasSize(2);
    assertThat(chained).extracting(r -> ((Number) r.getProperty("value")).intValue()).containsExactly(1, 3);
  }

  @Test
  void aDefinedFunctionThatFailsIsWrappedAndOptionalAnswersARowOfNulls() {
    database.command("sql", "DEFINE FUNCTION lib.broken \"SELECT FROM NoSuchTypeAnywhere WHERE k = :x\" PARAMETERS [x] LANGUAGE sql");

    assertThatThrownBy(() -> runCommand("CALL lib.broken(1) YIELD v RETURN v")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Error executing custom function: lib.broken");

    final List<Result> optional = runCommand("OPTIONAL CALL lib.broken(1) YIELD v RETURN v");
    assertThat(optional).hasSize(1);
    assertThat((Object) optional.getFirst().getProperty("v")).isNull();
  }

  @Test
  void aFunctionOfTheRegistryThatFailsIsWrappedAndOptionalAnswersARowOfNulls() {
    assertThatThrownBy(() -> run("CALL apoc.util.validate(true, 'bad %s', ['x']) YIELD value RETURN value"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("Error executing function: util.validate").hasMessageContaining("bad x");

    final List<Result> optional = run("OPTIONAL CALL apoc.util.validate(true, 'bad %s', ['x']) YIELD value RETURN value");
    assertThat(optional).hasSize(1);
    assertThat((Object) optional.getFirst().getProperty("value")).isNull();
  }

  // ---- PROFILE ------------------------------------------------------------------------------------------------------------------

  @Test
  void profileOfAChainedCallPrintsTheTimeTheCallsTookWhichIsPartOfTheQuery() {
    read.rows = 2;
    database.begin();
    final String profile;
    try (final ResultSet rs = database.query("opencypher", "PROFILE UNWIND range(1, 200) AS x CALL test.probeRead() YIELD a RETURN x, a")) {
      while (rs.hasNext())
        rs.next();
      profile = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    } finally {
      database.commit();
    }

    final Matcher cost = CALL_COST.matcher(profile);
    assertThat(cost.find()).as(profile).isTrue();
    final long costMicros = Long.parseLong(cost.group(1).replace(",", ""));
    final Matcher total = EXECUTION_TIME.matcher(profile);
    assertThat(total.find()).as(profile).isTrue();
    final double totalMicros = Double.parseDouble(total.group(1).replace(",", "")) * 1_000;

    assertThat(costMicros).as("the 200 calls took time: %s", profile).isGreaterThan(0);
    assertThat((double) costMicros).as("the calls are part of the query: %s", profile).isLessThanOrEqualTo(totalMicros + 1);
  }

  // ---- helpers ----------------------------------------------------------------------------------------------------------------------------

  private List<Result> run(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<Result> runCommand(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private int count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final int c = ((Number) rs.next().getProperty("c")).intValue();
      assertThat(rs.hasNext()).isFalse();
      return c;
    }
  }

  /** Yields {@code rows} rows of a (the row number) and b (its text), made as they are read; records what the step asked of it. */
  private static class Probe implements CypherProcedure {
    private final String              name;
    private final boolean             write;
    private final List<Set<String>>   requested = new CopyOnWriteArrayList<>();
    final AtomicInteger           closed    = new AtomicInteger();
    private volatile int              rows      = 3;
    private volatile boolean          sized     = true;
    private volatile int              failAtRow = -1;
    private volatile boolean          plainRows = false;
    private volatile boolean          failOnClose = false;
    private volatile List<String>     declared  = List.of("a", "b");
    private volatile RuntimeException failOnExecute;

    private Probe(final String name, final boolean write) {
      this.name = name;
      this.write = write;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 0;
    }

    @Override
    public String getDescription() {
      return "Test probe";
    }

    @Override
    public List<String> getYieldFields() {
      return declared;
    }

    @Override
    public boolean isWriteProcedure() {
      return write;
    }

    private Result row(final int i, final int failAt) {
      if (i == failAt)
        throw new IllegalStateException("row " + i + " refused");
      final ResultInternal r = new ResultInternal();
      r.setProperty("a", i);
      r.setProperty("b", "b" + i);
      if (!plainRows)
        return r;
      // Not a ResultInternal: only the two reads a consumer of a Result makes are answered
      return (Result) Proxy.newProxyInstance(Result.class.getClassLoader(), new Class<?>[] { Result.class }, (proxy, method, args) -> switch (method.getName()) {
        case "getPropertyNames" -> r.getPropertyNames();
        case "getProperty" -> r.getProperty((String) args[0]);
        case "hasProperty" -> r.hasProperty((String) args[0]);
        case "toString" -> "plain row " + i;
        case "hashCode" -> System.identityHashCode(proxy);
        case "equals" -> proxy == args[0];
        default -> throw new UnsupportedOperationException(method.getName());
      });
    }

    @Override
    public Stream<Result> execute(final Object[] arguments, final Result inputRow, final CommandContext context) {
      return execute(arguments, inputRow, context, Set.of("a", "b"));
    }

    @Override
    public Stream<Result> execute(final Object[] arguments, final Result inputRow, final CommandContext context,
        final Set<String> requestedYieldFields) {
      requested.add(Set.copyOf(requestedYieldFields));
      if (failOnExecute != null)
        throw failOnExecute;
      final int failAt = failAtRow;
      final int count = rows;
      final Stream<Result> stream = sized ?
          IntStream.range(0, count).mapToObj(i -> row(i, failAt)) :
          Stream.iterate(0, i -> i + 1).limit(count).map(i -> row(i, failAt));
      return stream.onClose(() -> {
        final int n = closed.incrementAndGet();
        if (failOnClose)
          throw new IllegalStateException("close failed " + n);
      });
    }
  }

  /** A procedure whose stream claims a known size and fails when asked for it; closing the stream fails too. */
  private static final class SizeFails extends Probe {
    private final RuntimeException failure;

    private SizeFails(final String name, final RuntimeException failure) {
      super(name, false);
      this.failure = failure;
    }

    @Override
    public Stream<Result> execute(final Object[] arguments, final Result inputRow, final CommandContext context,
        final Set<String> requestedYieldFields) {
      final Spliterator<Result> sizeFails = new Spliterator<>() {
        @Override
        public boolean tryAdvance(final Consumer<? super Result> action) {
          return false;
        }

        @Override
        public Spliterator<Result> trySplit() {
          return null;
        }

        @Override
        public long estimateSize() {
          throw failure;
        }

        @Override
        public int characteristics() {
          return SIZED;
        }
      };
      return StreamSupport.stream(sizeFails, false).onClose(() -> {
        closed.incrementAndGet();
        throw new IllegalStateException("close failed");
      });
    }
  }
}
