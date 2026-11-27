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
package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionExecutionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Concurrency regressions of the polyglot function library (#7006).
 */
class PolyglotFunctionConcurrencyTest {
  /**
   * Regression for #7006: guards the fix for #7006 against reintroducing concurrent access to the shared GraalVM
   * {@code Context}. An earlier version of the fix replaced the old (broken) {@code synchronized(polyglotEngine)}
   * with a {@link ReentrantReadWriteLock}, which correctly stopped {@code reloadEngine()} from closing the engine
   * under an in-flight call, but let multiple callers hold the read lock and invoke
   * {@code callback.execute(polyglotEngine)} concurrently - something GraalVM's JS context does not support without
   * explicit multi-threaded access configuration (the same class of problem already fixed for
   * {@code PolyglotQueryEngine} under issue #6759). The final fix uses a plain mutex instead, so every caller of
   * {@link PolyglotFunctionLibraryDefinition#execute} stays fully serialized.
   * <p>
   * This test increments a JS-global counter with a non-atomic read-modify-write from many threads. If calls into
   * the shared context were ever allowed to run concurrently, the lost-update race would make the final count come
   * out lower than the number of calls with overwhelming probability; under full serialization it is always exact.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentCallSerialization extends TestHelper {

    @Test
    void concurrentCallersOfTheSameFunctionAreSerialized() throws Exception {
      database.command("sql", """
          DEFINE FUNCTION issue7006.bump "globalThis.counter7006 = (globalThis.counter7006 || 0) + 1; return globalThis.counter7006;" LANGUAGE js;
          """);
      final FunctionDefinition function = database.getSchema().getFunction("issue7006", "bump");

      final int threadCount     = 8;
      final int callsPerThread  = 200;
      final ExecutorService     pool  = Executors.newFixedThreadPool(threadCount);
      final CountDownLatch      ready = new CountDownLatch(threadCount);
      final CountDownLatch      go    = new CountDownLatch(1);
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final AtomicInteger       completed = new AtomicInteger();

      try {
        for (int t = 0; t < threadCount; t++) {
          pool.submit(() -> {
            try {
              ready.countDown();
              go.await(10, TimeUnit.SECONDS);
              for (int i = 0; i < callsPerThread; i++)
                function.execute();
              completed.incrementAndGet();
            } catch (final Throwable e) {
              error.set(e);
            }
          });
        }

        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();

        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(error.get()).isNull();
        assertThat(completed.get()).isEqualTo(threadCount);

        final Number finalCount = (Number) function.execute();
        assertThat(finalCount.intValue()).isEqualTo(threadCount * callsPerThread + 1);
      } finally {
        database.getSchema().unregisterFunctionLibrary("issue7006");
      }
    }
  }

  /**
   * Regression for #7006: {@code PolyglotFunctionLibraryDefinition.execute()} used to synchronize on the
   * {@code polyglotEngine} field itself, whose value {@code reloadEngine()} (triggered by {@code DEFINE FUNCTION} /
   * {@code registerFunction()}) reassigns without holding that same monitor. A call already in flight on the old
   * engine had it closed underneath it instead of either completing first or being serialized behind the swap.
   * <p>
   * Every call locks the engine it runs on (a plain mutex, not a read-write lock, since the shared GraalVM
   * {@code Context} does not support concurrent callers either - see {@link ConcurrentCallSerialization}). A
   * redefinition publishes a new engine and leaves the replaced one to be closed by the last call that holds it, so an
   * in-flight call always completes on the engine it started on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentRedefinition extends TestHelper {

    @Test
    void redefinitionWaitsForInFlightCallInsteadOfClosingItsEngine() throws Exception {
      final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "issue7006");
      database.getSchema()
          .registerFunctionLibrary(library.registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));

      final CountDownLatch callStarted = new CountDownLatch(1);
      final CountDownLatch releaseCall = new CountDownLatch(1);
      final AtomicReference<Throwable> callError = new AtomicReference<>();
      final AtomicReference<Object> callResult = new AtomicReference<>();

      final Thread caller = new Thread(() -> {
        try {
          callResult.set(library.execute(polyglotEngine -> {
            callStarted.countDown();
            try {
              assertThat(releaseCall.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            try {
              return polyglotEngine.eval("40 + 2").asInt();
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }));
        } catch (final Throwable t) {
          callError.set(t);
        }
      }, "issue7006-caller");

      final Thread redefiner = new Thread(() -> library.registerFunction(new JavascriptFunctionDefinition("avg", "return (a + b) / 2;", "a", "b")),
          "issue7006-redefiner");

      caller.start();
      assertThat(callStarted.await(10, TimeUnit.SECONDS)).isTrue();

      // The redefinition is started while the caller is inside execute(), holding the lock of its engine: reloadEngine()
      // must not close that engine underneath it.
      redefiner.start();
      // Best-effort nudge to increase the odds the redefiner has actually reached (and blocked on) the lock before
      // we release the call; not required for correctness (the assertions below hold either way), just makes the
      // race window this test targets more likely to actually be exercised on a given run.
      Thread.sleep(200);
      releaseCall.countDown();

      caller.join(10_000);
      redefiner.join(10_000);

      assertThat(callError.get()).isNull();
      assertThat(callResult.get()).isEqualTo(42);
      assertThat(database.getSchema().getFunctionLibrary("issue7006").hasFunction("avg")).isTrue();
    }
  }

  /**
   * A definition added to a library while one of its calls is running completes without waiting for that call. The
   * running call keeps the engine it started on until it returns, and the next call sees the new definition.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DefinitionDuringCall extends TestHelper {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void definitionCompletesWhileACallOfTheSameLibraryIsRunning() throws Exception {
      database.getSchema().createDocumentType("Item");
      database.transaction(() -> database.newDocument("Item").set("name", "first").save());

      database.command("sql", """
          DEFINE FUNCTION calls.sum "return a + b;" PARAMETERS [a, b] LANGUAGE js;
          """);
      final JavascriptFunctionLibraryDefinition library = (JavascriptFunctionLibraryDefinition) database.getSchema()
          .getFunctionLibrary("calls");

      final CountDownLatch callStarted = new CountDownLatch(1);
      final CountDownLatch releaseCall = new CountDownLatch(1);
      final AtomicBoolean readInCall = new AtomicBoolean(true);
      final AtomicReference<Throwable> callError = new AtomicReference<>();
      final AtomicReference<Object> callResult = new AtomicReference<>();
      final AtomicReference<Throwable> definitionError = new AtomicReference<>();

      final Thread caller = new Thread(() -> {
        try {
          callResult.set(library.execute(polyglotEngine -> {
            callStarted.countDown();
            try {
              assertThat(releaseCall.await(30, TimeUnit.SECONDS)).isTrue();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            final long items = readInCall.get() ? database.query("sql", "select from Item").stream().count() : -1L;
            try {
              return polyglotEngine.eval("40 + " + items).asInt();
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }));
        } catch (final Throwable t) {
          callError.set(t);
        }
      }, "library-caller");

      final Thread definer = new Thread(() -> {
        try {
          database.command("sql", """
              DEFINE FUNCTION calls.avg "return (a + b) / 2;" PARAMETERS [a, b] LANGUAGE js;
              """);
        } catch (final Throwable t) {
          definitionError.set(t);
        }
      }, "library-definer");

      caller.start();
      assertThat(callStarted.await(10, TimeUnit.SECONDS)).isTrue();

      definer.start();
      definer.join(10_000);
      final boolean definitionCompletedDuringCall = !definer.isAlive();

      if (definitionCompletedDuringCall)
        // THE DATABASE STAYS READABLE WHILE THE CALL IS STILL RUNNING
        assertThat(database.query("sql", "select from Item").stream().count()).isEqualTo(1L);
      else
        // LET THE CALL RETURN WITHOUT READING, SO THE TEST ENDS
        readInCall.set(false);

      releaseCall.countDown();
      caller.join(30_000);
      definer.join(30_000);

      assertThat(definitionCompletedDuringCall).as("the definition must complete while the call is running").isTrue();
      assertThat(caller.isAlive()).isFalse();
      assertThat(callError.get()).isNull();
      assertThat(definitionError.get()).isNull();
      // THE RUNNING CALL KEPT ITS ENGINE AND READ THE DATABASE
      assertThat(callResult.get()).isEqualTo(41);

      // THE NEXT CALLS SEE BOTH FUNCTIONS
      assertThat(database.getSchema().getFunction("calls", "avg").execute(4, 8)).isEqualTo(6);
      assertThat(database.getSchema().getFunction("calls", "sum").execute(4, 8)).isEqualTo(12);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aCallThatAddsAFunctionToItsOwnLibraryKeepsItsEngine() {
      final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "nested");
      database.getSchema()
          .registerFunctionLibrary(library.registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));

      final Object result = library.execute(polyglotEngine -> {
        library.registerFunction(new JavascriptFunctionDefinition("twice", "return a * 2;", "a"));
        try {
          return polyglotEngine.eval("40 + 2").asInt();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });

      assertThat(result).isEqualTo(42);
      assertThat(library.getFunction("twice").execute(21)).isEqualTo(42);
      assertThat(library.getFunction("sum").execute(40, 2)).isEqualTo(42);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aRefusedDefinitionLeavesTheOtherFunctionsCallable() {
      final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "refused");
      database.getSchema()
          .registerFunctionLibrary(library.registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));

      assertThatThrownBy(() -> library.registerFunction(new JavascriptFunctionDefinition("broken", "return (;", "a")))
          .isInstanceOf(FunctionExecutionException.class);

      assertThat(library.hasFunction("broken")).isFalse();
      assertThat(library.getFunction("sum").execute(40, 2)).isEqualTo(42);
      // A LATER DEFINITION STILL WORKS
      library.registerFunction(new JavascriptFunctionDefinition("twice", "return a * 2;", "a"));
      assertThat(library.getFunction("twice").execute(21)).isEqualTo(42);
      assertThat(library.getFunction("sum").execute(1, 2)).isEqualTo(3);
    }
  }
}
