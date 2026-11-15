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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Engine code never submits work to the JDK common {@code ForkJoinPool}. The common pool is shared with user-supplied code
 * (Gremlin, Polyglot, custom SQL functions, the application around an embedded database) and with JDK internals, so
 * long-running engine work there starves both; the rule heads {@code QueryEngineManager}'s class javadoc and the
 * {@code engine-concurrency} skill lists the dedicated pools to submit to instead.
 * <p>
 * The census ranges over the authority: every call, or method reference, in the compiled classes of every module
 * ({@link CompiledClasses}) of a JDK API that runs its work on the common pool without naming it - a parallel stream, the
 * {@code Arrays.parallel*} family, a {@code CompletableFuture} async step with no executor, a {@code ForkJoinTask} forked
 * outside any pool - and {@code ForkJoinPool.commonPool()} itself. The overloads that take an {@code Executor} are the
 * dedicated-pool path and pass.
 */
class NoCommonForkJoinPoolCensusTest {
  private static final String      FORK_JOIN_POOL     = "java/util/concurrent/ForkJoinPool";
  private static final String      FORK_JOIN_TASK     = "java/util/concurrent/ForkJoinTask";
  private static final String      COMPLETABLE_FUTURE = "java/util/concurrent/CompletableFuture";
  private static final String      ARRAYS             = "java/util/Arrays";
  private static final String      EXECUTOR           = "Ljava/util/concurrent/Executor;";
  private static final Set<String> STREAMS            = Set.of("java/util/stream/BaseStream", "java/util/stream/Stream",
      "java/util/stream/IntStream", "java/util/stream/LongStream", "java/util/stream/DoubleStream");
  private static final Set<String> PARALLEL_ARRAYS    = Set.of("parallelSort", "parallelPrefix", "parallelSetAll");
  private static final Set<String> ASYNC_STEPS        = Set.of("supplyAsync", "runAsync", "thenApplyAsync", "thenAcceptAsync",
      "thenRunAsync", "thenComposeAsync", "thenCombineAsync", "thenAcceptBothAsync", "runAfterBothAsync", "applyToEitherAsync",
      "acceptEitherAsync", "runAfterEitherAsync", "whenCompleteAsync", "handleAsync", "exceptionallyAsync", "delayedExecutor");
  private static final Set<String> FORKS              = Set.of("fork", "invokeAll");

  private static final String BULK_SORT = "graph/GraphBatch#batchUpdateVertexHeadChunks";

  /**
   * The callers that pass: the one common-pool caller pending migration (tagged {@code NOTE (concurrency)} in source and listed
   * in the skill), and the parallel streams started from inside a task of a dedicated {@code ForkJoinPool}, which run on that
   * pool rather than on the common one - a shape the bytecode of the call cannot tell from the other.
   */
  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.of(//
      BULK_SORT,
      "Arrays.parallelSort of a bulk-load key array; a foreground operational task, not a per-query path, tagged NOTE "
          + "(concurrency) in source and listed in the engine-concurrency skill as the one common-pool caller pending migration",//
      "index/vector/LSMVectorIndex#buildGraphFromScratchExclusively",
      "the parallel insertion stream is started inside buildPool.submit(...), so it runs on the dedicated graph-build pool",//
      "index/vector/LSMVectorIndex#encodeVectors",
      "the parallel encoding stream is started inside pool.submit(...), so it runs on the dedicated pool it was handed"));

  @Test
  void noEngineClassSubmitsToTheCommonPool() throws Exception {
    final Map<String, Set<String>> callSites = callSitesOf(CompiledClasses.ofEveryModule());

    assertThat(callSites).as("the census must reach the reviewed common-pool caller, " + BULK_SORT).containsKey(BULK_SORT);
    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as(
            "engine code that runs work on the JDK common ForkJoinPool, against the rule at the head of QueryEngineManager's "
                + "javadoc: submit to a dedicated pool (QueryEngineManager.getExecutorService(), the index or time-series shard "
                + "executors), pass an Executor to the CompletableFuture step, or sort serially")
        .isEmpty();
    assertThat(CompiledClasses.stale(callSites, REVIEWED)).as(
        "REVIEWED entries that no longer reach the common pool: remove them, and their line in the engine-concurrency skill").isEmpty();
  }

  /** The control: the census still sees each shape of a submission, a method reference to one, and not the executor overload. */
  @Test
  void theCensusSeesEveryShapeOfASubmission() throws Exception {
    final Map<String, Set<String>> sites = callSitesOf(CompiledClasses.ofTheProbes());

    assertThat(sites).containsKeys("census/ShapeProbes$CommonPool#parallelStream", "census/ShapeProbes$CommonPool#parallelSort",
        "census/ShapeProbes$CommonPool#supplyAsyncWithoutAnExecutor", "census/ShapeProbes$CommonPool#methodReference");
    assertThat(sites).doesNotContainKeys("census/ShapeProbes$CommonPool#supplyAsyncWithAnExecutor",
        "census/ShapeProbes$CommonPool#namedInALiteral");
  }

  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) {
    return CompiledClasses.callSitesOf(classes, NoCommonForkJoinPoolCensusTest::submitsToTheCommonPool);
  }

  private static boolean submitsToTheCommonPool(final String owner, final String name, final String desc) {
    if (FORK_JOIN_POOL.equals(owner))
      return "commonPool".equals(name);
    // invokeinterface names the static receiver type, java/util/List as often as java/util/Collection: any owner
    if ("parallelStream".equals(name))
      return "()Ljava/util/stream/Stream;".equals(desc);
    if ("parallel".equals(name))
      return STREAMS.contains(owner);
    if (ARRAYS.equals(owner))
      return PARALLEL_ARRAYS.contains(name);
    if (COMPLETABLE_FUTURE.equals(owner))
      return ASYNC_STEPS.contains(name) && !desc.contains(EXECUTOR);
    if (FORK_JOIN_TASK.equals(owner))
      return FORKS.contains(name);
    return false;
  }
}
