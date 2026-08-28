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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.MultiIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/6815
 * <p>
 * {@code SelectExecutor.buildIterator()} installed the caller's {@code timeout()} on the source iterator only when
 * that source happened to be a {@link MultiIterator}. Two of the four sources it can return are
 * not: an index-answered plan yields a {@code MultiIndexCursor}, and a single-bucket {@code fromBuckets(...)} yields
 * the bucket iterator itself. Neither carries a deadline, so {@code timeout()} was a silent no-op for every indexed
 * query - by far the common shape - and for a one-bucket scan, on {@code documents()}/{@code vertices()}/
 * {@code edges()}, {@code count()} and {@code exists()} alike.
 * <p>
 * The deadline is now owned by the consumer ({@link SelectExecutor#checkForTimeout()}), which every source shape goes
 * through, so it no longer depends on which iterator the planner picked.
 * <p>
 * The assertions here are one-sided on purpose: each states that a bounded operation gave up, never that it survived
 * for at least some duration. A JVM stall can only make a deadline expire sooner, so it cannot turn a passing run red.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6815SelectTimeoutNotInstalledTest extends TestHelper {

  // BIG ENOUGH THAT A FULL SCAN CANNOT FINISH INSIDE THE 1 ms BUDGET count()/exists() ARE GIVEN BELOW. A SLOWER
  // MACHINE ONLY MAKES THAT MORE TRUE, WHICH IS THE DIRECTION THAT KEEPS THESE ASSERTIONS STABLE
  private static final int SCAN_ROWS = 20_000;
  private static final int ROWS      = 100;

  public Issue6815SelectTimeoutNotInstalledTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V")//
        .createProperty("id", Type.INTEGER)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

    database.getSchema().createDocumentType("Scan")//
        .createProperty("id", Type.INTEGER)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("V").set("id", i, "name", "John").save();
      for (int i = 0; i < SCAN_ROWS; i++)
        database.newDocument("Scan").set("id", i, "name", "John").save();
    });
  }

  /**
   * The issue's own repro: an indexed WHERE leaf makes the plan index-answered, so the source is a
   * {@code MultiIndexCursor} and the timeout used to be dropped on the floor.
   */
  @Test
  void indexAnsweredIterationThrowsOnTimeout() {
    assertThatThrownBy(() -> {
      final SelectIterator<Vertex> iter = database.select().fromType("V")//
          .where().property("id").ge().value(0)//
          .timeout(1, TimeUnit.MILLISECONDS, true).vertices();
      drainSlowly(iter);
    }).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  /**
   * Same plan shape with {@code exceptionOnTimeout = false}: the iteration must end early rather than throw, and the
   * result set must therefore be a strict prefix of the full one.
   */
  @Test
  void indexAnsweredIterationTruncatesWhenNotThrowing() {
    final SelectIterator<Vertex> iter = database.select().fromType("V")//
        .where().property("id").ge().value(0)//
        .timeout(1, TimeUnit.MILLISECONDS, false).vertices();

    final List<Vertex> got = drainSlowly(iter);
    assertThat(got.size()).isLessThan(ROWS);
  }

  /**
   * The second uninstrumented source: {@code fromBuckets(oneBucket)} returns {@code database.iterateBucket(...)},
   * a plain bucket iterator with no deadline of its own.
   */
  @Test
  void singleBucketIterationThrowsOnTimeout() {
    final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

    assertThatThrownBy(() -> {
      final SelectIterator<Vertex> iter = database.select().fromBuckets(bucket)//
          .where().property("name").eq().value("John")//
          .timeout(1, TimeUnit.MILLISECONDS, true).vertices();
      drainSlowly(iter);
    }).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  @Test
  void singleBucketIterationTruncatesWhenNotThrowing() {
    final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

    final SelectIterator<Vertex> iter = database.select().fromBuckets(bucket)//
        .where().property("name").eq().value("John")//
        .timeout(1, TimeUnit.MILLISECONDS, false).vertices();

    final List<Vertex> got = drainSlowly(iter);
    assertThat(got.size()).isLessThan(ROWS);
  }

  /**
   * {@code count()} never builds a {@link SelectIterator}: it drains {@code buildIterator()} in its own loop, so it
   * needs the deadline enforced there too.
   */
  @Test
  void countThrowsOnTimeout() {
    assertThatThrownBy(() -> database.select().fromType("Scan")//
        .where().property("id").ge().value(0)//
        .timeout(1, TimeUnit.MILLISECONDS, true).count())//
        .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  /**
   * The non-throwing counterpart returns the partial tally instead of the full one.
   */
  @Test
  void countTruncatesWhenNotThrowing() {
    final long count = database.select().fromType("Scan")//
        .where().property("id").ge().value(0)//
        .timeout(1, TimeUnit.MILLISECONDS, false).count();

    assertThat(count).isLessThan(SCAN_ROWS);
  }

  /**
   * {@code exists()} has the same shape as {@code count()}. A WHERE that matches nothing forces the full scan the
   * deadline has to cut short - a matching WHERE would return on the first record, long before any budget expires.
   */
  @Test
  void existsThrowsOnTimeout() {
    assertThatThrownBy(() -> database.select().fromType("Scan")//
        .where().property("name").eq().value("NoSuchName")//
        .timeout(1, TimeUnit.MILLISECONDS, true).exists())//
        .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  @Test
  void existsReturnsFalseWhenNotThrowing() {
    assertThat(database.select().fromType("Scan")//
        .where().property("name").eq().value("NoSuchName")//
        .timeout(1, TimeUnit.MILLISECONDS, false).exists()).isFalse();
  }

  /**
   * A select with no timeout must keep streaming the whole type: the deadline machinery has to stay inert when
   * {@code timeout()} was never called.
   */
  @Test
  void noTimeoutStillReturnsEverything() {
    assertThat(database.select().fromType("V")//
        .where().property("id").ge().value(0).vertices().toList()).hasSize(ROWS);
    assertThat(database.select().fromType("Scan")//
        .where().property("id").ge().value(0).count()).isEqualTo(SCAN_ROWS);
  }

  private static List<Vertex> drainSlowly(final SelectIterator<Vertex> iter) {
    final List<Vertex> got = new ArrayList<>();
    while (iter.hasNext()) {
      got.add(iter.next());
      try {
        Thread.sleep(2);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return got;
  }
}
