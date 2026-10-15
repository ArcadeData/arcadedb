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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10067: the bounded, primitive log of the schema changes a node applied. It counts the changes in a stretch of the log, and
 * refuses to count a stretch it can no longer vouch for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067SchemaChangeLogTest {

  @Test
  void anEmptyLogCountsNothing() {
    assertThat(new SchemaChangeLog(4).countBetween(0L, 100L)).isZero();
  }

  @Test
  void aStretchIsOpenAtItsStartAndClosedAtItsEnd() {
    final SchemaChangeLog log = new SchemaChangeLog(8);
    log.add(10L);
    log.add(20L);
    log.add(30L);

    assertThat(log.countBetween(5L, 25L)).isEqualTo(2);
    assertThat(log.countBetween(10L, 30L)).as("(10, 30]").isEqualTo(2);
    assertThat(log.countBetween(30L, 100L)).isZero();
    assertThat(log.countBetween(0L, 100L)).isEqualTo(3);
    assertThat(log.countBetween(11L, 19L)).isZero();
  }

  @Test
  void aChangeAppliedOutOfOrderOrTwiceIsKeptSortedAndCountedOnce() {
    final SchemaChangeLog log = new SchemaChangeLog(8);
    log.add(30L);
    log.add(10L);
    log.add(20L);
    log.add(20L);
    log.add(10L);

    assertThat(log.countBetween(0L, 100L)).isEqualTo(3);
    assertThat(log.countBetween(10L, 20L)).isEqualTo(1);
  }

  /** Full: the oldest makes room, and a stretch that reaches back past what is left is not vouched for. */
  @Test
  void aFullLogDropsItsOldestAndRefusesAStretchItNoLongerCovers() {
    final SchemaChangeLog log = new SchemaChangeLog(4);
    for (long index = 1; index <= 4; index++)
      log.add(index * 10);
    assertThat(log.countBetween(0L, 100L)).as("full, and the oldest (10) is after 0: it may have lost older ones").isEqualTo(Integer.MAX_VALUE);
    assertThat(log.countBetween(10L, 100L)).as("starts at the oldest it holds: exact").isEqualTo(3);

    log.add(50L);
    assertThat(log.countBetween(10L, 100L)).as("10 was dropped: a stretch from it may have lost one in between").isEqualTo(Integer.MAX_VALUE);
    assertThat(log.countBetween(20L, 100L)).as("starts at the oldest it holds now").isEqualTo(3);
    assertThat(log.countBetween(30L, 100L)).isEqualTo(2);
  }

  /** A change between two kept ones, when full, drops the oldest and lands in its place. */
  @Test
  void aChangeInTheMiddleOfAFullLogKeepsTheOrder() {
    final SchemaChangeLog log = new SchemaChangeLog(4);
    log.add(10L);
    log.add(20L);
    log.add(40L);
    log.add(50L);

    log.add(30L);

    assertThat(log.countBetween(20L, 100L)).isEqualTo(3);
    assertThat(log.countBetween(20L, 40L)).as("(20, 40]").isEqualTo(2);
    assertThat(log.countBetween(10L, 100L)).as("the oldest (10) was dropped to make room").isEqualTo(Integer.MAX_VALUE);
  }

  /** Older than everything a full log holds: nothing to make room with, and a stretch that includes it is refused anyway. */
  @Test
  void aChangeOlderThanEverythingAFullLogHoldsIsNotKeptAndItsStretchIsRefused() {
    final SchemaChangeLog log = new SchemaChangeLog(3);
    log.add(40L);
    log.add(50L);
    log.add(60L);

    log.add(5L);

    assertThat(log.countBetween(40L, 100L)).isEqualTo(2);
    assertThat(log.countBetween(0L, 100L)).isEqualTo(Integer.MAX_VALUE);
  }
}
