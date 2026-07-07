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
package com.arcadedb.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #4931: {@code check()} computed record positions with {@code int * int} arithmetic
 * that overflows for buckets beyond 2^31 positions (a 64GB bucket at 64KB pages with the default 2048-slot
 * record table); with {@code fix=true} the overflowed-but-positive RID deleted an innocent record. The
 * arithmetic is extracted into {@link LocalBucket#recordPosition} so the overflow boundary is testable
 * without building a 64GB bucket.
 */
class LocalBucketRecordPositionTest {

  private static final int MAX_RECORDS_IN_PAGE = 2048;

  @Test
  void recordPositionDoesNotOverflowBeyondIntRange() {
    // First page past the int overflow boundary with a 2048-slot table: 1_048_576 * 2048 = 2^31.
    final int overflowPageId = 1_048_576;

    final long position = LocalBucket.recordPosition(overflowPageId, MAX_RECORDS_IN_PAGE, 5);
    assertThat(position).as("position must be computed in long arithmetic")
        .isEqualTo(2_147_483_648L + 5L);

    // The pre-fix int arithmetic wrapped negative here - and a few pages later wrapped back into small
    // POSITIVE values that check(fix=true) would happily delete.
    final int overflowedInt = overflowPageId * MAX_RECORDS_IN_PAGE + 5;
    assertThat((long) overflowedInt).as("the old int arithmetic overflowed at this boundary")
        .isNotEqualTo(position);

    // Deep into the bucket the wrapped value collides with a legitimate early RID: the innocent-record case.
    final long deepPosition = LocalBucket.recordPosition(2_097_152, MAX_RECORDS_IN_PAGE, 42);
    assertThat(deepPosition).isEqualTo(4_294_967_296L + 42L);
    assertThat(2_097_152 * MAX_RECORDS_IN_PAGE + 42).as("the old arithmetic aliased page 2_097_152 slot 42 onto position 42")
        .isEqualTo(42);
  }

  @Test
  void recordPositionMatchesIntArithmeticInsideIntRange() {
    // Inside the int range the widened arithmetic must be identical to the historical values, or existing
    // RIDs would change meaning.
    assertThat(LocalBucket.recordPosition(0, MAX_RECORDS_IN_PAGE, 0)).isEqualTo(0L);
    assertThat(LocalBucket.recordPosition(3, MAX_RECORDS_IN_PAGE, 17)).isEqualTo(3L * 2048 + 17);
    assertThat(LocalBucket.recordPosition(1_000_000, MAX_RECORDS_IN_PAGE, 2047))
        .isEqualTo(1_000_000L * 2048 + 2047);
  }
}
