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
package com.arcadedb.test.support;

import com.arcadedb.exception.DatabaseNotFoundException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issues #11652 and #11653: the pure decisions of the load-test {@link DatabaseWrapper}, checked without Docker or a server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseWrapperHardeningTest {

  /** #11652: the retry loop read {@code getMessage().contains(...)} on an exception that carries no message. */
  @Test
  void anExceptionWithoutMessageIsAnUnexpectedSchemaFailureNotANullPointerException() {
    assertThatCode(() -> DatabaseWrapper.isRetryableSchemaFailure(new RuntimeException())).doesNotThrowAnyException();

    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new RuntimeException())).isFalse();
    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new IllegalStateException((String) null))).isFalse();
  }

  @Test
  void aFollowerThatHasNotOpenedTheDatabaseYetIsRetryable() {
    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new RuntimeException("Database 'graph' is not available"))).isTrue();
    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new DatabaseNotFoundException("Database 'graph' does not exist"))).isTrue();
  }

  /** The original expression read the message before it looked at the type, so a not-found answer with no message failed too. */
  @Test
  void aDatabaseNotFoundWithoutMessageIsStillRetryable() {
    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new DatabaseNotFoundException(null))).isTrue();
  }

  @Test
  void anyOtherFailureIsNotRetried() {
    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new RuntimeException("Type 'User' already exists"))).isFalse();
    assertThat(DatabaseWrapper.isRetryableSchemaFailure(new RuntimeException(""))).isFalse();
  }

  /** #11653: {@code count % pauseEvery} divided by zero for fewer than 10 operations, in a worker whose failure nobody reads. */
  @Test
  void theProgressPauseIntervalIsNeverZero() {
    for (int total = 0; total < 10; total++) {
      // the loops take the running count modulo this value
      assertThat(DatabaseWrapper.pauseInterval(total)).as("interval for %d operations", total).isGreaterThanOrEqualTo(1);
    }
  }

  @Test
  void theProgressPauseIntervalIsATenthOfTheTotal() {
    assertThat(DatabaseWrapper.pauseInterval(10)).isEqualTo(1);
    assertThat(DatabaseWrapper.pauseInterval(100)).isEqualTo(10);
    assertThat(DatabaseWrapper.pauseInterval(500)).isEqualTo(50);
    assertThat(DatabaseWrapper.pauseInterval(1_000_000)).isEqualTo(100_000);
  }
}
