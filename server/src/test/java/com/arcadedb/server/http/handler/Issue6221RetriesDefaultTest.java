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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The second half of issue #6221: the attempt count an auto-committed HTTP request runs under defaulted to a
 * hard-coded 1, so nothing retried unless the client asked for retries it had no reason to know existed.
 * <p>
 * It was dead code before #6201 - the auto-commit wrapper turned every exception into a
 * {@code TransactionException}, which matched neither arm of {@code LocalDatabase.transaction}'s
 * {@code catch (NeedRetryException | DuplicatedKeyException)} - and became live with it: an MVCC conflict that a
 * second attempt would have committed was answered 503 on the first. The default now follows
 * {@link GlobalConfiguration#TX_RETRIES}, which is where every other entry point takes its attempt count from and
 * is a knob an operator can already turn per database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6221RetriesDefaultTest {

  /** No {@code retries} in the payload: the configured value applies, not a hard-coded 1. */
  @Test
  void theDefaultFollowsTheConfiguredTransactionRetries() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_RETRIES, 7);

    assertThat(DatabaseAbstractHandler.resolveRetries(null, configuration)).isEqualTo(7);
    assertThat(DatabaseAbstractHandler.resolveRetries(new JSONObject(), configuration)).isEqualTo(7);
    assertThat(DatabaseAbstractHandler.resolveRetries(new JSONObject().put("retries", JSONObject.NULL), configuration))
        .as("an explicit null is not a value: it is the absence of one")
        .isEqualTo(7);
  }

  /** The request still wins over the configuration, including when it asks for no retry at all. */
  @Test
  void anExplicitRequestValueWinsOverTheConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_RETRIES, 7);

    assertThat(DatabaseAbstractHandler.resolveRetries(new JSONObject().put("retries", 2), configuration)).isEqualTo(2);
    assertThat(DatabaseAbstractHandler.resolveRetries(new JSONObject().put("retries", 1), configuration))
        .as("a client that wants a single attempt asks for one, and still gets one")
        .isEqualTo(1);
  }

  /**
   * An operator who turns retries off gets a single attempt everywhere, this endpoint included: the engine floors
   * an attempt count below 1 at one attempt, so 0 is not a request for zero work.
   */
  @Test
  void retriesTurnedOffLeavesASingleAttempt() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_RETRIES, 0);

    assertThat(DatabaseAbstractHandler.resolveRetries(null, configuration)).isZero();
  }
}
