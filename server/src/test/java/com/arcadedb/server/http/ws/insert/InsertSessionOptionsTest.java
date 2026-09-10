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
package com.arcadedb.server.http.ws.insert;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code options} of a {@code /ws} {@code start} frame (issue #7382), tested directly rather than through a
 * live server: every branch here is a decision about what the session will DO with the load, and one that
 * defaulted quietly where it should have refused would be answered with a perfectly successful-looking load run
 * under a policy the client did not ask for. It also stays cheap to extend as more of {@code InsertOptions}
 * arrives (issue #7404).
 */
class InsertSessionOptionsTest {

  @Test
  void noOptionsAtAllMeansOneTransactionForTheWholeSession() {
    final InsertSessionOptions options = InsertSessionOptions.parse(null);
    assertThat(options.transactionMode).isEqualTo(InsertSessionOptions.TransactionMode.PER_STREAM);
    assertThat(options.transactionModeName()).isEqualTo("per_stream");
    assertThat(options.targetType).isNull();
  }

  @Test
  void anEmptyOptionsObjectMeansTheSameThing() {
    assertThat(InsertSessionOptions.parse(new JSONObject()).transactionMode)
        .isEqualTo(InsertSessionOptions.TransactionMode.PER_STREAM);
  }

  @Test
  void theTargetTypeIsCarried() {
    assertThat(InsertSessionOptions.parse(new JSONObject().put("targetType", "Person")).targetType).isEqualTo("Person");
  }

  @Test
  void everyTransactionModeIsAcceptedInAnyCase() {
    assertThat(InsertSessionOptions.parse(new JSONObject().put("transactionMode", "per_batch")).transactionMode)
        .isEqualTo(InsertSessionOptions.TransactionMode.PER_BATCH);
    assertThat(InsertSessionOptions.parse(new JSONObject().put("transactionMode", "PER_ROW")).transactionMode)
        .isEqualTo(InsertSessionOptions.TransactionMode.PER_ROW);
    assertThat(InsertSessionOptions.parse(new JSONObject().put("transactionMode", "  Per_Stream  ")).transactionMode)
        .isEqualTo(InsertSessionOptions.TransactionMode.PER_STREAM);
  }

  /** gRPC's name for the same thing: a {@code /ws} session IS the request, so the two cannot differ here. */
  @Test
  void perRequestIsAnAliasOfPerStreamAndEchoesBackAsPerStream() {
    final InsertSessionOptions options = InsertSessionOptions.parse(new JSONObject().put("transactionMode", "per_request"));
    assertThat(options.transactionMode).isEqualTo(InsertSessionOptions.TransactionMode.PER_STREAM);
    assertThat(options.transactionModeName()).isEqualTo("per_stream");
  }

  @Test
  void aBlankTransactionModeFallsBackToTheDefaultRatherThanFailing() {
    assertThat(InsertSessionOptions.parse(new JSONObject().put("transactionMode", "   ")).transactionMode)
        .isEqualTo(InsertSessionOptions.TransactionMode.PER_STREAM);
  }

  @Test
  void anUnknownTransactionModeIsRefusedAndTheMessageListsTheRealOnes() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("transactionMode", "whenever")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("whenever")
        .hasMessageContaining("per_stream")
        .hasMessageContaining("per_batch")
        .hasMessageContaining("per_row");
  }

  /** {@code none} means "the caller manages the transaction", which a /ws session cannot name yet (#7403). */
  @Test
  void transactionModeNoneIsRefusedWithThePointerToItsIssue() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("transactionMode", "none")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("#7403");
  }

  /**
   * The four gRPC-only options are refused, not ignored: a loader ported from {@code InsertBidirectional} that
   * asked for upserts must not silently get plain inserts (#7404).
   */
  @Test
  void everyOptionThisServerDoesNotImplementIsRefusedByName() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("conflictMode", "update")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("conflictMode").hasMessageContaining("#7404");
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("keyColumns", new JSONArray().put("id"))))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyColumns");
    assertThatThrownBy(
        () -> InsertSessionOptions.parse(new JSONObject().put("updateColumnsOnConflict", new JSONArray().put("name"))))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("updateColumnsOnConflict");
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("validateOnly", true)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("validateOnly");
  }

  /** An explicit JSON null is "not set", not "set to something unsupported". */
  @Test
  void anExplicitNullForAnUnsupportedOptionIsNotTreatedAsAskingForIt() {
    final JSONObject options = new JSONObject().put("targetType", "Person");
    options.put("conflictMode", (Object) null);
    assertThat(InsertSessionOptions.parse(options).targetType).isEqualTo("Person");
  }
}
