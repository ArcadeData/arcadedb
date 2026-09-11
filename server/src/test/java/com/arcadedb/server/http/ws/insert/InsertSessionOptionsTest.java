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
  void noConflictOptionsMeansErrorOnADuplicateWithNoKeyColumnsAndNoDryRun() {
    final InsertSessionOptions options = InsertSessionOptions.parse(new JSONObject().put("targetType", "Person"));
    assertThat(options.conflictMode).isEqualTo(InsertSessionOptions.ConflictMode.ERROR);
    assertThat(options.conflictModeName()).isEqualTo("error");
    assertThat(options.keyColumns).isEmpty();
    assertThat(options.updateColumnsOnConflict).isEmpty();
    assertThat(options.validateOnly).isFalse();
  }

  /** Issue #7404: the four gRPC conflict options are honoured, in either the short or the gRPC spelling. */
  @Test
  void everyConflictModeIsAcceptedInEitherSpelling() {
    final JSONArray key = new JSONArray().put("id");
    assertThat(InsertSessionOptions.parse(new JSONObject().put("conflictMode", "update").put("keyColumns", key)).conflictMode)
        .isEqualTo(InsertSessionOptions.ConflictMode.UPDATE);
    assertThat(InsertSessionOptions.parse(new JSONObject().put("conflictMode", "CONFLICT_UPDATE").put("keyColumns", key)).conflictMode)
        .isEqualTo(InsertSessionOptions.ConflictMode.UPDATE);
    assertThat(InsertSessionOptions.parse(new JSONObject().put("conflictMode", " Ignore ")).conflictMode)
        .isEqualTo(InsertSessionOptions.ConflictMode.IGNORE);
    assertThat(InsertSessionOptions.parse(new JSONObject().put("conflictMode", "conflict_abort")).conflictMode)
        .isEqualTo(InsertSessionOptions.ConflictMode.ABORT);
    assertThat(InsertSessionOptions.parse(new JSONObject().put("conflictMode", "error")).conflictMode)
        .isEqualTo(InsertSessionOptions.ConflictMode.ERROR);
  }

  @Test
  void keyColumnsUpdateColumnsAndValidateOnlyAreCarried() {
    final InsertSessionOptions options = InsertSessionOptions.parse(new JSONObject().put("conflictMode", "update")
        .put("keyColumns", new JSONArray().put("id").put("tenant"))
        .put("updateColumnsOnConflict", new JSONArray().put("name"))
        .put("validateOnly", true));
    assertThat(options.keyColumns).containsExactly("id", "tenant");
    assertThat(options.keyColumnSet).containsExactlyInAnyOrder("id", "tenant");
    assertThat(options.updateColumnsOnConflict).containsExactly("name");
    assertThat(options.validateOnly).isTrue();
  }

  @Test
  void anUnknownConflictModeIsRefusedAndTheMessageListsTheRealOnes() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("conflictMode", "merge")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("merge")
        .hasMessageContaining("update")
        .hasMessageContaining("ignore");
  }

  /**
   * gRPC accepts an update mode with no key columns and then reports every duplicate as a CONFLICT, because it
   * has nothing to match the existing record on. A session that can never perform the update it asked for is
   * refused up front instead.
   */
  @Test
  void updateWithoutKeyColumnsIsRefusedAtStart() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("conflictMode", "update")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("keyColumns");
  }

  @Test
  void aBlankOrNonStringKeyColumnIsRefusedByName() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("keyColumns", new JSONArray().put("id").put(" "))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("keyColumns");
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("updateColumnsOnConflict", new JSONArray().put(42))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("updateColumnsOnConflict");
  }

  @Test
  void anExplicitNullForAConflictOptionMeansItsDefault() {
    final JSONObject options = new JSONObject().put("targetType", "Person");
    options.put("conflictMode", (Object) null);
    options.put("keyColumns", (Object) null);
    final InsertSessionOptions parsed = InsertSessionOptions.parse(options);
    assertThat(parsed.targetType).isEqualTo("Person");
    assertThat(parsed.conflictMode).isEqualTo(InsertSessionOptions.ConflictMode.ERROR);
    assertThat(parsed.keyColumns).isEmpty();
  }
}
