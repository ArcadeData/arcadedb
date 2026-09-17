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

import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code transactionMode: "none"} half of issue #7403 at the parsing layer: the mode and the presence of a
 * {@code transactionId} on the {@code start} frame have to agree, and neither half may be quietly coerced into
 * the other.
 */
class Issue7403InsertSessionOptionsTest {

  @Test
  void noneIsAcceptedOnlyWhenAnExternalTransactionIsNamed() {
    final InsertSessionOptions options =
        InsertSessionOptions.parse(new JSONObject().put("transactionMode", "none"), true);
    assertThat(options.transactionMode).isEqualTo(InsertSessionOptions.TransactionMode.NONE);
    assertThat(options.transactionModeName()).isEqualTo("none");
  }

  /**
   * The refusal still names this issue, which is what the pre-existing
   * {@code InsertSessionOptionsTest.transactionModeNoneIsRefusedWithThePointerToItsIssue} pins - but it now
   * names the field that resolves it instead of saying the feature does not exist.
   */
  @Test
  void noneWithoutAnExternalTransactionIsRefusedAndNamesTheFieldThatResolvesIt() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("transactionMode", "none"), false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("#7403")
        .hasMessageContaining("transactionId");
  }

  /**
   * A {@code transactionId} with any server-managed mode is refused rather than having one of the two win: the
   * session would either ignore the transaction the caller asked it to write into, or run under a commit policy
   * the caller did not ask for.
   */
  @Test
  void anExternalTransactionWithAServerManagedModeIsRefused() {
    for (final String mode : new String[] { "per_stream", "per_request", "per_batch", "per_row" })
      assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("transactionMode", mode), true))
          .as("mode '%s' with a transactionId", mode)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("transactionId");
  }

  /** The default is per_stream, so a transactionId with NO mode at all is the same contradiction. */
  @Test
  void anExternalTransactionWithNoModeAtAllIsRefused() {
    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject(), true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("transactionId");

    assertThatThrownBy(() -> InsertSessionOptions.parse(null, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("transactionId");
  }

  /** The single-argument form is the no-external-transaction case, which is what every other caller wants. */
  @Test
  void theSingleArgumentFormStillMeansNoExternalTransaction() {
    assertThat(InsertSessionOptions.parse(new JSONObject().put("transactionMode", "per_batch")).transactionMode)
        .isEqualTo(InsertSessionOptions.TransactionMode.PER_BATCH);

    assertThatThrownBy(() -> InsertSessionOptions.parse(new JSONObject().put("transactionMode", "none")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("#7403");
  }

  /** The conflict options are orthogonal to the transaction mode and keep working under {@code none}. */
  @Test
  void theConflictOptionsAreStillHonouredUnderNone() {
    final JSONObject json = new JSONObject()
        .put("transactionMode", "none")
        .put("conflictMode", "CONFLICT_UPDATE")
        .put("keyColumns", new com.arcadedb.serializer.json.JSONArray().put("id"));

    final InsertSessionOptions options = InsertSessionOptions.parse(json, true);
    assertThat(options.transactionMode).isEqualTo(InsertSessionOptions.TransactionMode.NONE);
    assertThat(options.conflictMode).isEqualTo(InsertSessionOptions.ConflictMode.UPDATE);
    assertThat(options.keyColumns).containsExactly("id");
  }
}
