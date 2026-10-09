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
package com.arcadedb.server;

import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8749: the shared decision of what a wire surface tells the client for an engine failure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ErrorConcealmentTest {
  @Test
  void productionConcealsAndDevelopmentKeepsTheText() {
    final DuplicatedKeyException e = new DuplicatedKeyException("User[email]", "[alice@example.com]", new RID(3, 7));
    assertThat(ErrorConcealment.clientMessage(true, this, "test", e.getMessage(), e)).isEqualTo(
        ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(ErrorConcealment.clientMessage(false, this, "test", e.getMessage(), e)).isEqualTo(e.getMessage());
    assertThat(ErrorConcealment.loggedClientMessage(true, "detail")).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(ErrorConcealment.loggedClientMessage(false, "detail")).isEqualTo("detail");
  }

  @Test
  void theLoggedTextIsOneLine() {
    // client data in a message must not be able to start a forged entry in a line-oriented log
    assertThat(ErrorConcealment.singleLine("key [a]\r\n2026-10-09 SEVERE forged\nnext\rlast")).isEqualTo(
        "key [a] 2026-10-09 SEVERE forged next last");
    final String plain = "no line break";
    assertThat(ErrorConcealment.singleLine(plain)).isSameAs(plain);
    assertThat(ErrorConcealment.singleLine(null)).isNull();
  }
}
