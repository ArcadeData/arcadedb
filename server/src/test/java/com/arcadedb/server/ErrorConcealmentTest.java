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

import java.util.NoSuchElementException;
import java.util.logging.Level;

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
        "key [a]  2026-10-09 SEVERE forged next last");
    // the other control characters too: a terminal escape sequence, NUL, NEL and the Unicode line separators
    assertThat(ErrorConcealment.singleLine("a\u001B[2Jb\u0000c\u0085d e f\tg")).isEqualTo("a [2Jb c d e f g");
    final String plain = "no line break";
    assertThat(ErrorConcealment.singleLine(plain)).isSameAs(plain);
    assertThat(ErrorConcealment.singleLine(null)).isNull();
  }

  /**
   * Issue #10092: a failure with no message was told to the client as the word {@code null} (or as a JSON null where the
   * text is a member). The failure's type names it better than that, and the rule lives here, beside
   * {@link ErrorConcealment#clientMessage}, so the next surface does not re-derive it.
   */
  @Test
  void aFailureWithoutMessageIsDescribedByItsType() {
    assertThat(ErrorConcealment.describe(new NoSuchElementException())).isEqualTo("NoSuchElementException");
    assertThat(ErrorConcealment.describe(new IllegalStateException(""))).isEqualTo("IllegalStateException");
    assertThat(ErrorConcealment.describe(new IllegalStateException("   "))).isEqualTo("IllegalStateException");
    assertThat(ErrorConcealment.describe(new IllegalStateException("a real message"))).isEqualTo("a real message");
    assertThat(ErrorConcealment.describe(null)).isEqualTo("unknown error");
  }

  @Test
  void clientMessageNeverHandsOutANullText() {
    final NoSuchElementException e = new NoSuchElementException();
    assertThat(ErrorConcealment.clientMessage(false, this, "test", null, e)).isEqualTo("NoSuchElementException");
    assertThat(ErrorConcealment.clientMessage(true, this, "test", null, e)).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(ErrorConcealment.clientMessage(false, this, "test", "kept", e, Level.INFO)).isEqualTo("kept");
    // A blank message names the failure no better than none does, and describe() already reads it that way
    assertThat(ErrorConcealment.clientMessage(false, this, "test", "", e)).isEqualTo("NoSuchElementException");
    assertThat(ErrorConcealment.clientMessage(false, this, "test", "  ", e)).isEqualTo("NoSuchElementException");
  }
}
