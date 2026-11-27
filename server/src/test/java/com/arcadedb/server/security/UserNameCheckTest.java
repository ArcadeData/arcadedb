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
package com.arcadedb.server.security;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two shapes of a user name: the one a user can be stored under and the one that reaches a peer as it was written.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UserNameCheckTest {

  @Test
  void anOrdinaryNameIsStorableAndCarried() {
    for (final String name : new String[] { "reader", "jane.doe@example.com", "John Smith", "o'brien", "a", "x y z" }) {
      assertThat(UserNameCheck.storedNameProblem(name)).as(name).isNull();
      assertThat(UserNameCheck.carriedNameProblem(name)).as(name).isNull();
      UserNameCheck.requireStorableName(name);
    }
  }

  @Test
  void whitespaceAtEitherEndIsRefused() {
    for (final String name : new String[] { "reader ", " reader", " ", "  reader", "reader\u00a0", "\u2003reader", "reader\u3000" }) {
      assertThat(UserNameCheck.storedNameProblem(name)).as(name).isEqualTo("starts or ends with whitespace");
      assertThatThrownBy(() -> UserNameCheck.requireStorableName(name)).isInstanceOf(IllegalArgumentException.class)
          .hasMessage("The user name starts or ends with whitespace");
    }
    assertThat(UserNameCheck.carriedNameProblem("reader ")).isEqualTo("starts or ends with whitespace");
    assertThat(UserNameCheck.carriedNameProblem(" reader")).isEqualTo("starts or ends with whitespace");
  }

  @Test
  void consecutiveWhitespaceIsRefused() {
    for (final String name : new String[] { "re  ader", "re \u00a0ader", "re\u2003 ader" })
      assertThat(UserNameCheck.storedNameProblem(name)).as(name).isEqualTo("contains consecutive whitespace characters");
    assertThat(UserNameCheck.carriedNameProblem("re  ader")).isEqualTo("contains consecutive whitespace characters");
  }

  @Test
  void aControlCharacterIsRefusedWhateverItsPosition() {
    for (final String name : new String[] { "re\tader", "re\nader", "re\rader", "re\u0000ader", "re\u007fader", "re\u0085ader", "\u0001reader" }) {
      assertThat(UserNameCheck.storedNameProblem(name)).as("stored").isEqualTo("contains a control character");
      assertThat(UserNameCheck.carriedNameProblem(name)).as("carried").isEqualTo("contains a control character");
    }
  }

  /** Storing is as open as it always was; only what a header cannot carry as written is closed to a peer request. */
  @Test
  void aNameOutsidePrintableAsciiIsStorableButNotCarried() {
    for (final String name : new String[] { "Zo\u00eb", "\u30e6\u30fc\u30b6\u30fc", "\u0414\u043c\u0438\u0442\u0440\u0438\u0439", "re\u20acader" }) {
      assertThat(UserNameCheck.storedNameProblem(name)).as(name).isNull();
      assertThat(UserNameCheck.carriedNameProblem(name)).as(name).isEqualTo("contains a character outside printable ASCII");
    }
  }

  /** A space the header parser leaves alone is not whitespace that a carried name has to worry about. */
  @Test
  void aCarriedNameTreatsOnlyTheSpaceAsWhitespace() {
    assertThat(UserNameCheck.carriedNameProblem("re\u00a0ader")).isEqualTo("contains a character outside printable ASCII");
    assertThat(UserNameCheck.storedNameProblem("re\u00a0ader")).isNull();
    assertThat(UserNameCheck.storedNameProblem("re\u00a0 ader")).isEqualTo("contains consecutive whitespace characters");
  }

  @Test
  void aMissingNameIsAProblem() {
    assertThat(UserNameCheck.storedNameProblem(null)).isEqualTo("is missing");
    assertThat(UserNameCheck.carriedNameProblem(null)).isEqualTo("is missing");
    assertThatThrownBy(() -> UserNameCheck.requireStorableName(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theRefusalNeverQuotesTheName() {
    assertThatThrownBy(() -> UserNameCheck.requireStorableName("secret\u0000name")).hasMessageNotContaining("secret");
  }
}
