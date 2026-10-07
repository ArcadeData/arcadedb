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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9314: a peer's refusal ending in a DEL character threw inside the detail formatter, because the slice was bounded by
 * the length of a different string than the one it cut, and the admin was told only "HTTP 500".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9314PeerDetailDelCharacterTest {
  private static String detail(final String text) {
    return SupportPeerQuery.detailOf(new JSONObject().put("detail", text).toString());
  }

  @Test
  void aTrailingDelKeepsTheText() {
    assertThat(detail("abc\u007f")).isEqualTo(": abc");
  }

  @Test
  void aLeadingDelKeepsTheText() {
    assertThat(detail("\u007fleading")).isEqualTo(": leading");
  }

  @Test
  void aDelInsideBecomesASpace() {
    assertThat(detail("msg with \u007f DEL inside")).isEqualTo(": msg with   DEL inside");
  }

  @Test
  void plainTextAndTheLengthCapAreUnchanged() {
    assertThat(detail("plain message")).isEqualTo(": plain message");
    assertThat(detail("x".repeat(500))).isEqualTo(": " + "x".repeat(300));
    assertThat(detail("\u007f" + "x".repeat(500) + "\u007f")).isEqualTo(": " + "x".repeat(300));
  }
}
