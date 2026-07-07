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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for issue #5037 item 3: a malformed HA read-consistency bookmark header must be rejected with a
 * descriptive {@link IllegalArgumentException} (mapped by the handler to HTTP 400) instead of letting a raw
 * {@link NumberFormatException} bubble up as a 500.
 */
class DatabaseAbstractHandlerBookmarkParseTest {

  @Test
  void absentOrBlankReturnsNegativeOne() {
    assertThat(DatabaseAbstractHandler.parseReadBookmark(null)).isEqualTo(-1);
    assertThat(DatabaseAbstractHandler.parseReadBookmark("")).isEqualTo(-1);
    assertThat(DatabaseAbstractHandler.parseReadBookmark("   ")).isEqualTo(-1);
  }

  @Test
  void validNumericIsParsed() {
    assertThat(DatabaseAbstractHandler.parseReadBookmark("0")).isEqualTo(0);
    assertThat(DatabaseAbstractHandler.parseReadBookmark("123")).isEqualTo(123);
    assertThat(DatabaseAbstractHandler.parseReadBookmark("  456  ")).isEqualTo(456);
    assertThat(DatabaseAbstractHandler.parseReadBookmark("9223372036854775807")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void malformedThrowsIllegalArgument() {
    assertThatThrownBy(() -> DatabaseAbstractHandler.parseReadBookmark("abc"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> DatabaseAbstractHandler.parseReadBookmark("12x"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> DatabaseAbstractHandler.parseReadBookmark("1.5"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
