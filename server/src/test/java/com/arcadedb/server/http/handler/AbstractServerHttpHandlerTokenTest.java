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

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractServerHttpHandlerTokenTest {

  private boolean callConstantTimeEquals(final String a, final String b) throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    return (boolean) m.invoke(null, a, b);
  }

  @Test
  void returnsTrueForEqualStrings() throws Exception {
    assertThat(callConstantTimeEquals("secret", "secret")).isTrue();
  }

  @Test
  void returnsFalseForDifferentStringsSameLength() throws Exception {
    assertThat(callConstantTimeEquals("secret", "secreT")).isFalse();
  }

  @Test
  void returnsFalseForDifferentLengths() throws Exception {
    assertThat(callConstantTimeEquals("secret", "secret2")).isFalse();
  }

  @Test
  void returnsFalseForNullFirst() throws Exception {
    assertThat(callConstantTimeEquals(null, "secret")).isFalse();
  }

  @Test
  void returnsFalseForNullSecond() throws Exception {
    assertThat(callConstantTimeEquals("secret", null)).isFalse();
  }

  @Test
  void returnsFalseForBothNull() throws Exception {
    assertThat(callConstantTimeEquals(null, null)).isFalse();
  }

  @Test
  void returnsFalseForEmptyVsNonEmpty() throws Exception {
    assertThat(callConstantTimeEquals("", "secret")).isFalse();
  }
}
