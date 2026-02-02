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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class CodeUtilsTest {

  @Test
  void executeIgnoringExceptionsRunsCallback() {
    final AtomicBoolean executed = new AtomicBoolean(false);

    CodeUtils.executeIgnoringExceptions(() -> executed.set(true), "test", false);

    assertThat(executed.get()).isTrue();
  }

  @Test
  void executeIgnoringExceptionsCatchesException() {
    final AtomicBoolean executed = new AtomicBoolean(false);

    CodeUtils.executeIgnoringExceptions(() -> {
      executed.set(true);
      throw new RuntimeException("Test exception");
    }, "test", false);

    assertThat(executed.get()).isTrue();
  }

  @Test
  void executeIgnoringExceptionsSimpleFormRunsCallback() {
    final AtomicBoolean executed = new AtomicBoolean(false);

    CodeUtils.executeIgnoringExceptions(() -> executed.set(true));

    assertThat(executed.get()).isTrue();
  }

  @Test
  void executeIgnoringExceptionsSimpleFormCatchesException() {
    final AtomicBoolean executed = new AtomicBoolean(false);

    CodeUtils.executeIgnoringExceptions(() -> {
      executed.set(true);
      throw new RuntimeException("Test exception");
    });

    assertThat(executed.get()).isTrue();
  }

  @Test
  void compareWithBothNull() {
    assertThat(CodeUtils.compare(null, null)).isTrue();
  }

  @Test
  void compareWithFirstNull() {
    assertThat(CodeUtils.compare(null, "value")).isFalse();
  }

  @Test
  void compareWithSecondNull() {
    assertThat(CodeUtils.compare("value", null)).isFalse();
  }

  @Test
  void compareWithEqualValues() {
    assertThat(CodeUtils.compare("abc", "abc")).isTrue();
  }

  @Test
  void compareWithDifferentValues() {
    assertThat(CodeUtils.compare("abc", "def")).isFalse();
  }

  @Test
  void splitWithSingleDelimiter() {
    final List<String> result = CodeUtils.split("a,b,c", ',');
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void splitWithNoDelimiter() {
    final List<String> result = CodeUtils.split("abc", ',');
    assertThat(result).containsExactly("abc");
  }

  @Test
  void splitWithConsecutiveDelimiters() {
    final List<String> result = CodeUtils.split("a,,b", ',');
    assertThat(result).containsExactly("a", "", "b");
  }

  @Test
  void splitWithDelimiterAtEnd() {
    final List<String> result = CodeUtils.split("a,b,", ',');
    assertThat(result).containsExactly("a", "b");
  }

  @Test
  void splitWithDelimiterAtStart() {
    final List<String> result = CodeUtils.split(",a,b", ',');
    assertThat(result).containsExactly("", "a", "b");
  }

  @Test
  void splitWithLimit() {
    final List<String> result = CodeUtils.split("a,b,c,d,e", ',', 3);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void splitWithStringSeparator() {
    final List<String> result = CodeUtils.split("a::b::c", "::");
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void splitEmptyString() {
    final List<String> result = CodeUtils.split("", ",");
    assertThat(result).isEmpty();
  }

  @Test
  void splitNullString() {
    final List<String> result = CodeUtils.split(null, ",");
    assertThat(result).isEmpty();
  }

  @Test
  void sleepDoesNotThrowOnPositiveDelay() {
    final long start = System.currentTimeMillis();
    final boolean result = CodeUtils.sleep(50);
    final long elapsed = System.currentTimeMillis() - start;
    assertThat(result).isTrue();
    assertThat(elapsed).isGreaterThanOrEqualTo(40); // Allow some tolerance
  }

  @Test
  void sleepReturnsFalseOnZeroDelay() {
    final boolean result = CodeUtils.sleep(0);
    assertThat(result).isFalse();
  }

  @Test
  void sleepReturnsFalseOnNegativeDelay() {
    final boolean result = CodeUtils.sleep(-1);
    assertThat(result).isFalse();
  }

  @Test
  void getStackTraceReturnsString() {
    final String stackTrace = CodeUtils.getStackTrace();
    assertThat(stackTrace).isNotNull();
    assertThat(stackTrace).isNotEmpty();
    assertThat(stackTrace).contains("CodeUtilsTest");
  }
}
