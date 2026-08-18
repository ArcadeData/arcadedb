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
package com.arcadedb.console;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of the `&lt;key&gt;=&lt;value&gt;` rule shared by the `-D` command line arguments (issue #5928) and by the
 * SET command (issue #6392), which used to disagree on it. The two call sites are covered end to end by {@link ConsoleTest},
 * but they read the result differently, so the rule itself is worth stating once on its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ConsoleSplitKeyValueTest {
  @Test
  void theValueIsEverythingAfterTheFirstSeparator() {
    assertThat(Console.splitKeyValue("a=b")).containsExactly("a", "b");
    assertThat(Console.splitKeyValue("a=b=c")).containsExactly("a", "b=c");
    assertThat(Console.splitKeyValue("a===")).containsExactly("a", "==");
  }

  @Test
  void anEmptyValueIsAValue() {
    assertThat(Console.splitKeyValue("a=")).containsExactly("a", "");
  }

  @Test
  void anEmptyKeyIsReturnedAsSuchForTheCallerToReject() {
    assertThat(Console.splitKeyValue("=b")).containsExactly("", "b");
    assertThat(Console.splitKeyValue("=")).containsExactly("", "");
  }

  /**
   * No separator at all is not an empty value: the caller decides, because the command line reads it as one while SET rejects it.
   */
  @Test
  void noSeparatorLeavesTheChoiceToTheCaller() {
    assertThat(Console.splitKeyValue("a")).isNull();
    assertThat(Console.splitKeyValue("")).isNull();
  }

  /**
   * The blanks around the separator belong to the caller too: SET trims them, the `-D` arguments keep them.
   */
  @Test
  void surroundingBlanksArePreserved() {
    assertThat(Console.splitKeyValue(" a = b ")).containsExactly(" a ", " b ");
  }
}
