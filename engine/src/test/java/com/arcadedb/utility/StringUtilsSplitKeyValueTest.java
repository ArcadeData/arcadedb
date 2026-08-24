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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of {@link StringUtils#splitKeyValue(String)}: split on the FIRST '=' only, so a value
 * containing further separators (a connection string, a base64 padding) is kept whole. The console's
 * {@code -D&lt;key&gt;=&lt;value&gt;} arguments and SET command (issue #6392), the Postgres wire's SET command,
 * and the Studio include directive's parameters (issue #6423) all share this one rule instead of each carrying
 * its own truncating {@code split("=")}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StringUtilsSplitKeyValueTest {
  @Test
  void theValueIsEverythingAfterTheFirstSeparator() {
    assertThat(StringUtils.splitKeyValue("a=b")).containsExactly("a", "b");
    assertThat(StringUtils.splitKeyValue("a=b=c")).containsExactly("a", "b=c");
    assertThat(StringUtils.splitKeyValue("a===")).containsExactly("a", "==");
  }

  @Test
  void anEmptyValueIsAValue() {
    assertThat(StringUtils.splitKeyValue("a=")).containsExactly("a", "");
  }

  @Test
  void anEmptyKeyIsReturnedAsSuchForTheCallerToReject() {
    assertThat(StringUtils.splitKeyValue("=b")).containsExactly("", "b");
    assertThat(StringUtils.splitKeyValue("=")).containsExactly("", "");
  }

  @Test
  void noSeparatorLeavesTheChoiceToTheCaller() {
    assertThat(StringUtils.splitKeyValue("a")).isNull();
    assertThat(StringUtils.splitKeyValue("")).isNull();
  }

  @Test
  void surroundingBlanksArePreserved() {
    assertThat(StringUtils.splitKeyValue(" a = b ")).containsExactly(" a ", " b ");
  }
}
