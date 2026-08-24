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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for issue #6423: {@code PostgresNetworkExecutor.setConfiguration()} used to split a
 * {@code SET <param> = <value>} command on EVERY '=', so a value containing a further '=' (e.g. a connection
 * string) was silently truncated - and, because the quote-stripping that followed assumed the truncated value
 * still ended in the closing quote, it chopped off the value's last character too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresSetCommandParsingTest {

  @Test
  void valueContainingEqualsIsKeptWhole() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET search_path = 'a=b'")).containsExactly("search_path", "a=b");
  }

  @Test
  void simpleEqualsAssignment() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET datestyle = 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void toKeywordAssignment() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET datestyle TO 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void toKeywordAssignmentIsCaseInsensitiveAndSplitsOnFirstOccurrenceOnly() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET search_path to 'a TO b'")).containsExactly("search_path", "a TO b");
  }

  @Test
  void toKeywordAssignmentWithValueContainingEqualsIsKeptWhole() {
    // The command uses ' TO ' as its separator, so the '=' inside the value must NOT be mistaken for
    // the (unused) '=' separator - the earlier-occurring separator wins, not '=' unconditionally.
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET search_path TO 'a=b'")).containsExactly("search_path", "a=b");
  }

  @Test
  void equalsAssignmentWithValueContainingToIsKeptWhole() {
    // Symmetric case: the command uses '=' as its separator, so a ' TO ' inside the value must not be
    // mistaken for the (unused) TO separator.
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET search_path = 'a TO b'")).containsExactly("search_path", "a TO b");
  }

  @Test
  void unquotedValueIsNotStripped() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET timezone = UTC")).containsExactly("timezone", "UTC");
  }

  @Test
  void paramNameIsLowerCased() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET DateStyle = 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void noSeparatorReturnsNull() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET justaname")).isNull();
  }

  @Test
  void emptyParamNameIsRejected() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET = somevalue")).isNull();
  }

  @Test
  void anUnclosedQuoteIsRejectedInsteadOfThrowing() {
    // A single stray quote is not a closed quoted value: substring(1, length - 1) on it used to throw
    // StringIndexOutOfBoundsException instead of being treated as the malformed command it is.
    assertThatCode(() -> PostgresNetworkExecutor.parseSetCommand("SET x = '")).doesNotThrowAnyException();
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET x = '")).isNull();
  }

  @Test
  void mismatchedQuotesAreRejected() {
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET x = 'abc\"")).isNull();
  }
}
