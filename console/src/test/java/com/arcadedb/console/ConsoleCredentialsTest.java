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
 * Issue https://github.com/ArcadeData/arcadedb/issues/6829: the console persists every typed line to `./.history` and
 * echoes it to stdout in batch mode, and its own command syntax carries the password inline. These are the rules that
 * decide what gets hidden.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ConsoleCredentialsTest {
  @Test
  void theConnectPasswordIsMasked() {
    assertThat(ConsoleCredentials.mask("connect remote:localhost/mydb root MySecret1!"))
        .isEqualTo("connect remote:localhost/mydb root ***");
    assertThat(ConsoleCredentials.mask("connect remote://localhost:2480/mydb root MySecret1!"))
        .isEqualTo("connect remote://localhost:2480/mydb root ***");
  }

  /**
   * A password is allowed to contain spaces (issue #6830), so the mask has to run to the end of the statement rather
   * than to the end of the first blank-delimited token.
   */
  @Test
  void aPasswordWithSpacesIsMaskedWhole() {
    assertThat(ConsoleCredentials.mask("connect remote:localhost/mydb root my secret pass"))
        .isEqualTo("connect remote:localhost/mydb root ***");
  }

  @Test
  void everyCommandThatOpensARemoteConnectionIsMasked() {
    assertThat(ConsoleCredentials.mask("list databases remote:localhost root MySecret1!"))
        .isEqualTo("list databases remote:localhost root ***");
    assertThat(ConsoleCredentials.mask("create database remote:localhost/mydb root MySecret1!"))
        .isEqualTo("create database remote:localhost/mydb root ***");
    assertThat(ConsoleCredentials.mask("drop database remote:localhost/mydb root MySecret1!"))
        .isEqualTo("drop database remote:localhost/mydb root ***");
  }

  @Test
  void theCreateUserPasswordIsMaskedWithAndWithoutTheGrantClause() {
    assertThat(ConsoleCredentials.mask("create user bob identified by MySecret1!"))
        .isEqualTo("create user bob identified by ***");
    assertThat(ConsoleCredentials.mask("create user bob identified by MySecret1! grant connect to mydb"))
        .isEqualTo("create user bob identified by *** grant connect to mydb");
    assertThat(ConsoleCredentials.mask("CREATE USER bob IDENTIFIED BY MySecret1! GRANT CONNECT TO mydb"))
        .isEqualTo("CREATE USER bob IDENTIFIED BY *** GRANT CONNECT TO mydb");
  }

  @Test
  void aCommandCarryingNoPasswordIsReturnedUnchanged() {
    for (final String command : new String[] { "select from V", "connect mydb", "connect remote:localhost/mydb root",
        "create user bob identified by", "", "close" }) {
      assertThat(ConsoleCredentials.mask(command)).isEqualTo(command);
    }
    assertThat(ConsoleCredentials.mask(null)).isNull();
  }

  /**
   * The masking runs over whole console lines, which can carry several statements. Matching on the command keyword of
   * each statement rather than on `remote:` or `identified by` anywhere in the text is what keeps an ordinary insert
   * from being mangled on its way to the log.
   */
  @Test
  void onlyTheCredentialBearingStatementOfALineIsTouched() {
    assertThat(ConsoleCredentials.mask("connect remote:localhost/mydb root MySecret1!; select from V"))
        .isEqualTo("connect remote:localhost/mydb root ***; select from V");
    assertThat(ConsoleCredentials.mask("close; connect remote:localhost/mydb root MySecret1!"))
        .isEqualTo("close; connect remote:localhost/mydb root ***");

    final String insert = "insert into Doc set note = 'identified by the auditor', url = ' remote:h root p'";
    assertThat(ConsoleCredentials.mask(insert)).isEqualTo(insert);
  }

  /**
   * A semicolon inside a string literal does not end a statement, exactly as {@link TerminalParser} sees it, so the
   * mask must not stop there and leave the rest of the password in the clear.
   */
  @Test
  void aSemicolonInsideThePasswordDoesNotEndTheStatement() {
    assertThat(ConsoleCredentials.mask("connect remote:localhost/mydb root 'a;b'"))
        .isEqualTo("connect remote:localhost/mydb root ***");
  }

  /**
   * The masker keys off the command keyword, so it deliberately does NOT recognise a bare password typed on its own -
   * which is exactly what the masked `Password for 'root': ` prompt reads. That line must therefore be kept out of the
   * history by disabling the history around the read (see {@code Console.askPassword}), never by hoping this class will
   * catch it: there is nothing in `MySecret1!` to key off.
   */
  @Test
  void aBarePasswordIsNotRecognisedAndMustNotBeLeftToThisClass() {
    assertThat(ConsoleCredentials.mask("MySecret1!")).isEqualTo("MySecret1!");
  }

  /**
   * Masking twice must not produce `***` for the mask itself: the history file is loaded and re-added on every session.
   */
  @Test
  void maskingIsIdempotent() {
    final String masked = ConsoleCredentials.mask("connect remote:localhost/mydb root MySecret1!");
    assertThat(ConsoleCredentials.mask(masked)).isEqualTo(masked);
  }
}
