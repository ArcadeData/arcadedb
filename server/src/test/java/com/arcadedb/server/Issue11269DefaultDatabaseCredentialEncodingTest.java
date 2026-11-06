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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11269: {@code arcadedb.server.defaultDatabases} reads a credential as {@code user:password[:group]}, so a password
 * containing a {@code ':'} was cut at it and the text after it became the GROUP - {@code mydb[myuser:mypass:admin:readonly]}
 * created {@code myuser} with the password {@code mypass} in the {@code admin} group, dropping the rest without a word - and
 * the format had no way to write such a password at all. The same for a {@code ']'}, which closed the block early and left
 * the rest of the password ignored after it.
 * <p>
 * A field now writes a separator percent-encoded ({@code %3A}), and the shapes that cannot be read unambiguously are refused
 * with a warning that names the user and never the password.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11269DefaultDatabaseCredentialEncodingTest extends StaticBaseServerTest {
  private static final String DATABASE = "Db11269";
  private static final String USER     = "user11269";

  private final List<String>   warnings = new CopyOnWriteArrayList<>();
  private       ArcadeDBServer server;

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    LogManager.instance().setLogger(capturingLogger());
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      if (server != null) {
        if (server.isStarted()) {
          if (server.getSecurity().existsUser(USER))
            server.getSecurity().dropUser(USER);
          if (server.existsDatabase(DATABASE))
            server.getDatabase(DATABASE).getEmbedded().drop();
          server.stop();
        }
        server = null;
      }
    } finally {
      // process-wide: put the default back, or every later log-asserting test in this fork captures nothing
      LogManager.instance().setLogger(new DefaultLogger());
      super.endTest();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The parse of one credential
  // ---------------------------------------------------------------------------------------------

  @Test
  void aPercentEncodedSeparatorIsPartOfTheField() {
    final DefaultDatabaseCredential credential = DefaultDatabaseCredential.parse("my%3Auser:my%3Apass%2C%3B%5B%5D%25:read%3Aonly");

    assertThat(credential.user()).isEqualTo("my:user");
    assertThat(credential.password()).isEqualTo("my:pass,;[]%");
    assertThat(credential.group()).isEqualTo("read:only");
  }

  @Test
  void onlyAWellFormedEscapeIsDecoded() {
    assertThat(DefaultDatabaseCredential.parse("jay:100%sure+50%").password())
        .as("a '%' not followed by two hexadecimal digits, and a '+', are read as written: a password that merely contains them is unchanged")
        .isEqualTo("100%sure+50%");
    assertThat(DefaultDatabaseCredential.parse("jay:caf%C3%A9%E2%82%AC").password())
        .as("a run of escapes is one UTF-8 sequence, so a character of several bytes is spelled as several escapes")
        .isEqualTo("café€");
    assertThat(DefaultDatabaseCredential.parse("jay:%٣٣").password())
        .as("only ASCII hexadecimal digits make an escape")
        .isEqualTo("%٣٣");
  }

  @Test
  void escapesThatAreNotUtf8AreRefused() {
    assertThatThrownBy(() -> DefaultDatabaseCredential.parse("jay:secret%FFvalue"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("password")
        .hasMessageNotContaining("secret");
  }

  /** The reported shape: the tail of the password used to become the group, and the rest was dropped. */
  @Test
  void aCredentialWithMoreThanThreeFieldsIsRefusedRatherThanTruncated() {
    assertThatThrownBy(() -> DefaultDatabaseCredential.parse("myuser:mypass:admin:readonly"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("4 ':'-separated fields")
        .hasMessageContaining("%3A")
        .hasMessageNotContaining("mypass");
  }

  @Test
  void theThreeShapesTheFormatReadsAreUnchanged() {
    assertThat(DefaultDatabaseCredential.parse("jay")).isEqualTo(new DefaultDatabaseCredential("jay", null, null));
    assertThat(DefaultDatabaseCredential.parse("jay:miner")).isEqualTo(new DefaultDatabaseCredential("jay", "miner", null));
    assertThat(DefaultDatabaseCredential.parse("jay:miner:admin")).isEqualTo(new DefaultDatabaseCredential("jay", "miner", "admin"));
    assertThat(DefaultDatabaseCredential.parse("jay:miner:")).as("an empty group is no group")
        .isEqualTo(new DefaultDatabaseCredential("jay", "miner", null));
  }

  @Test
  void anEmptyUserOrPasswordIsRefused() {
    assertThatThrownBy(() -> DefaultDatabaseCredential.parse(":miner")).hasMessageContaining("no user name");
    assertThatThrownBy(() -> DefaultDatabaseCredential.parse("jay:")).hasMessageContaining("empty password");
    assertThatThrownBy(() -> DefaultDatabaseCredential.parse("jay::admin")).hasMessageContaining("empty password");
  }

  // ---------------------------------------------------------------------------------------------
  // What the server does with it at startup
  // ---------------------------------------------------------------------------------------------

  @Test
  void aPasswordWithEncodedSeparatorsAuthenticatesAsWritten() {
    startWith(DATABASE + "[" + USER + ":Pass%3Aword%2C11269%5D:admin]");

    assertThat(server.getSecurity().authenticate(USER, "Pass:word,11269]", DATABASE).getName()).isEqualTo(USER);
    assertThat(server.getSecurity().getUser(USER).toJSON().getJSONObject("databases").getJSONArray(DATABASE).toList())
        .containsExactly("admin");
    assertThatThrownBy(() -> server.getSecurity().authenticate(USER, "Pass", DATABASE))
        .isInstanceOf(ServerSecurityException.class);
  }

  /** The issue's example: no user is created, rather than one in 'admin' with a truncated password. */
  @Test
  void aRawColonInThePasswordCreatesNoUserInsteadOfOneWithATruncatedPasswordAndAPromotedGroup() {
    startWith(DATABASE + "[" + USER + ":mypass11269:admin:readonly]");

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
    assertThat(server.getSecurity().existsUser(USER)).isFalse();
    assertThat(warnings).anySatisfy(line -> assertThat(line).contains("'" + USER + "'").contains("%3A"));
    assertThat(warnings).noneMatch(line -> line.contains("mypass11269"));
  }

  /**
   * A raw ']' closed the block early, and what followed it was ignored: the user was created with the part of the password
   * before it. The entry is refused, and the line quoting it masks the rest of the password.
   */
  @Test
  void aRawBracketInThePasswordRefusesTheEntryInsteadOfTruncatingThePassword() {
    startWith(DATABASE + "[" + USER + ":hun]ter11269:admin]");

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
    assertThat(server.getSecurity().existsUser(USER)).isFalse();
    assertThat(server.existsDatabase(DATABASE)).isFalse();
    assertThat(warnings).anySatisfy(line -> assertThat(line).contains("Unexpected text after the credentials block")
        .contains("'" + DATABASE + "[" + USER + ":*****]*****'"));
    assertThat(warnings).noneMatch(line -> line.contains("ter11269"));
  }

  private void startWith(final String defaultDatabases) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, defaultDatabases);
    server = new ArcadeDBServer(config);
    server.start();
  }

  private Logger capturingLogger() {
    return new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        record(level, message, exception,
            new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16,
                arg17 });
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        record(level, message, exception, args);
      }

      @Override
      public void flush() {
      }

      private void record(final Level level, final String message, final Throwable exception, final Object[] args) {
        if (message == null || level.intValue() < Level.WARNING.intValue())
          return;
        String line;
        try {
          line = args == null || args.length == 0 ? message : String.format(message, args);
        } catch (final IllegalFormatException e) {
          line = message;
        }
        if (exception != null)
          line += " " + exception;
        warnings.add(line);
      }
    };
  }
}
