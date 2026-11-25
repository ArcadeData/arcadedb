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
import com.arcadedb.server.security.SecurityUserFileRepository;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The {@code arcadedb.server.defaultDatabases} setting: the credentials it grants, how a credential is encoded, and what the
 * server logs of it.
 */
class DefaultDatabasesTest {
  // Regression: #10535 default-database credentials grant the database without rewriting the live user
  /**
   * Issue #10535: the credentials of a default database ({@code name[user:password:group]}) used to grant the database to an
   * existing user by changing the live user and saving the whole map after it. A configuration directory that cannot be written, the
   * ordinary shape of a Kubernetes ConfigMap mount, left the grant in force in memory with nothing on disk, and for a user that
   * already had access the failed save was reported as {@code "already exists with a different password"}.
   * <p>
   * The grant is now made on a copy and persisted before it is published, so a startup that cannot write it fails with the reason, and
   * a grant the user already holds is not written at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Credentials extends StaticBaseServerTest {
    private static final String DATABASE = "Db10535";
    private static final String USER     = "alice10535";
    private static final String PASSWORD = "Password10535!";

    private final File configDirectory = new File("./target/config");
    private final Path usersFile       = configDirectory.toPath().resolve(SecurityUserFileRepository.FILE_NAME);
    private       ArcadeDBServer server;

    @AfterEach
    @Override
    public void endTest() {
      try {
        stopServer();
      } finally {
        // Writable again before the helper deletes the directory, or it stays behind for the next class of the fork
        configDirectory.setWritable(true);
        super.endTest();
      }
    }

    /** A user that can already reach the database and proves it with its password gains the new group, persisted. */
    @Test
    void anExistingUserWithTheRightPasswordGainsTheGroup() throws Exception {
      startWith(credentials(PASSWORD, "admin"));
      stopServer();

      startWith(credentials(PASSWORD, "extra"));

      assertThat(groupsOfUser()).containsExactlyInAnyOrder("admin", "extra");
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).contains("extra");
    }

    /** The mismatch the warning describes: nothing is granted to a caller that does not know the password. */
    @Test
    void aWrongPasswordGrantsNothing() {
      startWith(credentials(PASSWORD, "admin"));
      stopServer();

      startWith(credentials("another-Password10535!", "extra"));

      assertThat(groupsOfUser()).containsExactly("admin");
    }

    /**
     * A users file that cannot be written fails the startup with the reason: the grant is not in force and nothing says it is, where it
     * used to be in force in memory and blamed on the password.
     */
    @Test
    void anUnwritableUsersFileFailsTheStartupInsteadOfBlamingThePassword() throws Exception {
      startWith(credentials(PASSWORD, "admin"));
      stopServer();
      final String beforeTheRefusal = Files.readString(usersFile, StandardCharsets.UTF_8);
      configDirectory.setWritable(false);
      assumeTrue(!configDirectory.canWrite(), "the directory is still writable (running as a superuser?)");

      server = new ArcadeDBServer(configuration(credentials(PASSWORD, "extra")));

      assertThatThrownBy(() -> server.start()).hasStackTraceContaining(SecurityUserFileRepository.FILE_NAME);
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("the refusal writes nothing").isEqualTo(beforeTheRefusal);
    }

    /** What the first start made is what every later start asks for: it needs no write, so a read-only directory is fine. */
    @Test
    void aGrantTheUserAlreadyHoldsNeedsNoWrite() {
      startWith(credentials(PASSWORD, "admin"));
      stopServer();
      configDirectory.setWritable(false);
      assumeTrue(!configDirectory.canWrite(), "the directory is still writable (running as a superuser?)");

      startWith(credentials(PASSWORD, "admin"));

      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
      assertThat(groupsOfUser()).containsExactly("admin");
    }

    /**
     * An entry written without a group, {@code Db[user:password]}: the user is created, the grant reaches the file, and a restart reads the
     * file back and finds the same user able to authenticate against the database.
     */
    @Test
    void anEntryWithoutAGroupRoundTripsThroughTheUsersFile() throws Exception {
      final String withoutGroup = DATABASE + "[" + USER + ":" + PASSWORD + "]";
      startWith(withoutGroup);
      assertThat(server.getSecurity().authenticate(USER, PASSWORD, DATABASE).getName()).isEqualTo(USER);
      final String firstStart = Files.readString(usersFile, StandardCharsets.UTF_8);
      assertThat(firstStart).contains(USER).contains(DATABASE);
      stopServer();

      // The grant is held, so the restart writes nothing: on a directory that cannot be written it still starts, and the file is as it was
      configDirectory.setWritable(false);
      assumeTrue(!configDirectory.canWrite(), "the directory is still writable (running as a superuser?)");
      startWith(withoutGroup);

      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
      assertThat(server.getSecurity().authenticate(USER, PASSWORD, DATABASE).getName()).isEqualTo(USER);
      assertThat(Files.readString(usersFile, StandardCharsets.UTF_8)).as("a restart does not rewrite the file").isEqualTo(firstStart);
    }

    private static String credentials(final String password, final String group) {
      return DATABASE + "[" + USER + ":" + password + ":" + group + "]";
    }

    private List<Object> groupsOfUser() {
      return server.getSecurity().getUser(USER).toJSON().getJSONObject("databases").getJSONArray(DATABASE).toList();
    }

    private void startWith(final String defaultDatabases) {
      server = new ArcadeDBServer(configuration(defaultDatabases));
      server.start();
    }

    private static ContextConfiguration configuration(final String defaultDatabases) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, defaultDatabases);
      return config;
    }

    private void stopServer() {
      if (server == null)
        return;
      try {
        if (server.isStarted())
          server.stop();
      } finally {
        server = null;
      }
    }
  }

  // Regression: #11269 a password containing ':' was cut and its tail became the group
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
  @Nested
  class CredentialEncoding extends StaticBaseServerTest {
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
              server.getDatabaseForOperator(DATABASE).getEmbedded().drop();
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

    /**
     * The user name and the group are decoded like the password. A '%' that does not start an escape is read as written,
     * so such a name is unchanged; one followed by two hexadecimal digits decodes, and '%25' is how it keeps its '%'.
     */
    @Test
    void aUserOrGroupNameIsDecodedLikeThePassword() {
      assertThat(DefaultDatabaseCredential.parse("ops%team:pw:read%only"))
          .as("no escape in either name: both are read as written")
          .isEqualTo(new DefaultDatabaseCredential("ops%team", "pw", "read%only"));
      assertThat(DefaultDatabaseCredential.parse("team%41:pw:group%41"))
          .as("a '%' followed by two hexadecimal digits is an escape in a name too")
          .isEqualTo(new DefaultDatabaseCredential("teamA", "pw", "groupA"));
      assertThat(DefaultDatabaseCredential.parse("team%2541:pw:group%2541"))
          .as("and '%25' keeps a literal '%' before two hexadecimal digits")
          .isEqualTo(new DefaultDatabaseCredential("team%41", "pw", "group%41"));
      assertThat(DefaultDatabaseCredential.parse("team%41"))
          .as("a credential naming an existing user is decoded the same way")
          .isEqualTo(new DefaultDatabaseCredential("teamA", null, null));
    }

    /** A name is logged and stored: an escape must not smuggle a line break or a NUL into it. A password may hold anything. */
    @Test
    void aControlCharacterInADecodedNameIsRefused() {
      assertThatThrownBy(() -> DefaultDatabaseCredential.parse("jay%0Aforged:pw")).hasMessageContaining("user name with a control character");
      assertThatThrownBy(() -> DefaultDatabaseCredential.parse("jay:pw:adm%00in")).hasMessageContaining("group with a control character");
      assertThat(DefaultDatabaseCredential.parse("jay:line%0Abreak%09tab").password()).isEqualTo("line\nbreak\ttab");
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

  // Regression: #9773 loadDefaultDatabases logged the raw setting, credentials included
  /**
   * Issue #9773: {@code ArcadeDBServer.loadDefaultDatabases} wrote the raw {@code arcadedb.server.defaultDatabases} value
   * into the server log on its misconfiguration paths - the WHOLE setting when an entry had no {@code '['}, the entry with
   * its credentials when a {@code '{'} was never closed - and from the log it reached support bundles verbatim, because the
   * redaction rule there is keyed on the setting's name and these lines spell it "default databases". The lines now quote
   * the one malformed entry, through {@link GlobalConfiguration#publishableValue(Object)}.
   * <p>
   * Each case puts the malformed entry where the loop meets it before any credential is applied, except the {@code '}'}
   * one, whose check runs after the credentials of its own entry: that one uses a user of its own, dropped afterwards.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LogRedaction extends StaticBaseServerTest {
    private static final String SECRET = "musk9773";
    private static final String USER   = "elon9773";

    private final List<String> warnings = new CopyOnWriteArrayList<>();
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

    /** The issue's first line: it used to quote the WHOLE setting, the credentials of every other entry included. */
    @Test
    void anEntryWithoutCredentialsIsReportedWithoutTheOtherEntriesPasswords() {
      startWith("Broken;Universe9773[" + USER + ":" + SECRET + "]");

      assertWarned("Error in default databases format", "'Broken'");
    }

    /** The commonest such entry: a credential block written without its brackets. */
    @Test
    void anEntryWithoutBracketsIsReportedWithItsPasswordMasked() {
      startWith("Universe9773:" + USER + ":" + SECRET);

      assertWarned("Error in default databases format", "'Universe9773:*****'");
    }

    /** A '[' never closed used to abort the startup with a StringIndexOutOfBoundsException naming neither the setting nor the entry. */
    @Test
    void anUnclosedCredentialBlockIsReportedInsteadOfAbortingTheStartup() {
      startWith("Universe9773[" + USER + ":" + SECRET);

      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
      assertWarned("Missing closing ']' in default databases format", "'Universe9773[*****'");
    }

    /** The issue's second line: it quoted the entry, credentials included. */
    @Test
    void anUnclosedCommandBlockIsReportedWithItsPasswordMasked() {
      startWith("Universe9773[" + USER + ":" + SECRET + "]{import:file:///does-not-exist-9773");

      assertWarned("Missing closing '}' in default databases format",
          "'Universe9773[" + USER + ":*****]{import:file:///does-not-exist-9773'");
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

    private void assertWarned(final String message, final String quotedEntry) {
      assertThat(warnings).as("the line naming the malformed entry").anySatisfy(line -> {
        assertThat(line).contains(message);
        assertThat(line).contains(quotedEntry);
      });
      assertThat(warnings).as("no line of the startup carries the password").noneMatch(line -> line.contains(SECRET));
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
}
