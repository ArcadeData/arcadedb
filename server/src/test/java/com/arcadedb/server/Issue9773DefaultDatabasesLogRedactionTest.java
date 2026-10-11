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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue9773DefaultDatabasesLogRedactionTest extends StaticBaseServerTest {
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
