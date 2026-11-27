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
package com.arcadedb.mcp;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the server log says about a {@code tools/call} that carries a statement. The dispatcher writes a line for the call before
 * the tool, the profile or the permissions are looked at, and another for the failure when the engine refuses the statement, so
 * both are checked here against the whole dispatch: the value after the keywords that introduce a credential is masked in every
 * line, whichever way the call ends, while the line still names the tool and the statement.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MCPRequestLogStatementsTest extends BaseGraphServerTest {
  private static final String VALUE     = "Zq7-masked-value-4471";
  private static final String USER_NAME = "request-log-probe";

  private MCPConfiguration   config;
  private ServerSecurityUser root;

  @BeforeEach
  void setupMCP() throws Exception {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setAllowAdmin(true);
    config.setToolProfile(MCPConfiguration.ToolProfile.ALL);
    config.setAllowedUsers(List.of("*"));
    root = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @AfterEach
  void dropProbeUser() throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER_NAME))
      security.dropUser(USER_NAME);
  }

  @Test
  void aStatementThatRunsIsLoggedWithoutTheValueOfItsPasswordClause() throws Exception {
    final List<String> lines = capturingLog(() -> call("execute_command", cypher("CREATE USER `" + USER_NAME + "` SET PASSWORD '" + VALUE + "'")));

    assertThat(lines).noneMatch(line -> line.contains(VALUE));
    assertThat(lines).anySatisfy(line -> assertThat(line).contains("tools/call 'execute_command'").contains("SET PASSWORD ***"));
  }

  @Test
  void aStatementTheProfileRefusesIsLoggedMasked() throws Exception {
    config.setToolProfile(MCPConfiguration.ToolProfile.RAG);

    final List<String> lines = capturingLog(() -> {
      final JSONObject result = call("execute_command", cypher("ALTER USER `" + USER_NAME + "` SET PASSWORD '" + VALUE + "'"));
      assertThat(result.getBoolean("isError", false)).isTrue();
    });

    assertThat(lines).noneMatch(line -> line.contains(VALUE));
    assertThat(lines).anySatisfy(line -> assertThat(line).contains("tools/call 'execute_command'").contains("SET PASSWORD ***"));
  }

  @Test
  void aReadOnlyConfigurationRefusalIsLoggedMasked() throws Exception {
    config.setAllowAdmin(false);
    config.setAllowInsert(false);
    config.setAllowUpdate(false);

    final List<String> lines = capturingLog(() -> call("query", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "cypher")
        .put("query", "ALTER USER `" + USER_NAME + "` SET PASSWORD '" + VALUE + "'")));

    assertThat(lines).noneMatch(line -> line.contains(VALUE));
    assertThat(lines).anySatisfy(line -> assertThat(line).contains("tools/call 'query'").contains("SET PASSWORD ***"));
  }

  /** The failure of a statement the engine cannot parse is logged with the engine's own text: it carries no value either. */
  @Test
  void aStatementTheEngineRefusesIsLoggedMaskedInBothServerModes() throws Exception {
    final JSONObject arguments = cypher("CREATE USER `" + USER_NAME + "` SET PASSWORD '" + VALUE + "' IDENTIFIED BY '" + VALUE + "' )(");

    for (final String mode : new String[] { "development", "production" }) {
      final List<String> lines = capturingLog(() -> withMode(mode, () -> call("execute_command", arguments)));

      assertThat(lines).as(mode).noneMatch(line -> line.contains(VALUE));
      assertThat(lines).as(mode).anySatisfy(line -> assertThat(line).contains("tools/call 'execute_command'"));
    }
  }

  @Test
  void aStatementWithoutACredentialIsLoggedAsItWas() throws Exception {
    final List<String> lines = capturingLog(() -> call("query", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1 AS one")));

    assertThat(lines).anySatisfy(line -> assertThat(line).contains("tools/call 'query'").contains("query=\"SELECT 1 AS one\""));
  }

  private JSONObject cypher(final String command) {
    return new JSONObject().put("database", getDatabaseName()).put("language", "cypher").put("command", command);
  }

  private JSONObject call(final String tool, final JSONObject arguments) {
    final JSONObject request = new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call")
        .put("params", new JSONObject().put("name", tool).put("arguments", arguments));
    return new MCPDispatcher(getServer(0), config, "http").dispatch(request, root, MCPStdioServer.STDIO_CLIENT).json()
        .getJSONObject("result");
  }

  private <T> T withMode(final String mode, final Callable<T> work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try {
      return work.call();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }

  /** Everything the server logs while {@code work} runs: each line as written, followed by the text of its exception chain. */
  private static List<String> capturingLog(final ThrowingRunnable work) throws Exception {
    final RecordingLogger logger = new RecordingLogger();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(logger);
    try {
      work.run();
    } finally {
      LogManager.instance().setLogger(previous);
    }
    return logger.lines;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static class RecordingLogger implements Logger {
    private final List<String> lines = new CopyOnWriteArrayList<>();

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      record(message, exception, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      record(message, exception, args);
    }

    private void record(final String message, final Throwable exception, final Object... args) {
      String line;
      try {
        line = String.format(message, args);
      } catch (final RuntimeException e) {
        line = message;
      }
      final StringBuilder text = new StringBuilder(line);
      for (Throwable cause = exception; cause != null; cause = cause.getCause())
        text.append(" | ").append(cause);
      lines.add(text.toString());
    }

    @Override
    public void flush() {
    }
  }
}
