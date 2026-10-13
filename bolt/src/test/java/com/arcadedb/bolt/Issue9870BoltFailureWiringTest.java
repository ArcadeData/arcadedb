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
package com.arcadedb.bolt;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9870: a FAILURE whose code a {@code classify*} method derived from a caught exception must go out through
 * {@code BoltNetworkExecutor.sendExecutionFailure}, the one sender that appends the leader's address to a follower's
 * refusal (issue #9845). Seven arms used to append it by hand, so a new arm could answer NotALeader and leave a
 * {@code bolt://} client without the address to reconnect to. This scans the executor's source and names every
 * {@code sendFailure} call whose code is not a {@code BoltException}/{@code BoltErrorCodes} constant: a code computed
 * from an exception, whether inline or through a local, belongs to {@code sendExecutionFailure}. The one exception is
 * that sender's own call, matched verbatim.
 * <p>
 * The path resolves against Maven's {@code basedir} (the module root), or the working directory when it is not set: an
 * IDE run from elsewhere fails on the read, not silently.
 */
class Issue9870BoltFailureWiringTest {
  private static final Path    EXECUTOR   = Path.of(System.getProperty("basedir", "."),
      "src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java");
  // A call (not the method's own declaration) whose first argument is neither a constant nor sendExecutionFailure's parameter
  private static final Pattern COMPUTED   = Pattern.compile("(?<!void )sendFailure\\(\\s*(?!BoltException\\.|BoltErrorCodes\\.|code, withLeaderRedirect\\(text, error\\)\\);)");

  @Test
  void everyComputedFailureCodeGoesThroughTheSenderThatAddsTheRedirect() throws IOException {
    final String source = Files.readString(EXECUTOR);
    assertThat(source).as("the scan must read the executor itself").contains("private void sendExecutionFailure(");
    assertThat(offenders(source)).as("arms that send a computed failure code without the leader redirect").isEmpty();
  }

  @Test
  void theScanFlagsAComputedCodeSentDirectly() {
    final String source = """
        sendFailure(BoltException.PROTOCOL_ERROR, "Unknown message");
        sendFailure(classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR), text);
        sendFailure(
            classifyDatabaseSelectionError(e), text);
        sendExecutionFailure(classifyExecutionError(e, BoltErrorCodes.TRANSACTION_ERROR), text, e);
        final String status = classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR);
        sendFailure(status, text);
        sendFailure(BoltErrorCodes.FORBIDDEN_ERROR, "denied");
        private void sendFailure(final String code, final String message) throws IOException {
        sendFailure(code, withLeaderRedirect(text, error));
        sendFailure(code, text);
        sendFailure(codeOfTheError, text);
        """;
    assertThat(offenders(source)).containsExactly(2, 3, 7, 11, 12);
  }

  /** The 1-based line of every {@code sendFailure} call whose code is computed rather than a constant. */
  static List<Integer> offenders(final String source) {
    final List<Integer> lines = new ArrayList<>();
    final Matcher matcher = COMPUTED.matcher(source);
    while (matcher.find())
      lines.add((int) source.substring(0, matcher.start()).chars().filter(c -> c == '\n').count() + 1);
    return lines;
  }
}
