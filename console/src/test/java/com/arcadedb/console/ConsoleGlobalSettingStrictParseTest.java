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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7870: the console is a writer of raw operator-typed text into {@link GlobalConfiguration}, through both its
 * {@code SET} command and its {@code -D<key>=<value>} arguments. It used {@code setValue}, whose Boolean arm is
 * {@code Boolean.parseBoolean}, so {@code SET arcadedb.txWAL = yes} silently turned the write-ahead log OFF. It now
 * goes through the same strict parse as every other writer of raw external text, and says so when it refuses.
 */
class ConsoleGlobalSettingStrictParseTest {
  private Console console;

  @BeforeAll
  static void eliminateTerminalThreads() {
    System.setProperty("org.jline.terminal.dumb", "true");
    System.setProperty("jline.terminal.type", "none");
  }

  @BeforeEach
  void setUp() throws IOException {
    GlobalConfiguration.resetAll();
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    console = new Console();
  }

  @AfterEach
  void tearDown() {
    console.close();
    System.clearProperty(GlobalConfiguration.TX_WAL.getKey());
    System.clearProperty(GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getKey());
    GlobalConfiguration.resetAll();
  }

  @Test
  void setCommandRefusesANonBooleanSpellingInsteadOfStoringFalse() {
    assertThat(GlobalConfiguration.TX_WAL.getValueAsBoolean()).as("precondition: the WAL is on by default").isTrue();

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(buffer::append);

    assertThatThrownBy(() -> console.parse("set arcadedb.txWAL = yes"))
        .hasMessageContaining("yes")
        .hasMessageContaining("arcadedb.txWAL");

    assertThat(GlobalConfiguration.TX_WAL.getValueAsBoolean()).as("'yes' must not turn the WAL off").isTrue();
    assertThat(GlobalConfiguration.TX_WAL.isChanged()).isFalse();
    assertThat(buffer.toString()).as("the refusal is reported to the operator").contains("ERROR").contains("yes");
  }

  @Test
  void setCommandStillAcceptsTrueAndFalseInAnyCase() throws IOException {
    assertThat(console.parse("set arcadedb.txWAL = FALSE")).isTrue();
    assertThat(GlobalConfiguration.TX_WAL.getValueAsBoolean()).isFalse();

    assertThat(console.parse("set arcadedb.txWAL = True")).isTrue();
    assertThat(GlobalConfiguration.TX_WAL.getValueAsBoolean()).isTrue();
  }

  @Test
  void setCommandRefusesANonNumericIntegerWithoutChangingTheSetting() {
    final int before = GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getValueAsInteger();

    assertThatThrownBy(() -> console.parse("set arcadedb.asyncTxBatchSize = lots")).hasMessageContaining("lots");

    assertThat(GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getValueAsInteger()).isEqualTo(before);
  }

  @Test
  void commandLineArgumentRefusesANonBooleanSpellingAndReportsIt() throws IOException {
    final String stderr = runCapturingStderr("-D" + GlobalConfiguration.TX_WAL.getKey() + "=yes");

    assertThat(GlobalConfiguration.TX_WAL.getValueAsBoolean()).as("'-Darcadedb.txWAL=yes' must not turn the WAL off").isTrue();
    assertThat(stderr).contains(GlobalConfiguration.TX_WAL.getKey()).contains("yes").contains("ignored");
  }

  @Test
  void commandLineArgumentWithANonNumericIntegerIsIgnoredInsteadOfAbortingTheConsole() throws IOException {
    final int before = GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getValueAsInteger();

    final String stderr = runCapturingStderr("-D" + GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getKey() + "=lots");

    assertThat(GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getValueAsInteger()).isEqualTo(before);
    assertThat(stderr).contains("lots").contains("ignored");
  }

  @Test
  void commandLineArgumentStillAppliesAValidBoolean() throws IOException {
    runCapturingStderr("-D" + GlobalConfiguration.TX_WAL.getKey() + "=false");

    assertThat(GlobalConfiguration.TX_WAL.getValueAsBoolean()).isFalse();
  }

  private static String runCapturingStderr(final String setting) throws IOException {
    final PrintStream original = System.err;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      Console.execute(new String[] { setting, "-b", "" });
    } finally {
      System.setErr(original);
    }
    return captured.toString(StandardCharsets.UTF_8);
  }
}
