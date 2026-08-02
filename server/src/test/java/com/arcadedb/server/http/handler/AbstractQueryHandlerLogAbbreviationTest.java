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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AbstractQueryHandler#abbreviateForLog(String)}: the truncation warning echoes a
 * client-supplied command, so the command must not be able to flood the log with its own size nor forge log
 * lines of its own with an embedded line break (issue #5711).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AbstractQueryHandlerLogAbbreviationTest {

  @Test
  void aShortSingleLineCommandIsUnchanged() {
    final String command = "SELECT i FROM V";
    assertThat(AbstractQueryHandler.abbreviateForLog(command)).isEqualTo(command);
  }

  @Test
  void aNullCommandStaysNull() {
    assertThat(AbstractQueryHandler.abbreviateForLog(null)).isNull();
  }

  @Test
  void aLongCommandIsCappedAndMarked() {
    final String command = "SELECT " + "x".repeat(500) + " FROM V";
    final String abbreviated = AbstractQueryHandler.abbreviateForLog(command);

    assertThat(abbreviated).hasSize(120 + "...".length());
    assertThat(abbreviated).startsWith("SELECT xxx").endsWith("...");
  }

  @Test
  void aCommandOfExactlyTheCapIsNotMarked() {
    final String command = "x".repeat(120);
    assertThat(AbstractQueryHandler.abbreviateForLog(command)).isEqualTo(command);
  }

  @Test
  void controlCharactersCannotForgeALogLine() {
    // A command carrying its own line break would otherwise appear in the log as a second, fabricated entry.
    final String abbreviated = AbstractQueryHandler.abbreviateForLog(
        "SELECT i FROM V\n2026-08-01 00:00:00.000 SEVER [Forged] nothing happened\r\tend");

    assertThat(abbreviated).doesNotContain("\n").doesNotContain("\r").doesNotContain("\t");
    assertThat(abbreviated).isEqualTo("SELECT i FROM V 2026-08-01 00:00:00.000 SEVER [Forged] nothing happened  end");
  }

  @Test
  void aLineBreakBeyondTheCapIsDroppedWithTheRest() {
    final String abbreviated = AbstractQueryHandler.abbreviateForLog("SELECT " + "x".repeat(200) + "\nforged");

    assertThat(abbreviated).doesNotContain("forged");
    assertThat(abbreviated).hasSize(120 + "...".length());
  }
}
