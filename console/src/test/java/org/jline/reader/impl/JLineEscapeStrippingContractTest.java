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
package org.jline.reader.impl;

import com.arcadedb.console.TerminalParser;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6827, second half. Teaching {@link TerminalParser} to keep the backslash fixed `-b` and `load`, but not the
 * interactive prompt: jline runs its OWN shell-style unescaping in {@link LineReaderImpl#finish(String)} on the accepted
 * line, before the parser is ever called. So a Windows path typed at the prompt still lost a level of escaping, and the
 * engine - whose string literals need `\\` for one backslash - answered with a token recognition error.
 * <p>
 * The console turns that unescaping off with {@link LineReader.Option#DISABLE_EVENT_EXPANSION}. This test lives in
 * jline's own package because {@code finish()} is protected, and it is deliberately about the library rather than about
 * ArcadeDB: it is the contract the fix depends on, so a jline upgrade that changed it would fail here, naming the
 * reason, rather than silently reopening the bug.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JLineEscapeStrippingContractTest {
  /** The correctly escaped form: what a user has to type for the engine to store `C:\Users\bob`. */
  private static final String TYPED = "insert into Doc set winPath = 'C:\\\\Users\\\\bob'";

  private LineReaderImpl newReader(final boolean disableEventExpansion) throws IOException {
    final Terminal terminal = TerminalBuilder.builder().system(false).dumb(true)
        .streams(new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream()).build();
    final LineReaderImpl reader = new LineReaderImpl(terminal, "test", null);
    reader.setParser(new TerminalParser());
    if (disableEventExpansion)
      reader.option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);
    return reader;
  }

  @Test
  void jlineStripsOneLevelOfEscapingUnlessEventExpansionIsDisabled() throws IOException {
    // THE BUG, STILL PRESENT IN THE LIBRARY: THIS IS WHY THE OPTION IS NOT OPTIONAL FOR US
    assertThat(newReader(false).finish(TYPED)).isEqualTo("insert into Doc set winPath = 'C:\\Users\\bob'");

    // ...AND WHAT THE CONSOLE CONFIGURES: THE LINE REACHES THE QUERY ENGINE EXACTLY AS TYPED
    assertThat(newReader(true).finish(TYPED)).isEqualTo(TYPED);
  }

  @Test
  void aRegexAndAnEscapedQuoteSurviveTheAcceptedLineToo() throws IOException {
    final LineReaderImpl reader = newReader(true);

    assertThat(reader.finish("select from V where p matches '\\\\d+'"))
        .isEqualTo("select from V where p matches '\\\\d+'");
    assertThat(reader.finish("select from V where name = 'it\\'s'"))
        .isEqualTo("select from V where name = 'it\\'s'");
  }
}
