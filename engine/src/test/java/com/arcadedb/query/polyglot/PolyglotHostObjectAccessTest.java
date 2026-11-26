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
package com.arcadedb.query.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.graalvm.polyglot.PolyglotException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The objects a script is handed (the bound {@code database}, and whatever its methods return) expose the query engines,
 * the process, thread and file handles of the host only to host code: a script keeps {@code database.command(...)},
 * {@code database.query(...)} and the record, schema and transaction API, nothing else.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PolyglotHostObjectAccessTest extends TestHelper {
  private static final String SAMPLE_CALL = "java.lang.Math::abs";

  @Test
  void scriptCannotRegisterClassesOnTheJavaEngine() {
    assertDenied("database.getQueryEngine('java').registerFunctions('java.lang.Math');", Map.of());

    // THE REGISTRATION DID NOT HAPPEN: THE CLASS IS STILL NOT CALLABLE THROUGH THE JAVA ENGINE
    assertThatThrownBy(() -> database.command("java", SAMPLE_CALL, -1)).isInstanceOf(CommandExecutionException.class)
        .hasRootCauseInstanceOf(SecurityException.class);
  }

  @Test
  void scriptCannotCallThroughTheJavaEngine() {
    assertDenied("var engine = database.getQueryEngine('java'); engine.registerFunctions('java.lang.Math'); engine.command('"
        + SAMPLE_CALL + "', null, -1);", Map.of());
  }

  @Test
  void scriptCannotUseAnyQueryEngineInstance() {
    assertDenied("database.getQueryEngine('sql').command('select 1', null, []);", Map.of());
    assertDenied("database.getQueryEngine('js').getLanguage();", Map.of());
    assertDenied("database.getQueryEngine('java').unregisterFunctions();", Map.of());
  }

  @Test
  void scriptCannotUseTheQueryEngineManager() {
    assertDenied("database.getQueryEngineManager().getAvailableLanguages();", Map.of());
    assertDenied("database.getQueryEngineManager().register('java.lang.Object');", Map.of());
    assertDenied("database.getQueryEngineManager().getEngine('java', database);", Map.of());
    // A MEMBER THE MANAGER INHERITS FROM THE POOL IT IS BUILT ON
    assertDenied("database.getQueryEngineManager().getExecutorService();", Map.of());
  }

  @Test
  void scriptCannotUseAValueTheJavaEngineReturns() {
    // THE HOST REGISTERED THE CLASS, SO THE JAVA ENGINE ANSWERS A RUNTIME: THE SCRIPT STILL HAS NOTHING TO CALL ON IT
    database.getQueryEngine("java").registerFunctions("java.lang.Runtime");
    try {
      assertDenied("database.command('java', 'java.lang.Runtime::getRuntime').next().getProperty('value').availableProcessors();",
          Map.of());
    } finally {
      database.getQueryEngine("java").unregisterFunctions();
    }
  }

  @Test
  void aFunctionCannotUseTheQueryEngines() {
    database.command("sql",
        "DEFINE FUNCTION eng.register 'database.getQueryEngine(\"java\").registerFunctions(\"java.lang.Math\"); return 1;' LANGUAGE js");

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT `eng.register`() AS r")) {
        rs.next();
      }
    }).hasStackTraceContaining("failed due to: Unknown identifier: registerFunctions");

    assertThatThrownBy(() -> database.command("java", SAMPLE_CALL, -1)).isInstanceOf(CommandExecutionException.class)
        .hasRootCauseInstanceOf(SecurityException.class);
  }

  @Test
  void aTriggerCannotUseTheQueryEngines() {
    database.command("sql", "CREATE DOCUMENT TYPE Watched");
    database.command("sql", "CREATE TRIGGER engine_use BEFORE CREATE ON TYPE Watched EXECUTE JAVASCRIPT "
        + "'database.getQueryEngine(\"java\").registerFunctions(\"java.lang.Math\"); true;'");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Watched").set("x", 1).save())).isInstanceOf(
        RuntimeException.class).hasStackTraceContaining("failed due to: Unknown identifier: registerFunctions");

    assertThatThrownBy(() -> database.command("java", SAMPLE_CALL, -1)).isInstanceOf(CommandExecutionException.class)
        .hasRootCauseInstanceOf(SecurityException.class);
  }

  @Test
  void hostCodeStillUsesTheJavaEngine() {
    // POSITIVE CONTROL: THE FIXTURE OF THE TESTS ABOVE WORKS WHEN IT IS HOST CODE THAT REGISTERS THE CLASS
    database.getQueryEngine("java").registerFunctions("java.lang.Math");
    try (final ResultSet rs = database.command("java", SAMPLE_CALL, -1)) {
      assertThat(rs.next().<Object>getProperty("value")).isEqualTo(1);
    }
  }

  @Test
  void scriptCannotUseAProcessOrThreadHandle() {
    assertDenied("handle.availableProcessors();", Map.of("handle", Runtime.getRuntime()));
    assertDenied("handle.pid();", Map.of("handle", ProcessHandle.current()));
    assertDenied("handle.getName();", Map.of("handle", Thread.currentThread()));
    assertDenied("handle.getName();", Map.of("handle", Thread.currentThread().getThreadGroup()));
    assertDenied("handle.command();", Map.of("handle", new ProcessBuilder("no-such-program")));
    assertDenied("handle.isAlive();", Map.of("handle", new IdleProcess()));
  }

  @Test
  void scriptCannotUseAFileOfTheHost() {
    assertDenied("handle.getName();", Map.of("handle", new File("somewhere")));
    assertDenied("database.getConfigurationFile().getParentFile();", Map.of());
    assertDenied("database.getFileManager().getFiles();", Map.of());
    assertDenied("database.getFileManager().getFile(0).getOSFile();", Map.of());
  }

  @Test
  void scriptCannotReadOrWriteThroughAnOpenFile(@TempDir final Path directory) throws IOException {
    final File file = directory.resolve("open.bin").toFile();
    try (final RandomAccessFile handle = new RandomAccessFile(file, "rw")) {
      handle.write(7);
      handle.seek(0);

      // MEMBERS OF THE FILE ITSELF AND THE ONES IT HAS FROM THE DATA INPUT/OUTPUT INTERFACES
      for (final String call : new String[] { "handle.read();", "handle.readByte();", "handle.readInt();", "handle.write(1);",
          "handle.writeByte(1);", "handle.writeInt(1);", "handle.length();", "handle.getChannel();", "handle.getFD();" })
        assertDenied(call, Map.of("handle", handle));

      // THE CHANNEL OF THE FILE: SIZE AND POSITION ARE DECLARED BY THE SEEKABLE CHANNEL INTERFACE TOO
      final FileChannel channel = handle.getChannel();
      for (final String call : new String[] { "handle.size();", "handle.position();", "handle.truncate(0);", "handle.force(true);" })
        assertDenied(call, Map.of("handle", channel));

      assertThat(handle.length()).isEqualTo(1L);
      assertThat(handle.getFilePointer()).isZero();
      assertThat(channel.isOpen()).isTrue();
    }

    try (final FileInputStream in = new FileInputStream(file); final FileOutputStream out = new FileOutputStream(file, true)) {
      assertDenied("handle.read();", Map.of("handle", in));
      assertDenied("handle.write(1);", Map.of("handle", out));
      assertThat(in.read()).isEqualTo(7);
      assertThat(file.length()).isEqualTo(1L);
    }
  }

  @Test
  void theRestOfTheDatabaseApiIsStillReachable() {
    // POSITIVE CONTROL: ONLY THE ENGINE-INTERNAL AND HOST HANDLES ARE WALLED OFF
    database.getSchema().createDocumentType("Item");
    try (final ResultSet rs = database.command("js", "database.getName();")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo(database.getName());
    }
    try (final ResultSet rs = database.command("js", "database.command('sql', 'select 1 as a').next().getProperty('a');")) {
      assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(1);
    }
    try (final ResultSet rs = database.command("js", "database.getSchema().existsType('Item');")) {
      assertThat(rs.next().<Boolean>getProperty("value")).isTrue();
    }
  }

  private void assertDenied(final String script, final Map<String, Object> parameters) {
    // THE MEMBER IS NOT THERE FOR THE SCRIPT: A SCRIPT THAT FAILED FOR ANY OTHER REASON DOES NOT SAY SO
    assertThatThrownBy(() -> database.command("js", script, parameters)).as("script: %s", script)
        .isInstanceOf(CommandExecutionException.class).hasCauseInstanceOf(PolyglotException.class).cause()
        .hasMessageContaining("failed due to: Unknown identifier");
  }

  /** A process that never ran: the script must not be able to call it, whatever it would answer. */
  private static class IdleProcess extends Process {
    @Override
    public OutputStream getOutputStream() {
      return OutputStream.nullOutputStream();
    }

    @Override
    public InputStream getInputStream() {
      return InputStream.nullInputStream();
    }

    @Override
    public InputStream getErrorStream() {
      return InputStream.nullInputStream();
    }

    @Override
    public int waitFor() {
      return 0;
    }

    @Override
    public int exitValue() {
      return 0;
    }

    @Override
    public void destroy() {
      // NOTHING TO STOP
    }
  }
}
