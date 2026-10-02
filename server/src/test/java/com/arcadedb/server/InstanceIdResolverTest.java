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
import com.arcadedb.InstanceId;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class InstanceIdResolverTest {
  private static final String VALID = "adb-123e4567-e89b-12d3-a456-426614174000";

  @TempDir
  Path root;

  Path databases;
  Path config;

  @BeforeEach
  void directories() throws IOException {
    databases = Files.createDirectory(root.resolve("databases"));
    config = Files.createDirectory(root.resolve("config"));
  }

  private static ContextConfiguration config(final String setting, final boolean derived) {
    final ContextConfiguration cfg = new ContextConfiguration();
    if (setting != null)
      cfg.setValue(GlobalConfiguration.INSTANCE_ID, setting);
    cfg.setValue(GlobalConfiguration.INSTANCE_DERIVED, derived);
    return cfg;
  }

  private String resolve(final String setting, final boolean derived, final String serverName) {
    return InstanceIdResolver.resolve(config(setting, derived), config, databases, "arcadedb", serverName);
  }

  private String resolve() {
    return resolve(null, false, "node-0");
  }

  private Path file() {
    return databases.resolve(InstanceIdResolver.FILE_NAME);
  }

  private Path legacy() {
    return config.resolve(InstanceIdResolver.LEGACY_FILE_NAME);
  }

  /** Runs {@code action} capturing everything logged as "LEVEL message args". */
  private static List<String> logged(final Runnable action) {
    final List<String> lines = new ArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object r, final Level level, final String message, final Throwable e, final String context,
          final Object a1, final Object a2, final Object a3, final Object a4, final Object a5, final Object a6, final Object a7,
          final Object a8, final Object a9, final Object a10, final Object a11, final Object a12, final Object a13,
          final Object a14, final Object a15, final Object a16, final Object a17) {
        lines.add(level + " " + message + " " + a1 + " " + a2 + " " + a3 + " " + a4);
      }

      @Override
      public void log(final Object r, final Level level, final String message, final Throwable e, final String context,
          final Object... args) {
        final StringBuilder b = new StringBuilder(level + " " + message);
        for (final Object a : args)
          b.append(' ').append(a);
        lines.add(b.toString());
      }

      @Override
      public void flush() {
      }
    });
    try {
      action.run();
    } finally {
      LogManager.instance().setLogger(previous);
    }
    return lines;
  }

  @Test
  void generatesAndPersistsInTheDatabasesDirectoryThenReuses() throws IOException {
    final String first = resolve();

    assertThat(InstanceId.isValid(first)).isTrue();
    assertThat(Files.readString(file())).contains(first).contains("node-0");
    assertThat(legacy()).doesNotExist();
    assertThat(resolve()).isEqualTo(first);
  }

  @Test
  void validSettingWinsOverEverything() throws IOException {
    Files.writeString(file(), InstanceId.generate() + "\nserver=node-0\n");

    assertThat(resolve(VALID.toUpperCase(), true, "node-0")).isEqualTo(VALID);
  }

  @Test
  void malformedSettingIsIgnored() {
    final String id = resolve("not-an-id", false, "node-0");

    assertThat(InstanceId.isValid(id)).isTrue();
    assertThat(file()).exists();
  }

  @Test
  void derivedIdNeedsNoFileIsStableAndDiffersPerServer() {
    final List<String> log = new ArrayList<>();
    final String[] id = new String[1];
    log.addAll(logged(() -> id[0] = resolve(null, true, "arcadedb-0")));

    assertThat(id[0]).isEqualTo(InstanceId.derive("arcadedb", "arcadedb-0"));
    assertThat(resolve(null, true, "arcadedb-0")).isEqualTo(id[0]);
    assertThat(resolve(null, true, "arcadedb-1")).isNotEqualTo(id[0]);
    assertThat(file()).doesNotExist();
    assertThat(log).anyMatch(l -> l.startsWith("INFO") && l.toLowerCase().contains("derived"));
  }

  @Test
  void derivedIdWithTheDefaultServerNameWarnsAndAnotherNameDoesNot() {
    final String defaultName = GlobalConfiguration.SERVER_NAME.getDefValue().toString();
    final List<String> withDefault = logged(() -> resolve(null, true, defaultName));
    assertThat(withDefault).anyMatch(l -> l.startsWith("WARNING") && l.contains("same instance id") && l.contains(defaultName));

    final List<String> named = logged(() -> resolve(null, true, "arcadedb-1"));
    assertThat(named).noneMatch(l -> l.startsWith("WARNING"));
  }

  @Test
  void legacyFileIsCopiedToTheDatabasesDirectoryAndLeftInPlace() throws IOException {
    Files.writeString(legacy(), VALID + System.lineSeparator());

    assertThat(resolve()).isEqualTo(VALID);
    assertThat(Files.readString(file())).contains(VALID).contains("node-0");
    assertThat(Files.readString(legacy())).contains(VALID);
    assertThat(resolve()).isEqualTo(VALID);
  }

  @Test
  void theDatabasesFileWinsOverTheLegacyOne() throws IOException {
    Files.writeString(legacy(), InstanceId.generate());
    Files.writeString(file(), VALID + "\nserver=node-0\n");

    assertThat(resolve()).isEqualTo(VALID);
  }

  @Test
  void aClonedDirectoryGetsANewIdAndALogWarning() throws IOException {
    Files.writeString(file(), VALID + "\nserver=node-0\n");

    final String[] id = new String[1];
    final List<String> log = logged(() -> id[0] = resolve(null, false, "node-1"));

    assertThat(id[0]).isNotEqualTo(VALID);
    assertThat(InstanceId.isValid(id[0])).isTrue();
    assertThat(Files.readString(file())).contains(id[0]).contains("node-1").doesNotContain(VALID);
    assertThat(log).anyMatch(l -> l.startsWith("WARNING") && l.contains("node-0") && l.contains("node-1")
        && l.contains(id[0]) && l.contains("cloned"));
    assertThat(resolve(null, false, "node-1")).isEqualTo(id[0]);
  }

  @Test
  void sameServerNameIsNotAClone() throws IOException {
    Files.writeString(file(), VALID + "\nserver=node-0\n");

    final List<String> log = logged(() -> assertThat(resolve()).isEqualTo(VALID));

    assertThat(log).noneMatch(l -> l.startsWith("WARNING"));
  }

  @Test
  void aFileWithOnlyTheIdIsAdoptedAndGetsTheServerName() throws IOException {
    Files.writeString(file(), VALID + System.lineSeparator());

    assertThat(resolve()).isEqualTo(VALID);
    assertThat(Files.readString(file())).contains("node-0");
  }

  @Test
  void invalidFileIsKeptAsInvalidAndRegenerated() throws IOException {
    Files.writeString(file(), "garbage");

    final String id = resolve();

    assertThat(InstanceId.isValid(id)).isTrue();
    assertThat(Files.readString(databases.resolve(InstanceIdResolver.INVALID_FILE_NAME))).isEqualTo("garbage");
    assertThat(Files.readString(file())).contains(id);
  }

  @Test
  void unwritableDatabasesDirectoryFallsBackToTheConfigurationDirectory() throws IOException {
    assumeThat(databases.toFile().setWritable(false)).isTrue();
    try {
      assumeThat(databases.toFile().canWrite()).as("running as a user that ignores permissions").isFalse();

      final String id = resolve();

      assertThat(Files.readString(legacy())).contains(id).contains("node-0");
      assertThat(resolve()).isEqualTo(id);
    } finally {
      databases.toFile().setWritable(true);
    }
  }

  @Test
  void noWritableDirectoryKeepsTheIdInMemoryWithOneWarning() {
    final Path nowhere = root.resolve("missing");
    final String[] id = new String[1];
    final List<String> log = logged(() -> id[0] = InstanceIdResolver.resolve(config(null, false), nowhere, nowhere, "arcadedb",
        "node-0"));

    assertThat(InstanceId.isValid(id[0])).isTrue();
    assertThat(log.stream().filter(l -> l.startsWith("WARNING")).count()).isEqualTo(1);
    assertThat(log).anyMatch(l -> l.contains("restart"));
    assertThat(InstanceIdResolver.resolve(config(null, false), nowhere, nowhere, "arcadedb", "node-0")).isEqualTo(id[0]);
  }

  @Test
  void aHiddenFileInTheDatabasesDirectoryIsNotADatabase() throws IOException {
    resolve();
    Files.createDirectory(databases.resolve("mydb"));

    // the server's discovery is File::isDirectory over this directory
    final java.io.File[] found = databases.toFile().listFiles(java.io.File::isDirectory);

    assertThat(file()).exists();
    assertThat(found).extracting(java.io.File::getName).containsExactly("mydb");
  }
}
