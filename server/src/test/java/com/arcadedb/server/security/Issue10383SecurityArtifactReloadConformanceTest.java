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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10383: the conformance census for the reloadable security artifacts, in the shape of the #10235 admission-gate
 * census. Every artifact a running server re-reads - {@code server-users.jsonl}, {@code server-groups.json},
 * {@code server-api-tokens.json} - is driven through a RELOAD with each of the four inputs a reload can meet, and must
 * follow the one rule #10326 set for the users file:
 * <ul>
 *   <li>what is in force is unchanged;</li>
 *   <li>the live file is left exactly as it was found - byte for byte, still absent, or still not a file: a read path
 *       never writes;</li>
 *   <li>a file that could be read but not used is preserved under {@code <name>-error.<ext>}. An unreadable or a missing
 *       file has nothing that could be copied.</li>
 * </ul>
 * A new reloadable artifact is a new {@link Artifact} constant here, or the census does not hold for it.
 */
class Issue10383SecurityArtifactReloadConformanceTest {

  private static final String CONFIG_PATH   = "target/test-security-10383-census";
  private static final String ROOT_PASSWORD = "PlayWithData123!";

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(ROOT_PASSWORD);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  enum Input {IO_ERROR, UNPARSEABLE, MISSING, WRONG_SHAPE}

  static Stream<Arguments> census() {
    final List<Arguments> cases = new ArrayList<>();
    for (final Artifact artifact : Artifact.values())
      for (final Input input : Input.values())
        cases.add(Arguments.of(artifact, input));
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} reload with {1}")
  @MethodSource("census")
  void aReloadThatCannotUseTheFileKeepsWhatIsInForceAndNeverWrites(final Artifact artifact, final Input input) throws Exception {
    final Subject subject = artifact.start();
    try {
      final Set<String> inForce = subject.inForce();
      assertThat(inForce).as("the fixture has more than the default in force").hasSizeGreaterThan(1);

      final Path file = Path.of(CONFIG_PATH, artifact.fileName);
      final Path errorFile = Path.of(CONFIG_PATH, artifact.errorFileName);
      String written = null;
      switch (input) {
      case IO_ERROR -> {
        // A directory of the same name: it exists, and every read of it fails.
        Files.delete(file);
        Files.createDirectory(file);
      }
      case UNPARSEABLE -> written = "{ this is not json";
      case MISSING -> Files.delete(file);
      case WRONG_SHAPE -> written = artifact.wrongShape(Files.readString(file, StandardCharsets.UTF_8));
      }
      if (written != null)
        Files.writeString(file, written, StandardCharsets.UTF_8);

      subject.reload();

      assertThat(subject.inForce()).as("what is in force is unchanged").isEqualTo(inForce);
      switch (input) {
      case IO_ERROR -> assertThat(file).as("the live path is left alone").isDirectory();
      case MISSING -> assertThat(file).as("nothing was recreated in place of the missing file").doesNotExist();
      default -> {
        assertThat(Files.readString(file, StandardCharsets.UTF_8)).as("the live file is what the operator wrote").isEqualTo(written);
        assertThat(Files.readString(errorFile, StandardCharsets.UTF_8)).as("the refused file is preserved").isEqualTo(written);
      }
      }
    } finally {
      subject.stop();
    }
  }

  /** Control: a reload of a valid file still replaces what is in force, for every artifact. */
  @ParameterizedTest
  @MethodSource("artifacts")
  void aValidReloadIsStillApplied(final Artifact artifact) throws Exception {
    final Subject subject = artifact.start();
    try {
      final Set<String> before = subject.inForce();
      subject.dropOneByHand();

      subject.reload();

      assertThat(subject.inForce()).hasSize(before.size() - 1);
      assertThat(Path.of(CONFIG_PATH, artifact.errorFileName)).doesNotExist();
    } finally {
      subject.stop();
    }
  }

  static Stream<Artifact> artifacts() {
    return Stream.of(Artifact.values());
  }

  interface Subject {
    Set<String> inForce();

    void reload();

    /** Removes one entry from the file, keeping it a valid document. */
    void dropOneByHand() throws Exception;

    void stop();
  }

  enum Artifact {
    USERS(SecurityUserFileRepository.FILE_NAME, SecurityUserFileRepository.ERROR_FILE_NAME) {
      @Override
      Subject start() {
        final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
        security.loadUsers();
        security.createUser(new JSONObject().put("name", "bob").put("password", security.encodePassword("BobPassword123!"))
            .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
        return new Subject() {
          @Override
          public Set<String> inForce() {
            return new TreeSet<>(security.getUsers());
          }

          @Override
          public void reload() {
            security.loadUsers();
          }

          @Override
          public void dropOneByHand() throws Exception {
            final Path file = Path.of(CONFIG_PATH, fileName);
            final StringBuilder kept = new StringBuilder();
            for (final String line : Files.readAllLines(file, StandardCharsets.UTF_8))
              if (!line.contains("\"bob\""))
                kept.append(line).append('\n');
            Files.writeString(file, kept.toString(), StandardCharsets.UTF_8);
          }

          @Override
          public void stop() {
            security.stopService();
          }
        };
      }

      @Override
      String wrongShape(final String current) {
        return "{\"nome\":\"bob\"}\n";
      }
    },

    GROUPS(SecurityGroupFileRepository.FILE_NAME, SecurityGroupFileRepository.ERROR_FILE_NAME) {
      @Override
      Subject start() throws Exception {
        // The watcher is never due during the test: the reload is driven by hand.
        final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(CONFIG_PATH, 600_000);
        final JSONObject custom = repository.getGroups().copy();
        groupsOf(custom).put("auditors", new JSONObject().put("resultSetLimit", 10L));
        repository.save(custom);
        return new Subject() {
          @Override
          public Set<String> inForce() {
            return new TreeSet<>(groupsOf(repository.getGroups()).keySet());
          }

          @Override
          public void reload() {
            repository.reload();
          }

          @Override
          public void dropOneByHand() throws Exception {
            final Path file = Path.of(CONFIG_PATH, fileName);
            final JSONObject document = new JSONObject(Files.readString(file, StandardCharsets.UTF_8));
            groupsOf(document).remove("auditors");
            Files.writeString(file, document.toString(2), StandardCharsets.UTF_8);
          }

          @Override
          public void stop() {
            repository.stop();
          }
        };
      }

      /** The scenario of the issue: the operator's valid document, with only its "version" key missing. */
      @Override
      String wrongShape(final String current) {
        final JSONObject document = new JSONObject(current);
        document.remove("version");
        return document.toString(2);
      }
    },

    TOKENS(ApiTokenConfiguration.FILE_NAME, ApiTokenConfiguration.ERROR_FILE_NAME) {
      @Override
      Subject start() {
        final ApiTokenConfiguration tokens = new ApiTokenConfiguration(CONFIG_PATH);
        tokens.load();
        tokens.createToken("ci", "*", 0, new JSONObject());
        tokens.createToken("backup", "*", 0, new JSONObject());
        return new Subject() {
          @Override
          public Set<String> inForce() {
            final Set<String> names = new TreeSet<>();
            for (final JSONObject token : tokens.listTokens())
              names.add(token.getString("name"));
            return names;
          }

          @Override
          public void reload() {
            tokens.reload();
          }

          @Override
          public void dropOneByHand() throws Exception {
            final Path file = Path.of(CONFIG_PATH, fileName);
            final JSONObject document = new JSONObject(Files.readString(file, StandardCharsets.UTF_8));
            final JSONArray kept = new JSONArray();
            final JSONArray all = document.getJSONArray("tokens");
            for (int i = 0; i < all.length(); i++)
              if (!"ci".equals(all.getJSONObject(i).getString("name")))
                kept.put(all.getJSONObject(i));
            document.put("tokens", kept);
            Files.writeString(file, document.toString(2), StandardCharsets.UTF_8);
          }

          @Override
          public void stop() {
            tokens.stop();
          }
        };
      }

      /** A token document without its "tokens" array: not "no tokens", which is spelled "tokens": []. */
      @Override
      String wrongShape(final String current) {
        return new JSONObject().put("version", 1).toString(2);
      }
    };

    final String fileName;
    final String errorFileName;

    Artifact(final String fileName, final String errorFileName) {
      this.fileName = fileName;
      this.errorFileName = errorFileName;
    }

    abstract Subject start() throws Exception;

    abstract String wrongShape(String current);
  }

  private static JSONObject groupsOf(final JSONObject document) {
    return document.getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
  }
}
