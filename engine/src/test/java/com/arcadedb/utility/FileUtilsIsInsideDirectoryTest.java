/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.utility;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link FileUtils#isInsideDirectory} guards the directories a database may name for a setting, so it has to answer as the file
 * system resolves a path: {@code ..} steps up from where a link led, not from where the link sits. Checked on a tree with links
 * out of and into the directory, against {@link Path#toRealPath} for every path that exists, plus the cases that do not.
 */
class FileUtilsIsInsideDirectoryTest {
  @TempDir
  Path temp;

  private Path root;
  private Path allowed;
  private Path outside;

  @BeforeEach
  void tree() throws IOException {
    root = temp.toRealPath();
    allowed = Files.createDirectories(root.resolve("allowed"));
    outside = Files.createDirectories(root.resolve("outside"));
    Files.createDirectories(allowed.resolve("a"));
    Files.createDirectories(allowed.resolve("b/c"));
    Files.createDirectories(outside.resolve("x"));
    Files.createDirectories(outside.resolve("y/z"));
    Files.writeString(allowed.resolve("a/file"), "x");
    Files.writeString(outside.resolve("x/file"), "x");
    try {
      Files.createSymbolicLink(allowed.resolve("toOut"), Path.of("../outside/y"));
      Files.createSymbolicLink(outside.resolve("toIn"), Path.of("../allowed/b"));
      Files.createSymbolicLink(allowed.resolve("toSelf"), Path.of("."));
      Files.createSymbolicLink(allowed.resolve("rel"), Path.of("a"));
      Files.createSymbolicLink(allowed.resolve("abs"), outside.resolve("x"));
      Files.createSymbolicLink(allowed.resolve("dangling"), Path.of("../nowhere"));
    } catch (final UnsupportedOperationException | IOException e) {
      Assumptions.abort("no symbolic links here: " + e);
    }
  }

  private boolean inside(final Path path) {
    return FileUtils.isInsideDirectory(path.toString(), allowed.toString());
  }

  @Test
  void theDirectoryItselfAndWhatIsBelowItAreInsideAndASiblingWithTheSamePrefixIsNot() throws IOException {
    assertThat(inside(allowed)).isTrue();
    assertThat(inside(allowed.resolve("a"))).isTrue();
    assertThat(inside(allowed.resolve("a/file"))).isTrue();
    assertThat(inside(allowed.resolve("b/c/not-created-yet/deeper"))).isTrue();
    assertThat(inside(root)).isFalse();
    assertThat(inside(outside)).isFalse();
    assertThat(inside(root.resolve("allowed-sibling"))).isFalse();
    Files.createDirectories(root.resolve("allowed2"));
    assertThat(inside(root.resolve("allowed2"))).isFalse();
    assertThat(inside(Path.of("/"))).isFalse();
  }

  @Test
  void dotDotCannotStepOutWhateverTheOrder() {
    assertThat(inside(Path.of(allowed + "/a/../b"))).isTrue();
    assertThat(inside(Path.of(allowed + "/a/./../b/."))).isTrue();
    assertThat(inside(Path.of(allowed + "/a/.."))).isTrue();
    assertThat(inside(Path.of(allowed + "/.."))).isFalse();
    assertThat(inside(Path.of(allowed + "/a/../.."))).isFalse();
    assertThat(inside(Path.of(allowed + "/a/../../outside/x"))).isFalse();
    assertThat(inside(Path.of(allowed + "/nothing/../../outside"))).isFalse();
    assertThat(inside(Path.of(allowed + "/../allowed/a"))).isTrue();
    // above the root there is nothing: the parent of the root is the root
    assertThat(FileUtils.isInsideDirectory("/../../..", "/")).isTrue();
  }

  @Test
  void aLinkLeadsWhereItPointsAndDotDotStepsUpFromThere() {
    // a link out of the directory
    assertThat(inside(allowed.resolve("toOut"))).isFalse();
    assertThat(inside(allowed.resolve("toOut/z"))).isFalse();
    assertThat(inside(allowed.resolve("abs/file"))).isFalse();
    // .. from the target of a link is the parent of the TARGET, not of the link
    assertThat(inside(Path.of(allowed + "/toOut/.."))).isFalse();
    // out through the link, up twice, and back in by name: inside again
    assertThat(inside(Path.of(allowed + "/toOut/../../allowed/a"))).isTrue();
    assertThat(inside(Path.of(allowed + "/toOut/../../outside"))).isFalse();
    assertThat(inside(Path.of(allowed + "/toSelf/.."))).isFalse();
    // a link inside the directory to a place inside it
    assertThat(inside(allowed.resolve("rel"))).isTrue();
    assertThat(inside(allowed.resolve("rel/file"))).isTrue();
    assertThat(inside(Path.of(allowed + "/rel/../b"))).isTrue();
    assertThat(inside(allowed.resolve("toSelf/a/file"))).isTrue();
    // a link from outside to a place inside the directory is inside
    assertThat(inside(outside.resolve("toIn"))).isTrue();
    assertThat(inside(outside.resolve("toIn/c"))).isTrue();
    assertThat(inside(Path.of(outside + "/toIn/.."))).isTrue();
    // the directory may itself be named through a link
    assertThat(FileUtils.isInsideDirectory(outside.resolve("toIn/c").toString(), outside.resolve("toIn").toString())).isTrue();
    assertThat(FileUtils.isInsideDirectory(allowed.resolve("b/c").toString(), outside.resolve("toIn").toString())).isTrue();
    assertThat(FileUtils.isInsideDirectory(allowed.resolve("a").toString(), outside.resolve("toIn").toString())).isFalse();
  }

  @Test
  void aLinkWhoseTargetDoesNotExistIsRefusedBecauseWhereItLeadsCannotBeTold() {
    assertThat(inside(allowed.resolve("dangling"))).isFalse();
    assertThat(inside(allowed.resolve("dangling/deeper"))).isFalse();
  }

  @Test
  void nothingIsInsideForNoPathOrNoDirectoryAndARelativePathReadsFromTheWorkingDirectory() {
    assertThat(FileUtils.isInsideDirectory(null, allowed.toString())).isFalse();
    assertThat(FileUtils.isInsideDirectory(allowed.toString(), null)).isFalse();
    assertThat(FileUtils.isInsideDirectory("", allowed.toString())).isFalse();
    assertThat(FileUtils.isInsideDirectory("  ", allowed.toString())).isFalse();
    assertThat(FileUtils.isInsideDirectory(allowed.toString(), "")).isFalse();
    assertThat(FileUtils.isInsideDirectory(allowed.toString(), " \t")).isFalse();
    // a NUL cannot be part of a path
    assertThat(FileUtils.isInsideDirectory(allowed + "/a\0b", allowed.toString())).isFalse();

    final Path working = Path.of("").toAbsolutePath();
    assertThat(FileUtils.isInsideDirectory("child", working.toString())).isTrue();
    assertThat(FileUtils.isInsideDirectory("./child/../other", ".")).isTrue();
    assertThat(FileUtils.isInsideDirectory("..", ".")).isFalse();
  }

  @Test
  void everyPathThatExistsIsJudgedAsTheFileSystemResolvesIt() throws IOException {
    final String[] names = { "a", "b", "c", "x", "y", "z", "toOut", "toIn", "toSelf", "rel", "abs", "file", "..", "..", ".", "outside",
        "allowed", "missing" };
    final Random random = new Random(0x5ec0L);
    int checked = 0;
    int inside = 0;
    for (int round = 0; round < 6000; round++) {
      final StringBuilder text = new StringBuilder(random.nextBoolean() ? allowed.toString() : outside.toString());
      for (int i = 1 + random.nextInt(6); i > 0; i--)
        text.append('/').append(names[random.nextInt(names.length)]);
      final Path path = Path.of(text.toString());
      final Path real;
      try {
        real = path.toRealPath();
      } catch (final IOException e) {
        // a name that is not there: judged by the cases above, the reference has nothing to say
        continue;
      }
      final boolean expected = real.startsWith(allowed);
      assertThat(FileUtils.isInsideDirectory(path.toString(), allowed.toString())).as(path + " is really " + real).isEqualTo(expected);
      checked++;
      if (expected)
        inside++;
    }
    assertThat(checked).as("random paths that exist").isGreaterThan(500);
    assertThat(inside).as("of them inside").isBetween(50, checked - 50);
  }
}
