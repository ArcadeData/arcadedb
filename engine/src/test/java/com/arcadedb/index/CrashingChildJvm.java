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
package com.arcadedb.index;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the {@code main} of a test class in a child JVM on the test classpath, so the child can end with
 * {@code Runtime.halt()}: no shutdown hook, no database close, exactly what a process kill leaves behind on disk. Same
 * launch line as {@code LSMTreeSortedBuildCrashTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CrashingChildJvm {
  public static final int CRASH_EXIT_CODE = 73;

  private CrashingChildJvm() {
  }

  public record Result(int exitCode, String output) {
  }

  public static Result run(final Class<?> mainClass, final long timeoutSeconds, final String... args)
      throws IOException, InterruptedException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final List<String> command = new ArrayList<>();
    command.add(java);
    command.add("-Xms128m");
    command.add("-Xmx1g");
    command.add("--add-exports");
    command.add("java.management/sun.management=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.util.concurrent.atomic=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.nio.channels.spi=ALL-UNNAMED");
    command.add("--add-modules");
    command.add("jdk.incubator.vector");
    command.add("--enable-native-access=ALL-UNNAMED");
    command.add("-cp");
    command.add(classpath);
    command.add(mainClass.getName());
    for (final String arg : args)
      command.add(arg);

    final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(timeoutSeconds, TimeUnit.SECONDS)).as(output).isTrue();
    return new Result(process.exitValue(), output);
  }
}
