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
package com.arcadedb.bench.env;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Runs a short host command with a deadline. The output goes to a temporary file rather than a pipe, so a command that hangs
 * without writing cannot block the caller past the deadline on a read.
 */
public final class Processes {
  private Processes() {
  }

  /**
   * @return the combined stdout and stderr of the command, or null when it cannot start, outlives the timeout (it is then
   * killed) or exits with a non-zero status
   */
  public static String output(final Duration timeout, final String... command) {
    Path out = null;
    Process process = null;
    try {
      out = Files.createTempFile("bench-process", ".out");
      process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(out.toFile()).start();
      if (!process.waitFor(timeout.toNanos(), TimeUnit.NANOSECONDS) || process.exitValue() != 0)
        return null;
      return Files.readString(out, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      return null;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } finally {
      if (process != null && process.isAlive())
        process.destroyForcibly();
      if (out != null)
        try {
          Files.deleteIfExists(out);
        } catch (final IOException ignored) {
          // a leftover temporary file is harmless
        }
    }
  }
}
