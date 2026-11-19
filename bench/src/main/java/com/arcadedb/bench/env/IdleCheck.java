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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;

/**
 * Decides whether the host is quiet enough to benchmark on.
 */
public final class IdleCheck {
  public static final double DEFAULT_MAX_LOAD_PER_CORE = 0.10;
  public static final long   DEFAULT_MAX_MEM_USED_MB   = 8192;

  private IdleCheck() {
  }

  public static Optional<String> check(final double load1, final long memUsedMb, final int cores, final double maxLoadPerCore,
      final long maxMemUsedMb) {
    final double loadLimit = maxLoadPerCore * cores;
    if (load1 > loadLimit)
      return Optional.of(String.format(Locale.ROOT, "load %.2f above %.2f", load1, loadLimit));
    if (memUsedMb > maxMemUsedMb)
      return Optional.of("memory used " + memUsedMb + " MB above " + maxMemUsedMb + " MB");
    return Optional.empty();
  }

  /**
   * Checks the current host with the default thresholds. Returns empty when /proc is not available (non-Linux).
   */
  public static Optional<String> checkHost() {
    try {
      final Path loadavg = Path.of("/proc/loadavg");
      final Path meminfo = Path.of("/proc/meminfo");
      if (!Files.isReadable(loadavg) || !Files.isReadable(meminfo))
        return Optional.empty();
      final double load1 = Double.parseDouble(Files.readString(loadavg).trim().split("\\s+")[0]);
      long totalKb = -1;
      long availKb = -1;
      for (final String line : Files.readAllLines(meminfo)) {
        if (line.startsWith("MemTotal:"))
          totalKb = parseKb(line);
        else if (line.startsWith("MemAvailable:"))
          availKb = parseKb(line);
      }
      if (totalKb < 0 || availKb < 0)
        return Optional.empty();
      return check(load1, (totalKb - availKb) / 1024, Runtime.getRuntime().availableProcessors(), DEFAULT_MAX_LOAD_PER_CORE,
          DEFAULT_MAX_MEM_USED_MB);
    } catch (final IOException | RuntimeException e) {
      return Optional.empty();
    }
  }

  private static long parseKb(final String line) {
    return Long.parseLong(line.split("\\s+")[1]);
  }
}
