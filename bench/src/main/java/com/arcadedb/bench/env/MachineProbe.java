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

import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Describes the machine a run executes on. Anything not readable is reported as "unknown", never an exception.
 */
public final class MachineProbe {
  private static final String UNKNOWN = "unknown";

  private MachineProbe() {
  }

  public static JSONObject probe(final String machineId) {
    final JSONObject json = new JSONObject();
    json.put("id", machineId);
    json.put("cpu", cpuModel());
    json.put("cores", Runtime.getRuntime().availableProcessors());
    json.put("memGb", memGb());
    json.put("os", osName());
    json.put("kernel", kernel());
    json.put("governor", readFirstLine("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"));
    json.put("noTurbo", readFirstLine("/sys/devices/system/cpu/intel_pstate/no_turbo"));
    return json;
  }

  private static String cpuModel() {
    final String value = readKey("/proc/cpuinfo", "model name");
    return value != null ? value : UNKNOWN;
  }

  private static Object memGb() {
    final String value = readKey("/proc/meminfo", "MemTotal");
    if (value == null)
      return UNKNOWN;
    try {
      return Math.round(Long.parseLong(value.split("\\s+")[0]) / 1024.0 / 1024.0);
    } catch (final NumberFormatException e) {
      return UNKNOWN;
    }
  }

  private static String osName() {
    final String pretty = readKey("/etc/os-release", "PRETTY_NAME");
    if (pretty != null)
      return pretty.replace("\"", "");
    final String name = System.getProperty("os.name");
    final String version = System.getProperty("os.version");
    if (name == null)
      return UNKNOWN;
    return version == null ? name : name + " " + version;
  }

  private static String kernel() {
    final String release = readFirstLine("/proc/sys/kernel/osrelease");
    if (!UNKNOWN.equals(release))
      return release;
    try {
      final Process process = new ProcessBuilder("uname", "-r").redirectErrorStream(true).start();
      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        final String line = reader.readLine();
        process.waitFor(2, TimeUnit.SECONDS);
        return line == null || line.isBlank() ? UNKNOWN : line.trim();
      } finally {
        process.destroy();
      }
    } catch (final IOException e) {
      return UNKNOWN;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return UNKNOWN;
    }
  }

  private static String readFirstLine(final String file) {
    try {
      final String content = Files.readString(Path.of(file)).trim();
      return content.isEmpty() ? UNKNOWN : content.lines().findFirst().orElse(UNKNOWN);
    } catch (final IOException | RuntimeException e) {
      return UNKNOWN;
    }
  }

  private static String readKey(final String file, final String key) {
    try {
      for (final String line : Files.readAllLines(Path.of(file))) {
        final int idx = line.indexOf(':') >= 0 ? line.indexOf(':') : line.indexOf('=');
        if (idx > 0 && line.substring(0, idx).trim().equals(key))
          return line.substring(idx + 1).trim();
      }
    } catch (final IOException | RuntimeException e) {
      // not readable: caller reports unknown
    }
    return null;
  }
}
