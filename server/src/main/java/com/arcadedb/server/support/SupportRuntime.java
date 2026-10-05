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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Detects whether the server runs in a container: {@code KUBERNETES_SERVICE_HOST} in the environment means
 * {@code kubernetes}; otherwise {@code /.dockerenv} or a docker/containerd control group of process 1 means {@code docker};
 * otherwise {@code none}. The evidence that decided it is recorded.
 */
public final class SupportRuntime {
  private SupportRuntime() {
  }

  public static JSONObject detect() {
    return detect(System.getenv(), path -> Files.exists(Paths.get(path)), SupportRuntime::readCgroup);
  }

  /**
   * @param cgroup the content of {@code /proc/1/cgroup}, or {@code null} when it cannot be read
   */
  public static JSONObject detect(final Map<String, String> environment, final Predicate<String> fileExists,
      final Supplier<String> cgroup) {
    final JSONArray evidence = new JSONArray();
    String container = "none";

    final String kubernetes = environment.get("KUBERNETES_SERVICE_HOST");
    if (kubernetes != null && !kubernetes.isBlank()) {
      container = "kubernetes";
      evidence.put("KUBERNETES_SERVICE_HOST");
    } else {
      if (fileExists.test("/.dockerenv")) {
        container = "docker";
        evidence.put("/.dockerenv");
      }
      final String content = cgroup.get();
      if (content != null) {
        final String lower = content.toLowerCase(Locale.ROOT);
        if (lower.contains("docker")) {
          container = "docker";
          evidence.put("/proc/1/cgroup: docker");
        } else if (lower.contains("containerd")) {
          container = "docker";
          evidence.put("/proc/1/cgroup: containerd");
        }
      }
    }

    return new JSONObject().put("container", container).put("evidence", evidence);
  }

  private static String readCgroup() {
    final Path path = Paths.get("/proc/1/cgroup");
    try {
      return Files.isReadable(path) ? Files.readString(path, StandardCharsets.UTF_8) : null;
    } catch (final IOException | RuntimeException e) {
      return null;
    }
  }
}
