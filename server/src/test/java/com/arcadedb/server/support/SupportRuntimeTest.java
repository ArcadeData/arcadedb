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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SupportRuntimeTest {

  @Test
  void kubernetesFromTheEnvironment() {
    final JSONObject runtime = SupportRuntime.detect(Map.of("KUBERNETES_SERVICE_HOST", "10.0.0.1"), p -> true, () -> "12:cpu:/docker/abc");
    assertThat(runtime.getString("container")).isEqualTo("kubernetes");
    assertThat(runtime.getJSONArray("evidence").toList()).containsExactly("KUBERNETES_SERVICE_HOST");
  }

  @Test
  void dockerFromDockerenv() {
    final JSONObject runtime = SupportRuntime.detect(Map.of(), Set.of("/.dockerenv")::contains, () -> null);
    assertThat(runtime.getString("container")).isEqualTo("docker");
    assertThat(runtime.getJSONArray("evidence").toList()).containsExactly("/.dockerenv");
  }

  @Test
  void dockerFromTheCgroup() {
    JSONObject runtime = SupportRuntime.detect(Map.of(), p -> false, () -> "0::/system.slice/docker-abc.scope");
    assertThat(runtime.getString("container")).isEqualTo("docker");
    assertThat(runtime.getJSONArray("evidence").toList()).containsExactly("/proc/1/cgroup: docker");

    runtime = SupportRuntime.detect(Map.of(), p -> false, () -> "0::/kubepods/besteffort/cri-containerd-abc.scope");
    assertThat(runtime.getString("container")).isEqualTo("docker");
    assertThat(runtime.getJSONArray("evidence").toList()).containsExactly("/proc/1/cgroup: containerd");
  }

  @Test
  void bothDockerEvidencesAreRecorded() {
    final JSONObject runtime = SupportRuntime.detect(Map.of(), Set.of("/.dockerenv")::contains, () -> "1:name=systemd:/docker/abc");
    assertThat(runtime.getJSONArray("evidence").length()).isEqualTo(2);
  }

  @Test
  void noneOtherwise() {
    final JSONObject runtime = SupportRuntime.detect(Map.of("PATH", "/bin"), p -> false, () -> "0::/user.slice/session-1.scope");
    assertThat(runtime.getString("container")).isEqualTo("none");
    assertThat(runtime.getJSONArray("evidence").length()).isZero();
  }

  @Test
  void blankKubernetesVariableIsIgnored() {
    assertThat(SupportRuntime.detect(Map.of("KUBERNETES_SERVICE_HOST", " "), p -> false, () -> null).getString("container"))
        .isEqualTo("none");
  }

  @Test
  void theRealDetectionAnswersOneOfTheKnownValues() {
    assertThat(SupportRuntime.detect().getString("container")).isIn("none", "docker", "kubernetes", "unknown");
  }
}
