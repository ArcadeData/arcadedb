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
package com.arcadedb.server.http;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoutePathNormalizerTest {

  @Test
  void collapsesAParameterizedTemplateToItsRegisteredPrefix() {
    assertThat(RoutePathNormalizer.normalize("/api/v1/cluster/peer/{peerId}"))
        .isEqualTo("/api/v1/cluster/peer/");
  }

  @Test
  void leavesAParameterlessTemplateUnchanged() {
    assertThat(RoutePathNormalizer.normalize("/api/v1/cluster")).isEqualTo("/api/v1/cluster");
  }

  @Test
  void collapsesTwoTemplatesUnderTheSamePrefixToOneEntry() {
    assertThat(RoutePathNormalizer.normalize(Set.of(
        "/api/v1/ha/snapshot/{database}", "/api/v1/ha/snapshot/{database}/checksums")))
        .containsExactly("/api/v1/ha/snapshot/");
  }

  /**
   * Issue #4896's required self-test: proves the actual-vs-declared comparison every per-plugin
   * anti-drift test relies on genuinely fails when the two sides disagree, in both directions -
   * not just that it passes when they happen to agree.
   */
  @Test
  void detectsARouteRegisteredButNotDeclared() {
    final Set<String> actual = Set.of("/api/v1/cluster", "/api/v1/cluster/undocumented");
    final Set<String> declared = RoutePathNormalizer.normalize(Set.of("/api/v1/cluster"));

    assertThatThrownBy(() -> assertThat(actual).containsExactlyInAnyOrderElementsOf(declared))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void detectsADeclaredPathWithNoRegisteredRoute() {
    final Set<String> actual = Set.of("/api/v1/cluster");
    final Set<String> declared = RoutePathNormalizer.normalize(Set.of("/api/v1/cluster", "/api/v1/cluster/stale"));

    assertThatThrownBy(() -> assertThat(actual).containsExactlyInAnyOrderElementsOf(declared))
        .isInstanceOf(AssertionError.class);
  }
}
