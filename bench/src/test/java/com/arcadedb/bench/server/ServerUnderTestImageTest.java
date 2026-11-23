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
package com.arcadedb.bench.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServerUnderTestImageTest {
  @Test
  void aNativeTagIsNative() {
    assertThat(ServerUnderTest.isNativeImage("arcadedata/arcadedb:latest-native")).isTrue();
  }

  @Test
  void aJvmTagIsNotNative() {
    assertThat(ServerUnderTest.isNativeImage("arcadedata/arcadedb:latest")).isFalse();
    assertThat(ServerUnderTest.isNativeImage("arcadedata/arcadedb")).isFalse();
  }

  @Test
  void aDigestPinnedNativeImageIsNative() {
    assertThat(ServerUnderTest.isNativeImage("arcadedata/arcadedb:latest-native@sha256:0123abcd")).isTrue();
  }

  @Test
  void aDigestPinnedJvmImageIsNotNative() {
    assertThat(ServerUnderTest.isNativeImage("arcadedata/arcadedb:latest@sha256:0123abcd")).isFalse();
    assertThat(ServerUnderTest.isNativeImage("arcadedata/arcadedb@sha256:0123abcd")).isFalse();
  }

  @Test
  void aRegistryPortIsNotATag() {
    assertThat(ServerUnderTest.isNativeImage("registry.native:5000/arcadedb")).isFalse();
  }
}
