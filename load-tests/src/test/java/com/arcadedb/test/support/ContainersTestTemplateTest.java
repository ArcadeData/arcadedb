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
package com.arcadedb.test.support;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same suites run against the JVM image and the native one, so the server must be configured in a way both read:
 * settings as environment variables, heap options where each image's entrypoint takes them. No Docker needed: the
 * containers are only built and inspected, never started.
 */
class ContainersTestTemplateTest {

  @Test
  void nativeDetectionUsesTagOnly() {
    assertThat(ContainersTestTemplate.isNativeTag("arcadedata/arcadedb:latest-native")).isTrue();
    assertThat(ContainersTestTemplate.isNativeTag("arcadedata/arcadedb:26.11.1-native-amd64")).isTrue();
    assertThat(ContainersTestTemplate.isNativeTag("localhost:5000/arcadedata/arcadedb:latest-native")).isTrue();
    assertThat(ContainersTestTemplate.isNativeTag("arcadedata/arcadedb:latest")).isFalse();
    assertThat(ContainersTestTemplate.isNativeTag("myregistry/arcadedb-native:latest")).isFalse();
    assertThat(ContainersTestTemplate.isNativeTag("localhost:5000/arcadedb-native")).isFalse();
  }

  /** The JVM image starts through its CMD ./bin/server.sh: a command would replace it and the server never starts. */
  @Test
  void jvmImageGetsMemoryEnvAndNoCommand() {
    final GenericContainer<?> container = ContainersTestTemplate.configureServer(
        new GenericContainer<>("arcadedata/arcadedb:latest"),
        Map.of("arcadedb.server.name", "arcadedb-0", "arcadedb.ha.enabled", "true"), "-Xms2G -Xmx2G", false);

    assertThat(container.getEnvMap())
        .containsEntry("arcadedb.server.name", "arcadedb-0")
        .containsEntry("arcadedb.ha.enabled", "true")
        .containsEntry("ARCADEDB_OPTS_MEMORY", "-Xms2G -Xmx2G");
    assertThat(container.getCommandParts()).isEmpty();
  }

  /** The native entrypoint is the binary itself: heap options reach it only as arguments, one per option. */
  @Test
  void nativeImageSplitsMemoryOptsIntoCommandArgs() {
    final GenericContainer<?> container = ContainersTestTemplate.configureServer(
        new GenericContainer<>("arcadedata/arcadedb:latest-native"),
        Map.of("arcadedb.server.name", "arcadedb-0"), "  -Xms4G   -Xmx4G -Dfoo=bar ", true);

    assertThat(container.getEnvMap())
        .containsEntry("arcadedb.server.name", "arcadedb-0")
        .doesNotContainKey("ARCADEDB_OPTS_MEMORY");
    assertThat(container.getCommandParts()).containsExactly("-Xms4G", "-Xmx4G", "-Dfoo=bar");
  }

  /** On the JVM a -D system property beats the environment variable of the same setting, so no JAVA_OPTS may linger. */
  @Test
  void noJavaOptsOnEitherImage() {
    for (final boolean nativeImage : new boolean[] { false, true })
      assertThat(ContainersTestTemplate.configureServer(new GenericContainer<>("arcadedata/arcadedb:latest"),
          Map.of("arcadedb.server.name", "n"), "-Xmx1G", nativeImage).getEnvMap()).doesNotContainKey("JAVA_OPTS");
  }
}
