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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * jvector 4.0.2 switches its native (Highway, FFM) provider on by default. ArcadeDB keeps it off unless
 * {@code arcadedb.vectorIndex.nativeVectorization} or an explicit {@code -Djvector.enable_native_vectorization} turns it on.
 * <p>
 * jvector chooses its provider once per JVM, so each case runs in a fresh JVM that prints the provider class it got. The
 * native provider needs Java 22+ and a Linux x86_64 or aarch64 jar resource; elsewhere (macOS, Windows, older Java) jvector
 * logs a warning and uses the Java Vector API. The cases that ask for native therefore accept {@code Native}, or {@code Panama}
 * with jvector's logged load failure; the default-off cases always expect {@code Panama}. None is skipped.
 */
class JVectorProviderSelectionTest {
  private static final String PANAMA = "PanamaVectorizationProvider";
  private static final String NATIVE = "NativeVectorizationProvider";

  /**
   * Runs in the child JVMs.
   */
  public static final class Probe {
    public static void main(final String[] args) {
      System.out.println("PROVIDER=" + VectorApiSupport.jvectorProvider().getClass().getSimpleName());
    }
  }

  @Test
  void theDefaultIsTheJavaVectorApi() throws Exception {
    assertThat(providerWith()).isEqualTo(PANAMA);
  }

  @Test
  void theSettingTurnsTheNativeProviderOn() throws Exception {
    assertNativeOrLoggedFallback("-Darcadedb.vectorIndex.nativeVectorization=true");
  }

  @Test
  void theSettingSetToFalseKeepsTheJavaVectorApi() throws Exception {
    assertThat(providerWith("-Darcadedb.vectorIndex.nativeVectorization=false")).isEqualTo(PANAMA);
  }

  @Test
  void anExplicitJVectorPropertyTurnsTheNativeProviderOn() throws Exception {
    assertNativeOrLoggedFallback("-Djvector.enable_native_vectorization=true");
  }

  @Test
  void theDeprecatedJVectorAliasTurnsTheNativeProviderOn() throws Exception {
    assertNativeOrLoggedFallback("-Djvector.experimental.enable_native_vectorization=true");
  }

  @Test
  void anExplicitJVectorPropertyWinsOverTheSetting() throws Exception {
    assertThat(providerWith("-Darcadedb.vectorIndex.nativeVectorization=true", "-Djvector.enable_native_vectorization=false"))
        .isEqualTo(PANAMA);
    assertNativeOrLoggedFallback("-Darcadedb.vectorIndex.nativeVectorization=false", "-Djvector.enable_native_vectorization=true");
  }

  @Test
  void aForcedJVectorProviderWinsOverTheSetting() throws Exception {
    assertThat(providerWith("-Darcadedb.vectorIndex.nativeVectorization=true", "-Djvector.vectorization_provider=panama"))
        .isEqualTo(PANAMA);
  }

  /**
   * The property asks for the native provider. It gets it when jvector can load its library; where it cannot (noexec temp
   * directory, missing permission, unsupported CPU tier, a platform without the library) jvector logs "Native vector API was
   * not enabled" and uses Panama. Either is correct; a Panama result on a platform that has the provider must come with
   * that log line, so the property being ignored is still caught.
   */
  private static void assertNativeOrLoggedFallback(final String... jvmArgs) throws Exception {
    final String[] result = run(jvmArgs);
    assertThat(result[0]).isIn(NATIVE, PANAMA);
    if (result[0].equals(PANAMA) && nativeProviderSupportedHere())
      assertThat(result[1]).as("Panama although native was asked for, with no load failure logged").contains(
          "Native vector API was not enabled");
  }

  /**
   * jvector bundles its native library for Linux on x86_64 and aarch64 only, and loads it through the foreign function API
   * of Java 22 and later.
   */
  private static boolean nativeProviderSupportedHere() {
    final String os = System.getProperty("os.name", "").toLowerCase();
    final String arch = System.getProperty("os.arch", "").toLowerCase();
    return Runtime.version().feature() >= 22 && os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64")
        || arch.equals("aarch64"));
  }

  private static String providerWith(final String... jvmArgs) throws Exception {
    return run(jvmArgs)[0];
  }

  /**
   * Returns the provider class name and the child's whole output.
   */
  private static String[] run(final String... jvmArgs) throws Exception {
    final List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    command.add("--add-modules");
    command.add("jdk.incubator.vector");
    command.add("--enable-native-access=ALL-UNNAMED");
    command.addAll(List.of(jvmArgs));
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(Probe.class.getName());

    final File output = File.createTempFile("jvector-provider", ".out");
    try {
      final ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output);
      // THE PARENT'S OPTIONS MUST NOT LEAK INTO THE CHILD, WHICH TESTS ONE SET OF FLAGS
      builder.environment().remove("JAVA_TOOL_OPTIONS");
      builder.environment().remove("JDK_JAVA_OPTIONS");
      builder.environment().remove("_JAVA_OPTIONS");
      final Process process = builder.start();
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new AssertionError("the probe JVM did not finish: " + command);
      }
      final String text = Files.readString(output.toPath(), StandardCharsets.UTF_8);
      assertThat(process.exitValue()).as(text).isZero();
      return new String[] { text.lines().filter(l -> l.startsWith("PROVIDER=")).map(l -> l.substring("PROVIDER=".length()))
          .findFirst().orElseThrow(() -> new AssertionError("no provider printed: " + text)), text };
    } finally {
      output.delete();
    }
  }
}
