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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8204: the scan behind the per-module fixture-port guards must be able to fail. Each case writes a small
 * source tree with one known shape and checks the verdict, so a guard passing on the real tree means the tree is clean,
 * not that the scan is blind.
 */
class PluginPortFixtureScanTest {

  private static final String PLUGIN   = "com.example.FooPlugin";
  private static final String ENTRY    = "\"Foo:" + PLUGIN + "\"";
  private static final Set<String> BASES = Set.of("BaseFooServerTest");

  @TempDir
  Path root;

  @Test
  void aSingleServerFixtureOnTheEphemeralBaseIsClean() throws IOException {
    write("FooIT", "class FooIT extends BaseFooServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); }\n}");

    final PluginPortFixtureScan.Result result = scan();

    assertThat(result.fixtures()).containsExactly("FooIT.java");
    assertThat(result.offenders()).isEmpty();
  }

  @Test
  void aFixtureInheritingTheBaseThroughAnIntermediateClassIsClean() throws IOException {
    write("AbstractFooIT", "public abstract class AbstractFooIT extends BaseFooServerTest {\n}");
    write("FooIT", "class FooIT extends AbstractFooIT {\n  void c() { PLUGINS.setValue(" + ENTRY + "); }\n}");

    assertThat(scan().offenders()).isEmpty();
  }

  @Test
  void aFixtureOnThePlainServerBaseIsFlagged() throws IOException {
    // The shape of the 67 classes issue #8204 reported: the plugin started on its default port.
    write("FooIT", "class FooIT extends BaseGraphServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); }\n}");

    assertThat(scan().offenders()).singleElement().asString().startsWith("FooIT.java: starts the plugin outside");
  }

  @Test
  void aRaftFixtureWithoutAllocatedPortsIsFlagged() throws IOException {
    // The shape of the Bolt and gRPC HA ITs before #7496 / #8203: BASE_PORT + index.
    write("FooHaIT", "class FooHaIT extends BaseRaftHATest {\n  private static final int BASE_FOO_PORT = 57697;\n"
        + "  void c() { config.setValue(PLUGINS, " + ENTRY + "); config.setValue(FOO_PORT.getKey(), String.valueOf(BASE_FOO_PORT + i)); }\n}");

    assertThat(scan().offenders()).containsExactlyInAnyOrder(
        "FooHaIT.java: a Raft cluster fixture that does not take its ports from allocateFixturePorts",
        "FooHaIT.java: keeps a hand-picked FOO_PORT in a constant");
  }

  @Test
  void aRaftFixtureWithAllocatedPortsIsClean() throws IOException {
    write("FooHaIT", "class FooHaIT extends BaseRaftHATest {\n  private final int[] fooPorts = allocateFixturePorts(3);\n"
        + "  void c() { config.setValue(PLUGINS, " + ENTRY + "); config.setValue(FOO_PORT.getKey(), String.valueOf(fooPorts[i])); }\n}");

    assertThat(scan().offenders()).isEmpty();
  }

  @Test
  void aHandPickedPortSettingIsFlaggedEvenOnTheEphemeralBase() throws IOException {
    write("SetIT", "class SetIT extends BaseFooServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); FOO_PORT.setValue(51000); }\n}");
    write("KeyIT", "class KeyIT extends BaseFooServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); c.setValue(FOO_PORT.getKey(), \"51000\"); }\n}");
    write("ZeroIT", "class ZeroIT extends BaseFooServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); FOO_PORT.setValue(0); }\n}");

    assertThat(scan().offenders()).containsExactlyInAnyOrder(
        "KeyIT.java: sets FOO_PORT to a hand-picked number",
        "SetIT.java: sets FOO_PORT to a hand-picked number");
  }

  @Test
  void aBoxedConstantIsFlaggedToo() throws IOException {
    write("BoxedIT", "class BoxedIT extends BaseFooServerTest {\n  private static final Integer FOO_PORT = 51000;\n"
        + "  void c() { PLUGINS.setValue(" + ENTRY + "); }\n}");

    assertThat(scan().offenders()).containsExactly("BoxedIT.java: keeps a hand-picked FOO_PORT in a constant");
  }

  @Test
  void sameNamedClassesInDifferentPackagesAreBothScanned() throws IOException {
    // Keyed by file name, the second FooIT read would overwrite the first and hide its violation.
    write("a/FooIT", "class FooIT extends BaseFooServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); }\n}");
    write("b/FooIT", "class FooIT extends BaseGraphServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); }\n}");

    final PluginPortFixtureScan.Result result = scan();

    assertThat(result.fixtures()).containsExactly("a/FooIT.java", "b/FooIT.java");
    assertThat(result.offenders()).singleElement().asString().startsWith("b/FooIT.java: starts the plugin outside");
  }

  @Test
  void dialingTheProductionDefaultIsFlagged() throws IOException {
    write("DialIT", "class DialIT extends BaseFooServerTest {\n  void c() { PLUGINS.setValue(" + ENTRY + "); connect(\"localhost\", 9999); }\n}");

    assertThat(scan().offenders()).containsExactly("DialIT.java: dials the production default port 9999");
  }

  @Test
  void aClassThatNeverStartsThePluginIsNotAFixture() throws IOException {
    // A pure client test may name the default port: it never starts a server that could collide.
    write("ClientTest", "class ClientTest {\n  void c() { new Client(\"localhost\", 9999); }\n}");

    final PluginPortFixtureScan.Result result = scan();

    assertThat(result.fixtures()).isEmpty();
    assertThat(result.offenders()).isEmpty();
  }

  private PluginPortFixtureScan.Result scan() throws IOException {
    return PluginPortFixtureScan.scan(root, PLUGIN, "FOO_PORT", 9999, BASES);
  }

  private void write(final String className, final String body) throws IOException {
    final Path file = root.resolve(className + ".java");
    Files.createDirectories(file.getParent());
    Files.writeString(file, "package x;\n\n" + body + "\n", StandardCharsets.UTF_8);
  }
}
