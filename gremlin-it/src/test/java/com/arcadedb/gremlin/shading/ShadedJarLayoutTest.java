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
package com.arcadedb.gremlin.shading;

import com.arcadedb.gremlin.ArcadeGraph;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Structural contract of the {@code shaded} uber-jar: every third-party library merged into it is
 * either relocated under {@code com.arcadedb.gremlin.shaded} or explicitly allowed to keep its own
 * coordinates, and Groovy's extension-module descriptors are not published at all.
 *
 * <p>Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/6771">#6771</a>
 * (a bundled {@code groovy-swing} descriptor collided with the consumer's own copy and Groovy
 * aborted with "Conflicting module versions") and
 * <a href="https://github.com/ArcadeData/arcadedb/issues/6793">#6793</a> (bundled-but-unrelocated
 * libraries cannot be excluded or overridden by a consumer, so they silently win or lose against
 * the consumer's own version).
 *
 * <p>The allow-list below is the contract, not an observation: adding a package to it means
 * accepting that consumers can no longer choose their own version of that library.
 */
class ShadedJarLayoutTest {

  /** Package prefixes (in JAR path form) that are deliberately NOT relocated. */
  private static final List<String> ALLOWED_UNRELOCATED = List.of(
      // ArcadeDB's own code, and the relocation target itself.
      "com/arcadedb/",
      // TinkerPop IS the API this jar exposes; relocating it would leave consumers nothing to call.
      // Its own privately shaded Jackson (org/apache/tinkerpop/shaded) travels with it.
      "org/apache/tinkerpop/",
      // TinkerPop's public signatures take commons-configuration2 types, e.g.
      // GraphFactory.open(Configuration). Relocating it turns every such call into a
      // NoSuchMethodError for consumers compiled against plain TinkerPop.
      "org/apache/commons/configuration2/",
      // Same reason: TinkerPop returns javatuples types from public API, e.g.
      // TraversalExplanation.getIntermediates() -> List<Triplet<...>>. Relocating it makes
      // ExplainTest fail with "com.arcadedb.gremlin.shaded.org.javatuples.Pair cannot be cast to
      // org.javatuples.Pair".
      "org/javatuples/",
      // A logging FACADE is meant to be shared with the host application: relocating it would
      // detach the uber-jar's logging from whatever binding the consumer configured.
      "org/slf4j/",
      // Groovy CANNOT be relocated. maven-shade rewrites string constants without respecting
      // package boundaries, so the pattern "groovy" also rewrites ".groovy" (the script
      // extension), "groovy.target.indy" (a system property) and "groovydoc"; the Groovy compiler
      // then fails with "ASM reporting processing error ... Script1.com.arcadedb.gremlin.shaded.groovy".
      // On top of that its META-INF descriptors carry class names as plain text that shade does
      // not rewrite. See GremlinGroovyEngineTest for the runtime half of this contract.
      "groovy/",
      "org/apache/groovy/",
      "org/codehaus/groovy/",
      // Groovy's own privately vendored ANTLR/ASM/picocli - already namespaced by Groovy itself.
      "groovyjarjar");

  private static JarFile shadedJar;

  @BeforeAll
  static void openShadedJar() throws Exception {
    final URL location = ArcadeGraph.class.getProtectionDomain().getCodeSource().getLocation();
    final File file = new File(location.toURI());
    assertThat(file.getName())
        .as("arcadedb-gremlin must be on the test classpath as the shaded uber-jar, not as target/classes")
        .endsWith("-shaded.jar");
    shadedJar = new JarFile(file);
  }

  @AfterAll
  static void closeShadedJar() throws Exception {
    if (shadedJar != null)
      shadedJar.close();
  }

  @Test
  void everyBundledThirdPartyPackageIsRelocated() {
    final Set<String> leaked = new TreeSet<>();

    for (final JarEntry entry : (Iterable<JarEntry>) shadedJar.stream()::iterator) {
      final String name = entry.getName();
      if (entry.isDirectory() || !name.endsWith(".class"))
        continue;

      // A multi-release overlay is a second copy of a class under its ORIGINAL name; shade does
      // not relocate it, so it has to be held to the same contract as the base entries.
      final String path = name.startsWith("META-INF/versions/")
          ? name.substring(name.indexOf('/', "META-INF/versions/".length()) + 1)
          : name;

      if (path.startsWith("com/arcadedb/gremlin/shaded/"))
        continue;
      if (ALLOWED_UNRELOCATED.stream().anyMatch(path::startsWith))
        continue;

      leaked.add(path.substring(0, path.lastIndexOf('/') + 1));
    }

    assertThat(leaked)
        .as("third-party packages bundled under their original names: a consumer can neither "
            + "exclude nor override these (#6793). Relocate them in gremlin/pom.xml, or add them "
            + "to ALLOWED_UNRELOCATED with the reason they must keep their coordinates")
        .isEmpty();
  }

  @Test
  void noGroovyExtensionModuleDescriptorIsPublished() {
    // Single-module properties file at a fixed path: it cannot be merged, so whichever copy
    // survives the shade advertises its moduleVersion to every classpath the uber-jar lands on
    // and collides with the consumer's own copy of that module (#6771).
    assertThat(shadedJar.getEntry("META-INF/groovy/org.codehaus.groovy.runtime.ExtensionModule"))
        .as("bundling an extension-module descriptor makes Groovy abort with 'Conflicting module "
            + "versions' whenever the consumer resolves the same module at another version")
        .isNull();
    assertThat(shadedJar.getEntry("META-INF/services/org.codehaus.groovy.runtime.ExtensionModule")).isNull();
  }

  @Test
  void noSwingUserInterfaceIsBundled() {
    // groovy-console/groovy-swing reach a headless server only as dead weight, and groovy-swing
    // was the module whose descriptor triggered #6771.
    final Set<String> swing = new TreeSet<>();
    for (final JarEntry entry : (Iterable<JarEntry>) shadedJar.stream()::iterator) {
      final String name = entry.getName();
      if (name.startsWith("groovy/swing/") || name.startsWith("groovy/console/"))
        swing.add(name);
    }
    assertThat(swing).isEmpty();
  }

  /**
   * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/7097">#7097</a>.
   * TinkerPop's {@code gremlin-shaded} bundles a relocated copy of whatever Jackson TinkerPop was
   * released with (2.15.2 for 3.8.1) together with Jackson's ORIGINAL {@code META-INF/maven}
   * metadata, so an SBOM/CVE scanner pointed at the distribution reported jackson-core 2.15.2 while
   * every plain Jackson jar in {@code lib/} was the BOM-managed version. gremlin/pom.xml now drops
   * TinkerPop's copy from the shade and rebuilds the same relocation from the BOM version, so this
   * pins that the bytecode that actually runs, the metadata a scanner reads and the version the
   * parent pom manages are one and the same.
   */
  @Test
  void bundledJacksonIsTheBomVersionAndItsMetadataSaysSo() throws Exception {
    final String managed = System.getProperty("jackson.version");
    assertThat(managed)
        .as("gremlin-it's surefire configuration passes ${jackson.version} as a system property")
        .isNotBlank();

    // The bytecode that runs. TinkerPop's own relocation prefix is the target, because gremlin-core
    // and gremlin-util were compiled against it and are not rewritten by our shade.
    assertThat(versionOf("org.apache.tinkerpop.shaded.jackson.core.json.PackageVersion"))
        .as("relocated jackson-core is TinkerPop's 2.15.2 copy, not the BOM version")
        .isEqualTo(managed);
    assertThat(versionOf("org.apache.tinkerpop.shaded.jackson.databind.cfg.PackageVersion"))
        .as("relocated jackson-databind is TinkerPop's 2.15.2 copy, not the BOM version")
        .isEqualTo(managed);

    // The metadata a scanner reads: one pom.properties per bundled Jackson artifact, each at the
    // version of the bytecode next to it. A stale copy left behind by gremlin-shaded would show up
    // here as a second, older jackson-core.
    final Map<String, String> declared = new TreeMap<>();
    for (final JarEntry entry : (Iterable<JarEntry>) shadedJar.stream()::iterator) {
      final String name = entry.getName();
      if (!name.startsWith("META-INF/maven/com.fasterxml.jackson") || !name.endsWith("/pom.properties"))
        continue;
      final Properties properties = new Properties();
      try (final InputStream in = shadedJar.getInputStream(entry)) {
        properties.load(in);
      }
      final String previous = declared.put(properties.getProperty("artifactId"), properties.getProperty("version"));
      assertThat(previous).as("two copies of %s in the uber-jar", name).isNull();
    }
    assertThat(declared)
        .containsOnlyKeys("jackson-annotations", "jackson-core", "jackson-databind")
        .containsEntry("jackson-core", managed)
        .containsEntry("jackson-databind", managed);
    // The BOM versions jackson-annotations per minor release (2.22, not 2.22.2).
    final String[] parts = managed.split("\\.");
    assertThat(declared.get("jackson-annotations")).startsWith(parts[0] + "." + parts[1]);
  }

  /** {@code PackageVersion.VERSION} of a relocated Jackson module, loaded by name because the package is a shade target. */
  private static String versionOf(final String packageVersionClass) throws Exception {
    return Class.forName(packageVersionClass).getField("VERSION").get(null).toString();
  }
}
