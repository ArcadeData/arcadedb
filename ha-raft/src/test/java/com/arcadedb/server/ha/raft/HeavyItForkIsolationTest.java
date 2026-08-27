/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the fork-isolation split introduced by issue #6343: the {@code ha-heavy} tag routes an IT to the second
 * failsafe execution in {@code ha-raft/pom.xml}, where it gets a JVM of its own instead of leaving whatever it
 * churned behind for the next 118 classes to inherit.
 * <p>
 * That routing is invisible at runtime. A class that loses the tag does not fail, does not warn, and does not
 * slow down - it simply rejoins the shared fork, and the isolation the split was built for is gone with nothing
 * to show for it. Nothing in the build catches that: the pom's {@code failIfNoTests} is evaluated against the
 * class scan, before the tag filter runs, so a {@code <groups>} expression that matches none of the classes the
 * scan found reports "Tests run: 0" and passes (measured against failsafe 3.5.6; the pom comment records both
 * halves of that experiment). This test is the guard, and it is a finer one than any build setting could be: it
 * also catches four classes keeping the tag while the fifth quietly does not.
 * <p>
 * The invariant is asserted in both directions, as a set equality rather than five containment checks. Losing the
 * tag is the regression; gaining it is a cost - an extra JVM start on a lane with roughly twenty minutes of
 * headroom - and neither should happen without somebody editing this list on purpose. The expected side is
 * written as class literals, so deleting or renaming one of these ITs breaks the build at compile time with the
 * name in the error, rather than here with a string that no longer matches anything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HeavyItForkIsolationTest {

  private static final String HEAVY_TAG = "ha-heavy";

  /**
   * The heavy set, chosen for what a class leaves behind in a shared JVM rather than for its wall clock. See the
   * failsafe configuration in {@code ha-raft/pom.xml} for the measurement each of these is justified by.
   */
  private static final Set<Class<?>> EXPECTED_HEAVY = Set.of(
      RaftMigrationCompactionRaceIT.class,
      RaftBulkInsertCompactionRaceIT.class,
      RaftHARandomCrashIT.class,
      RaftIndexCompactionReplicationIT.class,
      RaftHAComprehensiveIT.class,
      // Not heavy - it does nothing but read two system properties. It is tagged because it is the check that
      // the tag still routes anywhere, and it can only make that check from inside the fork it is asserting
      // about. See HeavyForkWiringIT.
      HeavyForkWiringIT.class);

  @Test
  void exactlyTheDeclaredItsRunInAForkOfTheirOwn() {
    final Set<String> tagged = taggedHeavyInThisPackage();

    assertThat(tagged)
        .as("""
            The set of @Tag("%s") ITs has drifted from the set the fork split was sized for.

            A class that lost the tag is back in the shared fork, silently: nothing fails, nothing warns, and the
            isolation issue #6343 built is gone. A class that gained it costs an extra JVM start on a lane with
            about twenty minutes of headroom.

            If the change was deliberate, update EXPECTED_HEAVY here and the rationale in ha-raft/pom.xml
            together - the list and the reason it exists have to stay in the same commit.""".formatted(HEAVY_TAG))
        .containsExactlyInAnyOrderElementsOf(EXPECTED_HEAVY.stream().map(Class::getName).collect(Collectors.toSet()));
  }

  @Test
  void theHeavySetIsNotEmpty() {
    // Not a tautology over the constant above: it is the assertion that the scan below can see anything at all.
    // If the classpath layout ever changes so that taggedHeavyInThisPackage() finds nothing, the test above would
    // still fail - but with a message about drift, pointing at the annotations instead of at the scan. This one
    // says which of the two broke.
    assertThat(EXPECTED_HEAVY).isNotEmpty();
    assertThat(taggedHeavyInThisPackage()).isNotEmpty();
  }

  /**
   * Every compiled class in this package that carries the heavy tag, by name.
   * <p>
   * Loaded with {@code initialize=false}: these are IT classes whose static initializers stand up servers and
   * touch the filesystem, and this test wants their annotations, not their behaviour.
   */
  private static Set<String> taggedHeavyInThisPackage() {
    final String pkg = HeavyItForkIsolationTest.class.getPackageName();
    final Path root = compiledTestClassesRoot().resolve(pkg.replace('.', '/'));
    assertThat(root).as("compiled test classes for package %s", pkg).isDirectory();

    try (Stream<Path> classFiles = Files.list(root)) {
      return classFiles
          .map(p -> p.getFileName().toString())
          .filter(name -> name.endsWith(".class") && !name.contains("$"))
          .map(name -> pkg + '.' + name.substring(0, name.length() - ".class".length()))
          .filter(HeavyItForkIsolationTest::carriesHeavyTag)
          .collect(Collectors.toCollection(LinkedHashSet::new));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Fails closed. Returning {@code false} for a class that would not load would drop it from the scanned set, and
   * a drift test that quietly forgets a class it could not read is worse than no drift test: a tag genuinely
   * going missing and a class failing to load would report the same way round, and the second would be the one
   * that looked like success. The scan only ever sees {@code .class} files this module just compiled into its own
   * package, so a load failure here means something is wrong that is worth stopping for and naming.
   */
  private static boolean carriesHeavyTag(final String className) {
    try {
      final Class<?> loaded = Class.forName(className, false, HeavyItForkIsolationTest.class.getClassLoader());
      return Arrays.stream(loaded.getAnnotationsByType(Tag.class)).anyMatch(t -> HEAVY_TAG.equals(t.value()));
    } catch (final ClassNotFoundException | LinkageError e) {
      throw new IllegalStateException(
          "cannot read the annotations of " + className + ", which this module compiled into its own test classes; "
              + "the ha-heavy drift check cannot be trusted while that is true", e);
    }
  }

  private static Path compiledTestClassesRoot() {
    try {
      return Path.of(HeavyItForkIsolationTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("cannot locate the compiled test classes", e);
    }
  }
}
