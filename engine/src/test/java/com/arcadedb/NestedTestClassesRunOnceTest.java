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
package com.arcadedb;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every test class that holds tests runs exactly once (#12500).
 * <p>
 * Surefire and Failsafe exclude inner classes by default ({@code **}{@code /*$*}), so a {@code @Nested} class runs through its
 * outer class and nowhere else. An explicit {@code <excludes>} in a plugin's configuration REPLACES that default, and the root pom
 * has one for each plugin (to keep {@code *IT} out of Surefire, and one integration test out of Failsafe). From then on a nested
 * class whose name ends in {@code Test} matches the include patterns of its own: it ran a second time as a top-level class, in the
 * other fork of a lane with {@code -Dsurefire.forkCount=2}, and both runs build the database {@code target/databases/<simple name>}
 * of {@code TestHelper}. One run then deletes the files the other is compacting, and the failure lands on a different test every
 * time ({@code IndexedRangeQueryTest$OutOfRangeSeekTest}: "Cannot rename temporary index file ... .temp_nuctidx").
 * <p>
 * The authority is the pom: what the plugins include is Surefire's and Failsafe's documented default unless the pom names its own
 * {@code <includes>}, and what they exclude is read from the root {@code pom.xml}, so a pattern written some other way is judged
 * by what it matches, not by its spelling. The range is the compiled test classes of this module: every class with a {@code $} in
 * its name that the include patterns match and that holds a test method. Each must run once, alone or through its outer class:
 * twice is this issue, never means the exclusion silently stopped a test that only ran as a class of its own. The lanes pass
 * {@code -Dsurefire.includes=**}{@code /*Test.java}, a subset of the default checked here. Only this module's classes are
 * reachable from here, and it is the module with {@code @Nested} classes named like tests; the pom check holds for every module,
 * which inherits it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class NestedTestClassesRunOnceTest {
  /** Where Surefire looks when the pom names no {@code <includes>}, from the plugin's documentation. */
  private static final List<String> SUREFIRE_DEFAULT_INCLUDES = List.of("**/Test*.java", "**/*Test.java", "**/*Tests.java", "**/*TestCase.java");
  /** Where Failsafe looks when the pom names no {@code <includes>}, from the plugin's documentation. */
  private static final List<String> FAILSAFE_DEFAULT_INCLUDES = List.of("**/IT*.java", "**/*IT.java", "**/*ITCase.java");
  /** What both plugins exclude when the pom names no {@code <excludes>}: inner classes. */
  private static final List<String> DEFAULT_EXCLUDES          = List.of("**/*$*");

  @Test
  void surefireRunsANestedTestClassOnlyThroughItsOuterClass() throws Exception {
    final Scan scan = Scan.of("maven-surefire-plugin", SUREFIRE_DEFAULT_INCLUDES);
    final List<Class<?>> candidates = nestedClassesMatchingTheIncludes(scan);

    assertThat(candidates).as("the scan must reach a nested class named like a test: the probe at the end of this class is one, and "
        + "if it is missing the include patterns or the walk of the compiled test classes read nothing").contains(NestedProbeTest.class);
    assertEachRunsOnce(scan, candidates);
  }

  @Test
  void failsafeRunsANestedTestClassOnlyThroughItsOuterClass() throws Exception {
    final Scan scan = Scan.of("maven-failsafe-plugin", FAILSAFE_DEFAULT_INCLUDES);

    assertThat(testClassFiles().stream().filter(scan::matchesTheIncludes).toList())
        .as("the scan must reach the integration tests of this module (*IT): the include patterns of Failsafe matched nothing")
        .isNotEmpty();
    assertEachRunsOnce(scan, nestedClassesMatchingTheIncludes(scan));
  }

  /** The census itself, on a shape it can no longer fail to see: the same patterns the pom is judged by, over a nested class. */
  @Test
  void theScanCountsAnInnerClassTheDefaultExcludeWouldDrop() {
    final Scan surefireWithNoExcludes = new Scan("maven-surefire-plugin", SUREFIRE_DEFAULT_INCLUDES, List.of("**/*IT.java"));
    final Scan surefireWithTheDefault = new Scan("maven-surefire-plugin", SUREFIRE_DEFAULT_INCLUDES, DEFAULT_EXCLUDES);
    final String nested = "com/arcadedb/index/IndexedRangeQueryTest$OutOfRangeSeekTest.class";

    assertThat(surefireWithNoExcludes.selects(nested)).as("an explicit <excludes> without the inner-class pattern selects a nested class").isTrue();
    assertThat(surefireWithTheDefault.selects(nested)).as("the default inner-class pattern drops a nested class").isFalse();
    assertThat(surefireWithTheDefault.selects("com/arcadedb/index/IndexedRangeQueryTest.class")).as("and keeps its outer class").isTrue();
    assertThat(surefireWithTheDefault.selects("com/arcadedb/index/FullTextMoreLikeThisIT.class")).isFalse();
  }

  private static void assertEachRunsOnce(final Scan scan, final List<Class<?>> candidates) {
    final List<String> offenders = new ArrayList<>();
    for (final Class<?> type : candidates) {
      if (!holdsTests(type) || !isRunnable(type))
        continue;

      // Handed to the engine as a class of its own. Jupiter resolves a @Nested class selected directly through its enclosing
      // class (the outer instance is built for it), which is how it ran in the log of #12500, 3 tests on their own; a member
      // class that is neither static nor @Nested is not a test container at all
      final boolean nested = isNestedAllTheWayUp(type);
      final boolean alone = (nested || type.getEnclosingClass() == null || Modifier.isStatic(type.getModifiers())) && scan.selects(fileOf(type));
      final Class<?> outermost = outermostOf(type);
      final boolean throughOuter = nested && isRunnable(outermost) && scan.selects(fileOf(outermost));

      if (alone && throughOuter)
        offenders.add(type.getName() + ": runs twice, alone and through " + outermost.getName());
      else if (!alone && !throughOuter)
        offenders.add(type.getName() + ": never runs, it is not run alone and " + (nested ? outermost.getName() + " is not run"
            : "it is not a @Nested class of a class that is run"));
    }

    assertThat(offenders).as("%s: a test class that holds tests must run exactly once. Twice means the pom's <excludes> replaced the default "
        + "inner-class exclusion %s, and a @Nested class named like a test runs again as a class of its own, in another fork, in the "
        + "database directory of the first (#12500). Never means a class with tests that only ran as a class of its own is now excluded: "
        + "make it a @Nested class of a class that runs, or give it a name the include patterns do not match", scan.plugin, DEFAULT_EXCLUDES)
        .isEmpty();
  }

  /** Every class with a {@code $} in its name that the include patterns of the plugin match, loaded without initializing it. */
  private static List<Class<?>> nestedClassesMatchingTheIncludes(final Scan scan) throws IOException, URISyntaxException, ClassNotFoundException {
    final List<Class<?>> nested = new ArrayList<>();
    for (final String file : testClassFiles())
      if (file.indexOf('$') >= 0 && scan.matchesTheIncludes(file))
        nested.add(Class.forName(file.substring(0, file.length() - ".class".length()).replace('/', '.'), false,
            NestedTestClassesRunOnceTest.class.getClassLoader()));
    return nested;
  }

  /** The compiled test classes of this module, relative to their root and with {@code /} as separator, sorted. */
  private static List<String> testClassFiles() throws IOException, URISyntaxException {
    final Path root = Path.of(NestedTestClassesRunOnceTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertThat(root).as("the compiled test classes of the engine module").isDirectory();
    try (final Stream<Path> files = Files.walk(root)) {
      return files.filter(p -> p.toString().endsWith(".class")).map(p -> root.relativize(p).toString().replace('\\', '/')).sorted().toList();
    }
  }

  private static boolean holdsTests(final Class<?> type) {
    return !ReflectionSupport.findMethods(type, method -> AnnotationSupport.isAnnotated(method, Testable.class), HierarchyTraversalMode.TOP_DOWN)
        .isEmpty();
  }

  /** What Jupiter can instantiate: not abstract, not private. */
  private static boolean isRunnable(final Class<?> type) {
    return !Modifier.isAbstract(type.getModifiers()) && !Modifier.isPrivate(type.getModifiers());
  }

  /** True when {@code type} is a member class and so is every class around it up to the outermost one, each a non-static {@code @Nested}. */
  private static boolean isNestedAllTheWayUp(final Class<?> type) {
    if (type.getEnclosingClass() == null)
      return false;
    for (Class<?> c = type; c.getEnclosingClass() != null; c = c.getEnclosingClass())
      if (!c.isMemberClass() || !c.isAnnotationPresent(Nested.class) || Modifier.isStatic(c.getModifiers()))
        return false;
    return true;
  }

  private static Class<?> outermostOf(final Class<?> type) {
    Class<?> outermost = type;
    while (outermost.getEnclosingClass() != null)
      outermost = outermost.getEnclosingClass();
    return outermost;
  }

  private static String fileOf(final Class<?> type) {
    return type.getName().replace('.', '/') + ".class";
  }

  /** One plugin's scan of the test classes: what its include patterns match, less what the pom's exclude patterns match. */
  private static final class Scan {
    private final String            plugin;
    private final List<PathMatcher> includes;
    private final List<PathMatcher> excludes;

    Scan(final String plugin, final List<String> includePatterns, final List<String> excludePatterns) {
      this.plugin = plugin;
      this.includes = matchersOf(includePatterns);
      this.excludes = matchersOf(excludePatterns);
    }

    /** The scan of {@code plugin} as the root {@code pom.xml} configures it, over the plugin's default patterns where it names none. */
    static Scan of(final String plugin, final List<String> defaultIncludes) throws Exception {
      final Path pom = Path.of(System.getProperty("basedir", "."), "..", "pom.xml").normalize();
      assertThat(pom).as("the root pom of the reactor, one level above the engine module").isRegularFile();

      final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      final Document document = factory.newDocumentBuilder().parse(pom.toFile());

      final XPath xpath = XPathFactory.newInstance().newXPath();
      final String configuration = "//plugin[artifactId='" + plugin + "']";
      assertThat(((NodeList) xpath.evaluate(configuration, document, XPathConstants.NODESET)).getLength())
          .as("the scan must reach the configuration of %s in the root pom", plugin).isPositive();

      final List<String> includes = patternsOf(xpath, document, configuration + "/configuration/includes/include");
      final List<String> excludes = patternsOf(xpath, document, configuration + "/configuration/excludes/exclude");
      return new Scan(plugin, includes.isEmpty() ? defaultIncludes : includes, excludes.isEmpty() ? DEFAULT_EXCLUDES : excludes);
    }

    /** True when an include pattern matches the class file, whatever the exclude patterns say. */
    boolean matchesTheIncludes(final String classFile) {
      return matchesAny(includes, classFile);
    }

    /** True when the plugin hands the class to the test engine as a class of its own. */
    boolean selects(final String classFile) {
      return matchesAny(includes, classFile) && !matchesAny(excludes, classFile);
    }

    private static boolean matchesAny(final List<PathMatcher> matchers, final String classFile) {
      // a dummy parent directory, so that "**/" matches a class of the root package too, as it does in the plugins
      final Path path = Path.of("root", classFile);
      for (final PathMatcher matcher : matchers)
        if (matcher.matches(path))
          return true;
      return false;
    }

    private static List<PathMatcher> matchersOf(final List<String> patterns) {
      final List<PathMatcher> matchers = new ArrayList<>(patterns.size());
      for (final String pattern : patterns) {
        assertThat(pattern).as("a pattern this test cannot read: only plain globs are understood, not the %regex[] or ! forms")
            .doesNotStartWith("%regex").doesNotStartWith("!");
        // the plugins match class files: a pattern naming a source file is read as its class
        final String glob = pattern.endsWith(".java") ? pattern.substring(0, pattern.length() - ".java".length()) + ".class" : pattern;
        matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + glob));
      }
      return matchers;
    }

    private static List<String> patternsOf(final XPath xpath, final Document document, final String expression) throws Exception {
      final NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
      final TreeSet<String> patterns = new TreeSet<>();
      for (int i = 0; i < nodes.getLength(); i++)
        // an item may carry a comma-separated list of patterns
        for (final String pattern : nodes.item(i).getTextContent().split(","))
          if (!pattern.isBlank())
            patterns.add(pattern.strip());
      return List.copyOf(patterns);
    }
  }

  /** The shape the scan must reach: a nested test class that the include patterns of Surefire match, as in the classes of #12500. */
  @Nested
  class NestedProbeTest {
    @Test
    void runsThroughItsOuterClass() {
      assertThat(getClass().getEnclosingClass()).isEqualTo(NestedTestClassesRunOnceTest.class);
    }
  }
}
