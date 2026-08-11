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
package com.arcadedb.query.polyglot;

import com.arcadedb.schema.trigger.ScriptTriggerExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for GHSA-wx28-2265-f788: allow-list entries must be matched as literal patterns and never compiled as
 * regular expressions, and the built-in deny-list must win over any allow-list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HostClassLookupFilterTest {

  @Test
  void singleStarCoversOnlyItsOwnPackageLevel() {
    assertThat(HostClassLookupFilter.matches("java.util.ArrayList", "java.util.*")).isTrue();
    assertThat(HostClassLookupFilter.matches("java.util.Map$Entry", "java.util.*")).isTrue();
    assertThat(HostClassLookupFilter.matches("java.util.zip.ZipFile", "java.util.*")).isFalse();
    assertThat(HostClassLookupFilter.matches("java.util.jar.JarFile", "java.util.*")).isFalse();
    assertThat(HostClassLookupFilter.matches("java.util.concurrent.atomic.AtomicLong", "java.util.*")).isFalse();
  }

  @Test
  void doubleStarIsRecursive() {
    assertThat(HostClassLookupFilter.matches("java.time.LocalDate", "java.time.**")).isTrue();
    assertThat(HostClassLookupFilter.matches("java.time.format.DateTimeFormatter", "java.time.**")).isTrue();
    assertThat(HostClassLookupFilter.matches("java.timezone.Foo", "java.time.**")).isFalse();
  }

  @Test
  void patternWithoutWildcardIsAnExactClassName() {
    assertThat(HostClassLookupFilter.matches("java.math.BigDecimal", "java.math.BigDecimal")).isTrue();
    assertThat(HostClassLookupFilter.matches("java.math.BigDecimalX", "java.math.BigDecimal")).isFalse();
    assertThat(HostClassLookupFilter.matches("java.math.BigInteger", "java.math.BigDecimal")).isFalse();
  }

  /**
   * Issue #6045 code review: a {@code Type$*} nested-type entry ends with {@code *} like a package wildcard, but
   * pins every nested type of one specific enclosing class rather than a package - it is precise, not a wildcard,
   * and must not be classified as one (only a wildcard match can be defeated by
   * {@link HostClassLookupFilter#SAFE_MARKER_ANCESTORS}).
   */
  @Test
  void wildcardPatternExcludesNestedTypePatterns() {
    assertThat(HostClassLookupFilter.isWildcardPattern("java.io.**")).isTrue();
    assertThat(HostClassLookupFilter.isWildcardPattern("java.util.*")).isTrue();
    assertThat(HostClassLookupFilter.isWildcardPattern("java.lang.ProcessBuilder$*")).isFalse();
    assertThat(HostClassLookupFilter.isWildcardPattern("java.util.ServiceLoader$*")).isFalse();
    assertThat(HostClassLookupFilter.isWildcardPattern("java.math.BigDecimal")).isFalse();
  }

  @Test
  void dotsAreLiteralAndNotRegexWildcards() {
    // "java.util.*" as a regex matched java<any char>util<anything>; literally it must not.
    assertThat(HostClassLookupFilter.matches("javaXutilY", "java.util.*")).isFalse();
    assertThat(HostClassLookupFilter.matches("javaxutil.Anything", "java.util.**")).isFalse();
    assertThat(HostClassLookupFilter.matches("java_math.BigDecimal", "java.math.BigDecimal")).isFalse();
  }

  @Test
  void anEmptyAllowListDisablesHostClassLookup() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(null, null);
    assertThat(filter.test("java.util.ArrayList")).isFalse();
    assertThat(filter.test("java.lang.String")).isFalse();
  }

  @Test
  void theBuiltInDenyListOverridesTheAllowList() {
    // Even an embedder that allows the whole JDK cannot reach a process, a file, a socket or the class loader.
    final HostClassLookupFilter filter = new HostClassLookupFilter(List.of("java.**", "javax.**", "jdk.**"), null);

    assertThat(filter.test("java.util.ArrayList")).isTrue();
    assertThat(filter.test("java.lang.String")).isTrue();

    assertThat(filter.test("java.lang.Runtime")).isFalse();
    assertThat(filter.test("java.lang.ProcessBuilder")).isFalse();
    assertThat(filter.test("java.lang.ProcessBuilder$Redirect")).isFalse();
    assertThat(filter.test("java.lang.System")).isFalse();
    assertThat(filter.test("java.lang.Class")).isFalse();
    assertThat(filter.test("java.lang.ClassLoader")).isFalse();
    assertThat(filter.test("java.lang.Thread")).isFalse();
    assertThat(filter.test("java.lang.reflect.Method")).isFalse();
    assertThat(filter.test("java.lang.invoke.MethodHandles")).isFalse();
    assertThat(filter.test("java.io.File")).isFalse();
    assertThat(filter.test("java.io.FileInputStream")).isFalse();
    assertThat(filter.test("java.nio.file.Files")).isFalse();
    assertThat(filter.test("java.net.Socket")).isFalse();
    assertThat(filter.test("java.net.URL")).isFalse();
    assertThat(filter.test("java.util.zip.ZipFile")).isFalse();
    assertThat(filter.test("java.util.jar.JarFile")).isFalse();
    assertThat(filter.test("java.util.logging.FileHandler")).isFalse();
    assertThat(filter.test("java.util.concurrent.ThreadPoolExecutor")).isFalse();
    assertThat(filter.test("java.util.Formatter")).isFalse();
    assertThat(filter.test("java.util.Scanner")).isFalse();
    assertThat(filter.test("java.util.Timer")).isFalse();
    assertThat(filter.test("java.util.ServiceLoader")).isFalse();
    assertThat(filter.test("java.security.AccessController")).isFalse();
    assertThat(filter.test("java.sql.DriverManager")).isFalse();
    assertThat(filter.test("javax.script.ScriptEngineManager")).isFalse();
    assertThat(filter.test("javax.tools.ToolProvider")).isFalse();
    assertThat(filter.test("javax.naming.InitialContext")).isFalse();
    assertThat(filter.test("jdk.internal.misc.Unsafe")).isFalse();
  }

  @Test
  void callerSuppliedRestrictionsAreAddedToTheDenyList() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(List.of("java.util.*"), List.of("java.util.Random"));

    assertThat(filter.test("java.util.ArrayList")).isTrue();
    assertThat(filter.test("java.util.Random")).isFalse();
  }

  @Test
  void theTriggerAllowListAdmitsValueTypesOnly() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(ScriptTriggerExecutor.ALLOWED_PACKAGES, null);

    assertThat(filter.test("java.util.ArrayList")).isTrue();
    assertThat(filter.test("java.util.HashMap")).isTrue();
    assertThat(filter.test("java.util.UUID")).isTrue();
    assertThat(filter.test("java.util.Base64")).isTrue();
    assertThat(filter.test("java.util.stream.Collectors")).isTrue();
    assertThat(filter.test("java.util.function.Function")).isTrue();
    assertThat(filter.test("java.util.regex.Pattern")).isTrue();
    assertThat(filter.test("java.time.LocalDate")).isTrue();
    assertThat(filter.test("java.time.format.DateTimeFormatter")).isTrue();
    assertThat(filter.test("java.math.BigDecimal")).isTrue();

    assertThat(filter.test("java.util.zip.ZipFile")).isFalse();
    assertThat(filter.test("java.util.jar.JarFile")).isFalse();
    assertThat(filter.test("java.util.logging.FileHandler")).isFalse();
    assertThat(filter.test("java.util.prefs.Preferences")).isFalse();
    assertThat(filter.test("java.util.concurrent.ThreadPoolExecutor")).isFalse();
    assertThat(filter.test("java.util.spi.ToolProvider")).isFalse();
    assertThat(filter.test("java.util.Formatter")).isFalse();
    assertThat(filter.test("java.util.Timer")).isFalse();
    assertThat(filter.test("java.util.ServiceLoader")).isFalse();
    assertThat(filter.test("java.lang.Runtime")).isFalse();
    assertThat(filter.test("java.io.File")).isFalse();
  }

  /**
   * GHSA-j57p-qmrh-v7xv: {@code java.util.ResourceBundle} is a bare (non-wildcard) entry in {@link HostClassLookupFilter#DENIED},
   * so name-equality matching alone does not cover its two public JDK subclasses, both of which live directly in the
   * allow-listed {@code java.util} package and inherit the denied static {@code getBundle(String)} factory.
   */
  @Test
  void subclassesOfADeniedTypeAreRejectedEvenWhenAdmittedByName() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(ScriptTriggerExecutor.ALLOWED_PACKAGES, null);

    assertThat(filter.test("java.util.ResourceBundle")).isFalse();
    assertThat(filter.test("java.util.PropertyResourceBundle")).isFalse();
    assertThat(filter.test("java.util.ListResourceBundle")).isFalse();
  }

  @Test
  void hierarchyCheckDoesNotFalsePositiveOnCommonMarkerInterfaces() {
    // Serializable/Cloneable/Comparable live in denied-by-wildcard packages (java.io.**) or are otherwise ubiquitous;
    // an over-broad hierarchy walk must not reject every collection/value class that happens to implement them.
    final HostClassLookupFilter filter = new HostClassLookupFilter(ScriptTriggerExecutor.ALLOWED_PACKAGES, null);

    assertThat(filter.test("java.util.ArrayList")).isTrue();
    assertThat(filter.test("java.util.HashMap")).isTrue();
    assertThat(filter.test("java.util.UUID")).isTrue();
    assertThat(filter.test("java.math.BigDecimal")).isTrue();
    assertThat(filter.test("java.time.LocalDate")).isTrue();
  }

  /** Marker type used only by {@link #hierarchyCheckDoesNotFalsePositiveOnCloseable()}. */
  private static final class ResourceHandle implements java.io.Closeable {
    @Override
    public void close() {
      // Nothing to release - this type exists only to be resolved through Class.forName() by the test below.
    }
  }

  /**
   * {@code java.io.Closeable} lives inside the now-fully-checked {@code java.io.**} wildcard family (issue #6045
   * code review) and extends {@code AutoCloseable}, so it needs the same explicit safe-marker treatment: its one
   * method just signals "release a resource", it does not itself grant one. Without that entry, any allow-listed
   * class implementing it for ordinary resource-management reasons would be newly, incorrectly rejected.
   */
  @Test
  void hierarchyCheckDoesNotFalsePositiveOnCloseable() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(List.of("com.arcadedb.query.polyglot.**"), null);

    assertThat(filter.test(ResourceHandle.class.getName())).isTrue();
  }

  /**
   * Issue #6045 code review: {@link HostClassLookupFilter#SAFE_MARKER_ANCESTORS} must only ever defeat a
   * <i>wildcard</i> {@code DENIED} match - it exists to stop a package wildcard from catching a marker interface
   * that merely happens to live in that package, not to make the interface immune to deny-listing altogether. A
   * caller who deliberately, precisely denies {@code java.io.Serializable} itself (e.g. via
   * {@code extraDeniedPatterns}) must still have every {@code Serializable}-implementing class rejected through
   * the hierarchy walk, exactly like any other precisely-denied ancestor.
   */
  @Test
  void safeMarkerExceptionDoesNotOverrideAPreciseDenyEntryNamingIt() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(ScriptTriggerExecutor.ALLOWED_PACKAGES, List.of("java.io.Serializable"));

    assertThat(filter.test("java.util.UUID")).isFalse();
    // A class NOT implementing Serializable must be unaffected by the caller's precise deny entry.
    assertThat(filter.test("java.util.function.Function")).isTrue();
  }

  /**
   * Issue #6045: a first version of the hierarchy walk excluded every package-wildcard {@code DENIED} entry (e.g.
   * {@code java.security.**}), on the reasoning that it already matches every class in that namespace by name, so
   * checking it during the walk would only add false positives from marker interfaces. That also let a
   * capability-bearing wildcard-only-denied ancestor slip through undetected if reached via an allow-listed
   * subclass. {@code java.util.PropertyPermission} - admitted by name under {@code java.util.*} - extends
   * {@code java.security.BasicPermission} extends {@code java.security.Permission}, both wildcard-denied by
   * {@code java.security.**} and not pinned by any precise entry, so it was the concrete instance of the audited
   * gap in the JDK classpath reachable from {@link ScriptTriggerExecutor#ALLOWED_PACKAGES}.
   */
  @Test
  void hierarchyCheckAlsoCatchesAWildcardOnlyDeniedAncestor() {
    final HostClassLookupFilter filter = new HostClassLookupFilter(ScriptTriggerExecutor.ALLOWED_PACKAGES, null);

    assertThat(filter.test("java.util.PropertyPermission")).isFalse();
  }

  @Test
  void callerSuppliedRestrictionAlsoRejectsItsSubclasses() {
    // A subclass of a caller-supplied (extra) denied type must be rejected too, not just the JDK built-in deny-list.
    // java.util.Hashtable extends java.util.Dictionary; java.util.HashMap does not and must stay admitted.
    final HostClassLookupFilter filter = new HostClassLookupFilter(List.of("java.util.*"), List.of("java.util.Dictionary"));

    assertThat(filter.test("java.util.Dictionary")).isFalse();
    assertThat(filter.test("java.util.Hashtable")).isFalse();
    assertThat(filter.test("java.util.HashMap")).isTrue();
  }
}
