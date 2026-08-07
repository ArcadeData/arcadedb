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
}
