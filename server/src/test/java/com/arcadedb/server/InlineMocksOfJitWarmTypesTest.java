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

import com.arcadedb.utility.SubclassMocks;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard for issue #8851: no test mocks one of the engine classes the HA and HTTP layers delegate to with
 * Mockito's default INLINE mock maker.
 * <p>
 * An inline mock retransforms the mocked class in place, and on the Graal JIT code compiled earlier against the
 * unmocked class survives the retransformation: production code a real-server test warmed up in the same fork then
 * reads the mock's fields instead of its stubs, while the test's own calls answer correctly (see {@link SubclassMocks}
 * and issue #8021). It only shows on a GraalVM JDK and only for one class order, so no single run catches it; the rule
 * is held by reading the sources instead. A test mocks these types through {@link SubclassMocks}, either explicitly or
 * by statically importing {@code SubclassMocks.mock} in place of {@code Mockito.mock}.
 * <p>
 * The import is a file-level exemption: once a file imports {@code SubclassMocks.mock}, every bare {@code mock(...)} in
 * it is a subclass mock, whatever type it names, and stays one only while that import does. {@code Mockito.mock(...)}
 * is flagged regardless. The scan is a regular-expression approximation, not a parser: a mock or spy built through a
 * helper that receives the class as a variable, or a {@code spy(instance)} of a variable, is out of its reach.
 */
class InlineMocksOfJitWarmTypesTest {

  /** The reactor root, seen from the {@code server} module that surefire runs this class in. */
  private static final Path REACTOR_ROOT = Path.of("..");

  private static final List<String> SKIPPED_TOP_LEVEL = List.of(".worktrees", "e2e", "load-tests", "node_modules");

  /**
   * Concrete engine classes whose methods {@code RaftReplicatedDatabase}, {@code ServerDatabase} and the HTTP handlers
   * call on every request, so they are compiled - and inlined - by the time any real-server test in the fork is done.
   * Extend it with any further non-final class that production code calls on a hot path and tests hand in as a mock,
   * after converting that type's existing inline mocks (issue #8867 tracks the server/HA types still outside the list).
   * A final class does not belong here: the subclass maker cannot mock it.
   */
  private static final String GUARDED_TYPES = "LocalDatabase|TransactionContext|TransactionManager|LocalSchema|FileManager|ComponentFile";

  /** A guarded type by its simple or fully qualified name ({@code com.arcadedb.database.LocalDatabase}). */
  private static final String GUARDED_TYPE = "(?:[a-z_]\\w*\\.)*(?:" + GUARDED_TYPES + ")";

  /**
   * A mock call whose own settings pick a mock maker ({@code withSettings().mockMaker(SUBCLASS)}) is not flagged. An
   * approximation: it exempts the call when {@code mockMaker} appears anywhere before the statement's next {@code ;},
   * so a second mock call or a lambda body on the same statement could hide an inline mock.
   */
  private static final String NO_EXPLICIT_MAKER = "\\.class\\b(?![^;]*mockMaker)";

  /** {@code Mockito.mock(LocalDatabase.class...)}: the inline maker whatever this file imports. */
  private static final Pattern QUALIFIED_MOCK = Pattern.compile("\\bMockito\\.mock\\(\\s*" + GUARDED_TYPE + NO_EXPLICIT_MAKER);

  /** {@code mock(LocalDatabase.class...)}: the inline maker when {@code mock} is {@code Mockito.mock}. */
  private static final Pattern BARE_MOCK = Pattern.compile("(?<![\\w.])mock\\(\\s*" + GUARDED_TYPE + NO_EXPLICIT_MAKER);

  /**
   * {@code spy(LocalDatabase.class)} or {@code spy(new LocalDatabase(...))}, bare or {@code Mockito.}-qualified: an
   * inline spy retransforms the class exactly like an inline mock. {@code SubclassMocks.spy(instance)} is the way out.
   */
  private static final Pattern INLINE_SPY = Pattern.compile(
      "(?<![\\w.]|SubclassMocks\\.)(?:Mockito\\.)?spy\\(\\s*(?:new\\s+" + GUARDED_TYPE + "\\s*\\(|" + GUARDED_TYPE + "\\.class\\b)");

  /** {@code @Mock LocalDatabase database;}: the extension builds it with the configured (inline) maker. */
  private static final Pattern ANNOTATED_MOCK = Pattern.compile("@(?:Mock|Spy)\\b[^;=]*?\\b(?:" + GUARDED_TYPES + ")\\s+\\w+\\s*[;=]");

  /** The static import that turns every bare {@code mock(...)} of a file into a subclass mock. */
  private static final Pattern SUBCLASS_MOCK_IMPORT = Pattern.compile("import\\s+static\\s+com\\.arcadedb\\.utility\\.SubclassMocks\\.(?:mock|\\*)\\s*;");

  /** Every module's test tree holds far more than this; below it the walk has silently lost its root. */
  private static final int EXPECTED_MINIMUM_SOURCES = 1000;

  @Test
  void noTestHandsAnInlineMockOfAJitWarmEngineTypeToProductionCode() throws IOException {
    assertThat(REACTOR_ROOT.resolve("server").resolve("pom.xml"))
        .as("the scan must start at the reactor root, or it checks nothing").isRegularFile();

    final Map<String, String> sources = readTestSources();
    assertThat(sources).as("the scan must find the test sources of every module").hasSizeGreaterThan(EXPECTED_MINIMUM_SOURCES);

    assertThat(offenders(sources))
        .as("mock LocalDatabase, TransactionContext and the other engine types the HA/HTTP layers delegate to through "
            + "com.arcadedb.utility.SubclassMocks: an inline mock reads stale fields through JIT-warm code on the Graal JIT "
            + "(issues #8021, #8851)")
        .isEmpty();
  }

  @Test
  void theScanCatchesEveryShapeItGuards() {
    final Map<String, String> sources = new TreeMap<>();
    sources.put("a/Bare.java", """
        import static org.mockito.Mockito.mock;
        class Bare { @Test void t() { LocalDatabase db = mock(LocalDatabase.class); } }""");
    sources.put("a/BareWithAnswer.java", """
        import static org.mockito.Mockito.mock;
        class BareWithAnswer { @Test void t() { TransactionContext tx = mock(TransactionContext.class, RETURNS_DEEP_STUBS); } }""");
    sources.put("a/Qualified.java", """
        import static com.arcadedb.utility.SubclassMocks.mock;
        class Qualified { @Test void t() { TransactionManager tm = Mockito.mock(TransactionManager.class); } }""");
    sources.put("a/FullyQualified.java", """
        import static org.mockito.Mockito.mock;
        class FullyQualified { @Test void t() { Object db = mock(com.arcadedb.database.LocalDatabase.class); } }""");
    sources.put("a/SpyOfClass.java", """
        class SpyOfClass { @Test void t() { LocalDatabase db = Mockito.spy(LocalDatabase.class); } }""");
    sources.put("a/SpyOfNew.java", """
        import static org.mockito.Mockito.spy;
        class SpyOfNew { @Test void t() { TransactionManager tm = spy(new TransactionManager(db)); } }""");
    sources.put("a/AnnotatedSpy.java", """
        class AnnotatedSpy { @Spy private FileManager files = new FileManager(); @Test void t() { } }""");
    sources.put("a/Annotated.java", """
        class Annotated { @Mock private LocalSchema schema; @Test void t() { } }""");

    // The shapes that are fine: an explicit subclass mock, a file importing SubclassMocks.mock, an unguarded type, a
    // mock whose own settings choose the maker.
    sources.put("b/Explicit.java", """
        import static org.mockito.Mockito.mock;
        class Explicit { @Test void t() { LocalDatabase db = SubclassMocks.mock(LocalDatabase.class); Schema s = mock(Schema.class); } }""");
    sources.put("b/Imported.java", """
        import static com.arcadedb.utility.SubclassMocks.mock;
        class Imported { @Test void t() { FileManager fm = mock(FileManager.class); ComponentFile f = mock(ComponentFile.class); } }""");
    sources.put("b/OwnSettings.java", """
        class OwnSettings { @Test void t() { LocalDatabase db = Mockito.mock(LocalDatabase.class, withSettings().mockMaker(MockMakers.SUBCLASS)); } }""");
    sources.put("b/SubclassSpy.java", """
        class SubclassSpy { @Test void t() { LocalDatabase db = SubclassMocks.spy(real); } }""");
    sources.put("b/Unguarded.java", """
        import static org.mockito.Mockito.mock;
        class Unguarded { @Test void t() { LocalDatabaseFactory f = mock(LocalDatabaseFactory.class); } }""");

    final List<String> offenders = offenders(sources);
    for (final String name : List.of("Bare", "BareWithAnswer", "Qualified", "FullyQualified", "SpyOfClass", "SpyOfNew", "AnnotatedSpy",
        "Annotated"))
      assertThat(offenders).as("the scan must flag %s", name).anyMatch(o -> o.startsWith("a/" + name + ".java"));
    assertThat(offenders).as("the scan must not flag a shape that is fine").noneMatch(o -> o.startsWith("b/"));
  }

  private static Map<String, String> readTestSources() throws IOException {
    final Map<String, String> sources = new TreeMap<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).toList()) {
        final String moduleName = module.getFileName().toString();
        if (SKIPPED_TOP_LEVEL.stream().anyMatch(moduleName::startsWith))
          continue;
        final Path testSources = module.resolve("src").resolve("test").resolve("java");
        if (!Files.isDirectory(testSources))
          continue;
        try (final Stream<Path> files = Files.walk(testSources)) {
          // Not this class: its own self-test spells out every shape it flags.
          for (final Path file : files.filter(p -> p.toString().endsWith(".java"))
              .filter(p -> !p.getFileName().toString().equals(InlineMocksOfJitWarmTypesTest.class.getSimpleName() + ".java")).toList())
            sources.put(REACTOR_ROOT.relativize(file).toString().replace('\\', '/'), Files.readString(file, StandardCharsets.UTF_8));
        }
      }
    }
    return sources;
  }

  /** One line per violation, {@code <file>:<line>: <what>}. */
  static List<String> offenders(final Map<String, String> sources) {
    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, String> entry : sources.entrySet()) {
      final String name = entry.getKey();
      final String source = entry.getValue();
      report(offenders, name, source, QUALIFIED_MOCK.matcher(source), "Mockito.mock(...) of a JIT-warm engine type");
      if (!SUBCLASS_MOCK_IMPORT.matcher(source).find())
        report(offenders, name, source, BARE_MOCK.matcher(source), "inline mock(...) of a JIT-warm engine type");
      report(offenders, name, source, INLINE_SPY.matcher(source), "inline spy(...) of a JIT-warm engine type");
      report(offenders, name, source, ANNOTATED_MOCK.matcher(source), "@Mock/@Spy of a JIT-warm engine type");
    }
    return offenders;
  }

  private static void report(final List<String> offenders, final String name, final String source, final Matcher matcher,
      final String what) {
    while (matcher.find())
      offenders.add(location(name, source, matcher.start()) + ": " + what);
  }

  private static String location(final String name, final String source, final int offset) {
    int line = 1;
    for (int i = 0; i < offset; i++)
      if (source.charAt(i) == '\n')
        line++;
    return name + ":" + line;
  }
}
