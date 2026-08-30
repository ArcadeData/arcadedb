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

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the property of this build that fails in the worst possible direction: a tag filter that selects the
 * wrong set of tests and reports success while doing it.
 * <p>
 * Surefire's and failsafe's {@code groups}/{@code excludedGroups} parameters each have a same-named {@code -D}
 * user property, and Maven resolves such a parameter from the plugin configuration first, falling back to the
 * user property only where the configuration leaves it unset. Which way round that lands decides whether a
 * command-line tag filter reaches the run, and the build behaves plausibly either way, so both mistakes are
 * invisible:
 * <ul>
 * <li>a <b>literal</b> where a property reference belongs makes {@code -DexcludedGroups=...} a no-op, so a CI
 * lane that narrows or widens the default exclusion silently runs the set the pom named instead. That is issue
 * #5697, and it is why the parent's two defaults are written as {@code ${excludedGroups}} and
 * {@code ${failsafe.excludedGroups}} rather than as the tags they resolve to;</li>
 * <li>an <b>unset</b> parameter where an explicit expression belongs lets a stray {@code -Dgroups=...} narrow an
 * execution that was meant to own every class it can see. Measured on surefire 3.5.6: an execution whose
 * {@code <groups>} is left out runs <b>0 tests and passes</b> under {@code -Dgroups=bogus-tag}. That is step 5
 * of the {@code ha-raft} fork-split checklist, and issue #6794.</li>
 * </ul>
 * Both are checked against the poms as written rather than against {@code help:effective-pom}, which is how
 * issue #6794 originally sketched it. Interpolation is precisely the distinction under test - the effective pom
 * resolves {@code ${excludedGroups}} to {@code benchmark} and so cannot tell a passthrough from the literal that
 * breaks it - and reading the sources costs no Maven invocation, so this runs in the ordinary unit lane and on a
 * developer's machine instead of only in a workflow step.
 * <p>
 * <b>Why the check lives in this module.</b> The partition rule is the one issue #6794 asked for, and the only
 * executions in this repository ever meant to be a tag partition are {@code ha-raft}'s: the {@code ha-heavy}
 * split from issue #6343. That split is not on {@code main} today - it was reverted in f4567f6176 because it
 * surfaced issue #6848 - so the partition rule currently finds nothing in the reactor to judge, and is instead
 * proven able to fail against the split's own configuration as a fixture. It arms itself, with no edit here, on
 * the day the split returns.
 */
class PomTagFilterContractTest {

  private static final Set<String> TAG_PLUGINS = Set.of("maven-surefire-plugin", "maven-failsafe-plugin");
  private static final List<String> TAG_PARAMS = List.of("groups", "excludedGroups");

  /** A value that is nothing but one property reference: what a caller's {@code -D} can still override. */
  private static final Pattern PROPERTY_ONLY = Pattern.compile("\\$\\{[^{}$\\s]+}");

  /** Directories holding build output, tool state, or a nested checkout rather than sources of this build. */
  private static final Set<String> PRUNED = Set.of("target", "node_modules", ".git", ".worktrees", ".m2repo", ".idea", "dist", "venv", ".venv");

  /**
   * One {@code <groups>} or {@code <excludedGroups>} as written in a pom.
   *
   * @param executionId the enclosing {@code <execution>}'s id, or {@code null} for the plugin-wide configuration
   *                    that every inherited execution reads
   */
  private record TagParameter(String pom, String plugin, String executionId, String name, String value) {
    ExecutionKey execution() {
      return new ExecutionKey(pom, plugin, executionId);
    }

    String where() {
      return execution() + " -> <" + name + ">";
    }
  }

  private record ExecutionKey(String pom, String plugin, String executionId) {
    @Override
    public String toString() {
      return pom + " -> " + plugin + (executionId == null ? " (plugin configuration)" : " execution '" + executionId + "'");
    }
  }

  // -----------------------------------------------------------------------------------------------------------
  // The live checks, against the reactor's own poms.
  // -----------------------------------------------------------------------------------------------------------

  /**
   * Issue #5697. A plugin-wide {@code <groups>}/{@code <excludedGroups>} is inherited by every module and applies
   * to the executions CI drives, so a literal there outranks the command line and every {@code -DexcludedGroups}
   * in {@code .github/workflows/} quietly stops meaning anything.
   */
  @Test
  void inheritedTagDefaultsStayOverridableFromTheCommandLine() {
    final List<TagParameter> literals = reactorTagParameters().stream()
        .filter(p -> p.executionId() == null)
        .filter(p -> !PROPERTY_ONLY.matcher(p.value().trim()).matches())
        .toList();

    assertThat(literals)
        .as("""
            A plugin-wide tag filter is written as a literal instead of as a property reference.

            Plugin configuration beats the same-named -D user property, so this value wins over every
            -Dgroups/-DexcludedGroups on the command line, and the build says nothing while it does. The lanes in
            .github/workflows/ that pass their own tag filter would silently run the set named here instead
            (issue #5697).

            Write the default as a property reference and put the value in <properties>, so a caller can still
            override it.

            Offenders:
            %s""".formatted(render(literals)))
        .isEmpty();
  }

  /**
   * Issue #5697's second half. Failsafe's {@code excludedGroups} parameter defaults to the <em>same</em>
   * {@code ${excludedGroups}} user property surefire uses, so a build that gives failsafe no parameter of its own
   * silently applies the unit lane's exclusion to integration tests. The parent gives it a namespaced property
   * instead; deleting that override is a one-line regression with no other symptom.
   */
  @Test
  void integrationTestsDoNotInheritTheUnitLaneTagExclusion() {
    // The parent alone, not every module: a module is free to override the default for itself, and the rule
    // above already holds it to being overridable. What must never merge is the two plugins' sources here.
    final List<TagParameter> parent = reactorTagParameters().stream()
        .filter(p -> p.executionId() == null && p.pom().equals("pom.xml"))
        .toList();

    final TagParameter surefire = onlyOne(parent, "maven-surefire-plugin", "excludedGroups");
    final TagParameter failsafe = onlyOne(parent, "maven-failsafe-plugin", "excludedGroups");

    assertThat(failsafe.value().trim())
        .as("""
            maven-failsafe-plugin's <excludedGroups> must not read the same property maven-surefire-plugin does.

            Failsafe's own parameter default IS ${excludedGroups} - the surefire property - so sharing it makes
            the unit lane's tag exclusion apply to integration tests too, dropping e.g. @Tag("benchmark") ITs from
            every -Pintegration run without a word (issue #5697). Give failsafe a namespaced property of its own.

            surefire: %s
            failsafe: %s""".formatted(surefire.value().trim(), failsafe.value().trim()))
        .isNotEqualTo(surefire.value().trim());
  }

  /**
   * Issue #6794, and the exact inverse of the two rules above. An execution that is one half of a tag
   * <em>partition</em> owns a fixed set of classes, so its selection has to be written out rather than left to
   * resolve through the mojo's user property, where a {@code -Dgroups} aimed at some other lane would narrow it.
   * It is the quietest of the three failures: the execution runs zero tests and the build passes, so ~118 ITs
   * leave the lane with nothing red anywhere.
   */
  @Test
  void aTagPartitionedExecutionPinsBothOfItsTagFilters() {
    final List<String> violations = unpinnedPartitionParameters(reactorTagParameters());

    assertThat(violations)
        .as("""
            An execution that filters by tag has left a tag parameter open to the command line.

            Both halves of a tag partition have to name both parameters. An omitted one resolves through
            surefire's own ${groups}/${excludedGroups} user property, and so does a value that is nothing but a
            reference to that same property, so a -Dgroups or -DexcludedGroups meant for another module narrows
            this execution to whatever it names. Measured on 3.5.6: with <groups> omitted, -Dgroups=bogus-tag
            makes the execution run 0 tests and the build succeed (issue #6794).

            "Everything" is spelled any() | none() - any() alone matches only classes carrying at least one tag.
            "Nothing extra" is spelled any() & none(), because an empty value does not compose into an expression.

            Offenders:
            %s""".formatted(String.join("\n", violations)))
        .isEmpty();
  }

  /**
   * The three checks above all assert that a collected set holds no offenders, so an empty collection would
   * satisfy them for the wrong reason. This is what says the scan reached the build at all.
   */
  @Test
  void theScanReachesTheBuild() {
    final Path root = repositoryRoot();
    final List<String> scanned = reactorPoms().stream().map(pom -> root.relativize(pom).toString()).toList();

    // A count alone is a loose canary - a pruning bug that drops several real modules can still clear a floor.
    // Naming the parent and a module makes it precise: the parent is where the two live rules find their
    // subject, and a module pom proves the walk descends rather than stopping at the root.
    assertThat(scanned).as("poms found under %s", root).contains("pom.xml", "ha-raft/pom.xml", "engine/pom.xml").hasSizeGreaterThan(20);
    assertThat(reactorTagParameters())
        .as("plugin-wide tag parameters in the reactor: the parent configures one for surefire and one for failsafe")
        .filteredOn(p -> p.executionId() == null)
        .hasSizeGreaterThanOrEqualTo(2);
  }

  // -----------------------------------------------------------------------------------------------------------
  // Evidence the rules can fail. The reactor holds no tag-partitioned execution today - the ha-heavy split was
  // reverted in f4567f6176 pending issue #6848 - so the partition rule is exercised against that split's own
  // configuration, in the shape it had and in the shapes issue #6794 says would reopen the hole.
  // -----------------------------------------------------------------------------------------------------------

  @Test
  void theSplitAsItWasWrittenSatisfiesThePartitionRule() {
    final List<TagParameter> split = parse(pomWithExecutions("""
        <execution>
          <id>default</id>
          <configuration>
            <groups>any() | none()</groups>
            <excludedGroups>ha-heavy | (${failsafe.excludedGroups})</excludedGroups>
          </configuration>
        </execution>
        <execution>
          <id>heavy-its-in-their-own-fork</id>
          <configuration>
            <groups>ha-heavy</groups>
            <excludedGroups>${failsafe.excludedGroups}</excludedGroups>
          </configuration>
        </execution>
        """));

    assertThat(split).as("both executions of the split, both parameters each").hasSize(4);
    assertThat(unpinnedPartitionParameters(split)).isEmpty();
  }

  @Test
  void anOmittedTagParameterIsReported() {
    final List<String> violations = unpinnedPartitionParameters(parse(pomWithExecutions("""
        <execution>
          <id>default</id>
          <configuration>
            <excludedGroups>ha-heavy</excludedGroups>
          </configuration>
        </execution>
        """)));

    assertThat(violations).hasSize(1);
    assertThat(violations.getFirst()).contains("execution 'default'").contains("<groups>").contains("is not configured");
  }

  /**
   * The regression issue #6794 names by hand: {@code <groups>any() | none()</groups>} "simplified" away, or
   * written as the very user property it has to be immune to. Both leave the parameter resolving through
   * {@code -Dgroups}, which is what makes the execution narrowable.
   */
  @Test
  void aBareUserPropertyIsReportedLikeAnOmission() {
    final List<String> violations = unpinnedPartitionParameters(parse(pomWithExecutions("""
        <execution>
          <id>heavy-its-in-their-own-fork</id>
          <configuration>
            <groups>${groups}</groups>
            <excludedGroups>${excludedGroups}</excludedGroups>
          </configuration>
        </execution>
        """)));

    assertThat(violations).hasSize(2);
    assertThat(String.join("\n", violations))
        .contains("<groups>")
        .contains("<excludedGroups>")
        .contains("the command line can still narrow it");
  }

  @Test
  void aBlankTagParameterIsReported() {
    final List<String> violations = unpinnedPartitionParameters(parse(pomWithExecutions("""
        <execution>
          <id>heavy-its-in-their-own-fork</id>
          <configuration>
            <groups>ha-heavy</groups>
            <excludedGroups></excludedGroups>
          </configuration>
        </execution>
        """)));

    assertThat(violations).hasSize(1);
    assertThat(violations.getFirst()).contains("<excludedGroups>").contains("is blank");
  }

  /**
   * The plugin-wide rule and the partition rule pull in opposite directions, so each has to leave the other's
   * shape alone. A property reference is what a plugin-wide default must be, and it is exactly what a partitioned
   * execution must not be.
   */
  @Test
  void thePluginWideRuleSeesLiteralsAndNotExecutions() {
    assertThat(parse(pomWithPluginBody("<configuration><excludedGroups>benchmark</excludedGroups></configuration>")))
        .singleElement()
        .matches(p -> p.executionId() == null, "plugin-wide")
        .matches(p -> !PROPERTY_ONLY.matcher(p.value().trim()).matches(), "a literal, which the rule rejects");

    assertThat(parse(pomWithPluginBody("<configuration><excludedGroups>${excludedGroups}</excludedGroups></configuration>")))
        .singleElement()
        .matches(p -> PROPERTY_ONLY.matcher(p.value().trim()).matches(), "a passthrough, which the rule allows");

    // An execution-level parameter is not a plugin-wide default and must not be judged by the plugin-wide rule.
    assertThat(parse(pomWithExecutions("<execution><id>x</id><configuration><groups>ha-heavy</groups></configuration></execution>")))
        .singleElement()
        .matches(p -> "x".equals(p.executionId()), "attributed to its execution");

    // An execution with no id of its own carries no partition: the parent declares exactly one such execution.
    assertThat(parse(pomWithExecutions("<execution><configuration><groups>ha-heavy</groups></configuration></execution>"))).isEmpty();
  }

  // -----------------------------------------------------------------------------------------------------------
  // The rules, expressed over collected parameters so the fixtures above run the same code as the reactor scan.
  // -----------------------------------------------------------------------------------------------------------

  /**
   * Treats an execution-level parameter that is absent, blank, or nothing but its own user property as equally
   * open to the command line.
   * <p>
   * "Absent" leans on {@link #inheritedTagDefaultsStayOverridableFromTheCommandLine}, and the two rules are load
   * bearing for each other: Maven merges the plugin-wide {@code <configuration>} into a named execution for
   * every parameter the execution does not override, so an omitted parameter resolves either to the mojo's own
   * {@code ${groups}}/{@code ${excludedGroups}} default or to whatever the plugin-wide block says - and it is
   * the first rule that keeps the latter a property reference rather than a literal. Either way it stays
   * narrowable from the command line, which is what this rule forbids. Relaxing the first rule would quietly
   * remove that half of the premise, so relax neither alone.
   */
  private static List<String> unpinnedPartitionParameters(final List<TagParameter> parameters) {
    final Set<ExecutionKey> partitions = new LinkedHashSet<>();
    for (final TagParameter p : parameters)
      if (p.executionId() != null)
        partitions.add(p.execution());

    final List<String> violations = new ArrayList<>();
    for (final ExecutionKey partition : partitions)
      for (final String name : TAG_PARAMS) {
        final TagParameter declared = parameters.stream()
            .filter(p -> p.execution().equals(partition) && p.name().equals(name))
            .findFirst().orElse(null);

        if (declared == null)
          violations.add("  " + partition + " -> <" + name + "> is not configured, so it resolves through the ${" + name + "} user property");
        else if (declared.value().isBlank())
          violations.add("  " + declared.where() + " is blank, which is not an expression; write any() & none() for \"nothing\"");
        else if (declared.value().trim().equals("${" + name + "}"))
          violations.add("  " + declared.where() + " is only the ${" + name
              + "} user property, so the command line can still narrow it exactly as if it were unset");
      }
    return violations;
  }

  private static TagParameter onlyOne(final List<TagParameter> parameters, final String plugin, final String name) {
    final List<TagParameter> found = parameters.stream().filter(p -> p.plugin().equals(plugin) && p.name().equals(name)).toList();
    assertThat(found).as("exactly one plugin-wide <%s> for %s in the parent pom", name, plugin).hasSize(1);
    return found.getFirst();
  }

  private static String render(final List<TagParameter> parameters) {
    return parameters.isEmpty() ? "  (none)"
        : parameters.stream().map(p -> "  " + p.where() + " = " + p.value().trim()).reduce((a, b) -> a + "\n" + b).orElseThrow();
  }

  // -----------------------------------------------------------------------------------------------------------
  // Reading the poms.
  // -----------------------------------------------------------------------------------------------------------

  private static List<TagParameter> reactorTagParameters() {
    final Path root = repositoryRoot();
    final List<TagParameter> parameters = new ArrayList<>();
    for (final Path pom : reactorPoms())
      parameters.addAll(collect(read(pom), root.relativize(pom).toString()));
    return parameters;
  }

  private static List<Path> reactorPoms() {
    final List<Path> poms = new ArrayList<>();
    try {
      Files.walkFileTree(repositoryRoot(), new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
          return PRUNED.contains(dir.getFileName().toString()) ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
          if ("pom.xml".equals(file.getFileName().toString()))
            poms.add(file);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return poms;
  }

  /**
   * Walks up from the working directory to the one holding both the Maven wrapper and the parent pom.
   * Deliberately not a hardcoded {@code ../}: the same test resolves from a module basedir under surefire, from a
   * checkout root under an IDE, and from a git worktree.
   */
  private static Path repositoryRoot() {
    Path candidate = Path.of("").toAbsolutePath();
    while (candidate != null) {
      if (Files.isRegularFile(candidate.resolve("mvnw")) && Files.isRegularFile(candidate.resolve("pom.xml")))
        return candidate;
      candidate = candidate.getParent();
    }
    throw new IllegalStateException("no directory holding both mvnw and pom.xml above " + Path.of("").toAbsolutePath());
  }

  /** Every {@code <groups>}/{@code <excludedGroups>} configured for surefire or failsafe in one pom. */
  private static List<TagParameter> collect(final Document doc, final String pom) {
    final List<TagParameter> parameters = new ArrayList<>();
    final NodeList plugins = doc.getElementsByTagName("plugin");
    for (int i = 0; i < plugins.getLength(); i++) {
      final Element plugin = (Element) plugins.item(i);
      final String artifactId = childText(plugin, "artifactId");
      if (artifactId == null || !TAG_PLUGINS.contains(artifactId))
        continue;

      for (final Element configuration : children(plugin, "configuration"))
        add(parameters, pom, artifactId, null, configuration);

      for (final Element executions : children(plugin, "executions"))
        for (final Element execution : children(executions, "execution")) {
          // An execution with no id of its own is Maven's unnamed "default"; the parent declares one and it
          // carries no tag filter. Attributing a partition to it would invent one the pom does not describe.
          final String id = childText(execution, "id");
          if (id == null)
            continue;
          for (final Element configuration : children(execution, "configuration"))
            add(parameters, pom, artifactId, id, configuration);
        }
    }
    return parameters;
  }

  private static void add(final List<TagParameter> into, final String pom, final String plugin, final String executionId,
      final Element configuration) {
    for (final String name : TAG_PARAMS)
      for (final Element parameter : children(configuration, name))
        into.add(new TagParameter(pom, plugin, executionId, name, parameter.getTextContent()));
  }

  private static List<Element> children(final Element parent, final String name) {
    final List<Element> found = new ArrayList<>();
    final NodeList nodes = parent.getChildNodes();
    for (int i = 0; i < nodes.getLength(); i++) {
      final Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE && name.equals(node.getNodeName()))
        found.add((Element) node);
    }
    return found;
  }

  private static String childText(final Element parent, final String name) {
    final List<Element> found = children(parent, name);
    return found.isEmpty() ? null : found.getFirst().getTextContent().trim();
  }

  private static String pomWithExecutions(final String executions) {
    return pomWithPluginBody("<executions>" + executions + "</executions>");
  }

  /** A minimal surefire declaration wrapping the fragment, so a fixture reads as the pom snippet it stands for. */
  private static String pomWithPluginBody(final String pluginBody) {
    return "<project><build><plugins><plugin><artifactId>maven-surefire-plugin</artifactId>" + pluginBody + "</plugin></plugins></build></project>";
  }

  private static List<TagParameter> parse(final String xml) {
    try {
      return collect(newDocumentBuilder().parse(new InputSource(new StringReader(xml))), "fixture");
    } catch (final IOException | SAXException e) {
      throw new IllegalStateException("cannot parse the fixture pom", e);
    }
  }

  private static Document read(final Path pom) {
    try {
      return newDocumentBuilder().parse(pom.toFile());
    } catch (final IOException | SAXException e) {
      throw new IllegalStateException("cannot parse " + pom, e);
    }
  }

  private static DocumentBuilder newDocumentBuilder() {
    try {
      final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      factory.setExpandEntityReferences(false);
      return factory.newDocumentBuilder();
    } catch (final ParserConfigurationException e) {
      throw new IllegalStateException("cannot configure an XML parser", e);
    }
  }
}
