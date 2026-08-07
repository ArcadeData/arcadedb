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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.CaseAlternative;
import com.arcadedb.query.opencypher.ast.CaseExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.IsTypedExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.ListSliceExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternComprehensionExpression;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.ShortestPathExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5686: an uncorrelated {@code COUNT { }} / {@code COLLECT { }} body, or a scoped
 * {@code CALL (v) { }} body, written inside a correlated outer query lost the two count push-downs and paid a full
 * count once per outer row.
 * <p>
 * The guard that cost it was introduced by #5674 and is necessary: both push-downs answer from the schema and the CSR
 * arrays and never look at the incoming rows, so a body counting {@code MATCH (n)-[:KNOWS]->(m)} with {@code n}
 * already bound would be answered with the count over every {@code n} in the graph. What was too coarse is the
 * question it asked - whether the seed row <b>carries</b> a variable, rather than whether the body <b>reads</b> one.
 * <p>
 * The cost is measured rather than timed: a push-down answers a count from the cached type counter or the CSR arrays
 * and reads no record at all, so {@code readRecord} in the database statistics separates the two plans exactly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherUncorrelatedSubqueryCountPushDownIssue5686Test extends TestHelper {
  private static final int BIG = 300;

  /** One body per push-down: the O(1) {@code Type.count()} one, and the CSR chain one at one hop and at two. */
  private static final List<String> PUSHED_DOWN_BODIES = List.of(
      "MATCH (m:Big) RETURN count(m)",
      "MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*)",
      "MATCH (a:Q)-[:LINKS]->(b:Q)-[:LINKS]->(c:Q) RETURN count(*)");

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:P {name: 'a'}), (:P {name: 'b'})");
    database.command("opencypher", "UNWIND range(1, " + BIG + ") AS i CREATE (:Big {k: i})");
    // A chain the CSR count push-down can propagate through, and the fixture the correlation guard needs.
    database.command("opencypher", "CREATE (q1:Q {k: 1})-[:LINKS]->(q2:Q {k: 2})-[:LINKS]->(q3:Q {k: 3})");
  }

  // ===================================================================================================
  // 1. the gap this closes: an uncorrelated body keeps the push-down under a correlated outer query
  // ===================================================================================================

  /**
   * The issue's case. The body names only {@code m}; the outer row carries only {@code n}. Answering it from the
   * cached type counter is the same answer, and it is what the identical body gets when written with no outer row.
   */
  @Test
  void anUncorrelatedBodyKeepsTheTypeCountPushDownUnderACorrelatedQuery() {
    final long uncorrelated = recordsReadBy("RETURN COLLECT { MATCH (m:Big) RETURN count(m) } AS c");
    assertThat(uncorrelated).as("the same body with no outer row is answered by the push-down").isZero();

    assertThat(collectOfLongs("MATCH (n:P) RETURN COLLECT { MATCH (m:Big) RETURN count(m) } AS c"))
        .containsExactly((long) BIG, (long) BIG);
    // Two P rows are read by the outer MATCH; not one of the 300 Big vertices is.
    assertThat(recordsReadBy("MATCH (n:P) RETURN COLLECT { MATCH (m:Big) RETURN count(m) } AS c"))
        .isLessThan(BIG);
  }

  /** The same gap through the other door: a {@code CALL (n) { }} imports {@code n} and the body never reads it. */
  @Test
  void aScopedCallBodyThatImportsAVariableItNeverReadsKeepsThePushDown() {
    final String query = "MATCH (n:P) CALL (n) { MATCH (m:Big) RETURN count(m) AS c } RETURN n.name AS name, c";

    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(BIG);
    }
    assertThat(recordsReadBy(query)).isLessThan(BIG);
  }

  /** The case that never lost it - a {@code CALL { }} importing nothing - kept as the control it is. */
  @Test
  void aCallBodyImportingNothingStillKeepsThePushDown() {
    assertThat(recordsReadBy("MATCH (n:P) CALL { MATCH (m:Big) RETURN count(m) AS c } RETURN n.name AS name, c"))
        .isLessThan(BIG);
  }

  // ===================================================================================================
  // 2. the guard that must not regress: a correlated body is never answered by the global count
  // ===================================================================================================

  /**
   * The test #5674 left behind, restated here because this change is the one that could break it: {@code q} is bound
   * to one vertex, so the body's count is that vertex's outgoing edges, not the graph's.
   */
  @Test
  void aCorrelatedBodyIsStillNotAnsweredByTheGlobalCount() {
    assertThat(collectOfLongs("MATCH (q:Q {k: 1}) RETURN COLLECT { MATCH (q)-[:LINKS]->(x:Q) RETURN count(*) } AS c"))
        .containsExactly(1L);
    assertThat(collectOfLongs("RETURN COLLECT { MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) } AS c"))
        .containsExactly(2L);
  }

  /** A body counting its own type, correlated only through a node the outer row already bound. */
  @Test
  void aBodyWhoseCountedVariableIsTheSeededOneIsStillCorrelated() {
    assertThat(collectOfLongs("MATCH (q:Q {k: 1}) RETURN COLLECT { MATCH (q:Q) RETURN count(q) } AS c"))
        .containsExactly(1L);
  }

  /** And one correlated through a scoped CALL rather than through an expression body. */
  @Test
  void aScopedCallBodyThatReadsTheImportedVariableIsStillCorrelated() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Q {k: 1}) CALL (q) { MATCH (q)-[:LINKS]->(x:Q) RETURN count(*) AS c } RETURN c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ===================================================================================================
  // 3. the collector the gate is built on
  // ===================================================================================================

  /** Every variable a pattern binds is a reference: written under an outer name, it <i>is</i> the outer one. */
  @Test
  void everyVariableAPatternBindsIsCollected() {
    assertThat(namesOf("MATCH p = (a:Q)-[r:LINKS]->(b:Q) RETURN count(a)"))
        .containsExactlyInAnyOrder("p", "a", "r", "b");
  }

  /**
   * A name read only through a property, only in a predicate, only in the projection, or only inside a body nested
   * one level further down is still read. Each is written where the engine writes it - as the body of an expression
   * under an outer {@code MATCH (n:P)} - because that is the only position where an outer name is in scope.
   */
  @Test
  void aNameReadOutsideThePatternIsCollected() {
    assertThat(namesOfBody("MATCH (m:Big) WHERE m.k = n.k RETURN count(m)")).contains("n");
    assertThat(namesOfBody("MATCH (m:Big) RETURN n.name")).contains("n");
    assertThat(namesOfBody("MATCH (m:Big) RETURN COUNT { MATCH (x:Big) WHERE x.k = n.k }")).contains("n");
    assertThat(namesOfBody("MATCH (m:Big) WHERE EXISTS { MATCH (n)-[:LINKS]->(:Q) } RETURN count(m)")).contains("n");
  }

  /** A body naming nothing the seed carries is not correlated, whichever way the two sets are disjoint. */
  @Test
  void aDisjointBodyDoesNotReferenceTheSeededNames() {
    final CypherReferencedVariables referenced = referencedBy("MATCH (m:Big) RETURN count(m)");
    assertThat(referenced.isComplete()).isTrue();
    assertThat(referenced.referencesAny(Set.of("n"))).isFalse();
    assertThat(referenced.referencesAny(Set.of("m"))).isTrue();
    assertThat(referenced.referencesAny(Set.of())).isFalse();
  }

  /**
   * Anything outside the modelled shape answers "referenced", so the caller loses the optimization rather than the
   * correctness. Each of these is a clause or a projection the walk either does not reach or cannot read a name out
   * of, and none of them can appear in a statement the push-downs accept.
   */
  @Test
  void anUnmodelledShapeIsIncompleteAndReferencesEverything() {
    for (final String query : List.of(
        "MATCH (m:Big) RETURN *",
        "MATCH (m:Big) WITH * RETURN count(m)",
        "MATCH (m:Big) SET m.k = 1 RETURN count(m)",
        "MATCH (m:Big) DELETE m",
        "MATCH (m:Big) REMOVE m.k RETURN count(m)",
        "CREATE (m:Big) RETURN count(m)",
        "MERGE (m:Big {k: 1}) RETURN count(m)",
        "MATCH (m:Big) FOREACH (i IN [1] | SET m.k = i) RETURN count(m)",
        "MATCH (m:Big) CALL { MATCH (x:Big) RETURN count(x) AS c } RETURN c",
        "MATCH (m:Big) CALL db.ping() YIELD ok RETURN ok")) {
      final CypherReferencedVariables referenced = referencedBy(query);
      assertThat(referenced.isComplete()).as(query).isFalse();
      assertThat(referenced.referencesAny(Set.of("anything"))).as(query).isTrue();
    }
  }

  /**
   * The one omission this design cannot make loud on its own. An expression or predicate type added to the AST and
   * not classified in {@link CypherReferencedVariables} makes every statement containing it incomplete - which loses
   * the push-down silently and everywhere, breaking nothing that would fail. So the package is walked and every
   * concrete type in it is required to be classified.
   */
  @Test
  void everyExpressionTypeInTheAstIsClassifiedByTheCollector() {
    final List<Class<?>> unclassified = new ArrayList<>();
    for (final Class<?> type : astExpressionTypes())
      if (!CypherReferencedVariables.classifies(type))
        unclassified.add(type);

    assertThat(unclassified)
        .as("classify these in CypherReferencedVariables: a name-carrying arm of Collector.visit, or one of the "
            + "NAMELESS sets when the walk already reaches everything inside them")
        .isEmpty();
  }

  /**
   * The second, subtler way to be wrong, and the only one that puts a <b>wrong count</b> back on the table.
   * <p>
   * Classifying a type as nameless says two things at once: it carries no name of its own, <i>and</i> the walk
   * reaches everything inside it. The first is visible at the declaration; the second lives in a different file, in
   * {@code CypherExpressionWalker}'s switch. A composite added to a NAMELESS set with no arm on that switch is read
   * as a leaf: the walk never descends, an outer name inside it is never collected, {@code isComplete()} stays
   * {@code true}, and the push-down is applied to a body that is correlated after all. The classification guard
   * above cannot see it - the type <i>is</i> classified.
   * <p>
   * So each composite is built here around a planted variable and walked. A type in a NAMELESS set with no walker
   * arm fails as loudly as an unclassified one, and a composite added with no fixture fails for want of one.
   */
  @Test
  void everyNamelessCompositeIsOneTheWalkerDescendsInto() {
    final Map<Class<?>, Object> fixtures = plantedFixtures();

    final List<Class<?>> missingFixture = new ArrayList<>();
    final List<Class<?>> notDescendedInto = new ArrayList<>();
    for (final Class<?> type : astExpressionTypes()) {
      if (!CypherReferencedVariables.classifies(type) || !nestsAnExpression(type, new HashSet<>()))
        continue;

      final Object fixture = fixtures.get(type);
      if (fixture == null) {
        missingFixture.add(type);
        continue;
      }
      if (!collectPlanted(fixture))
        notDescendedInto.add(type);
    }

    assertThat(missingFixture)
        .as("these nest an expression and are classified, so build one here around planted() and walk it")
        .isEmpty();
    assertThat(notDescendedInto)
        .as("the walk did not reach inside these: add an arm for each to CypherExpressionWalker, or they will hide "
            + "an outer name and the push-down will answer a correlated body")
        .isEmpty();
  }

  /**
   * The tie the issue asks to be asserted rather than assumed. The collector claims completeness for a shape wider
   * than the one the push-downs accept - {@code isMatchReturnOnlyStatement()} plus a {@code RETURN} of exactly one
   * count item - and nothing in the code says the second is inside the first. Every body the push-downs answer is
   * asserted here to be both: made of nothing but {@code MATCH} and {@code RETURN}, and modelled.
   */
  @Test
  void everyBodyThePushDownsAnswerIsOneTheCollectorModels() {
    for (final String body : PUSHED_DOWN_BODIES) {
      final CypherStatement statement = parse(body);
      for (final ClauseEntry entry : statement.getClausesInOrder())
        assertThat(entry.getType()).as(body)
            .isIn(ClauseEntry.ClauseType.MATCH, ClauseEntry.ClauseType.RETURN);
      assertThat(referencedBy(body).isComplete()).as(body).isTrue();
    }
  }

  /**
   * And what that buys, measured: a body reading nothing the seed carries costs, per outer row, exactly what the
   * identical body costs written with no outer row at all. Before this change the seed row alone took the push-down
   * away, and the body paid a full count once per outer row - 300 records read per row for the first of these.
   */
  @Test
  void anUncorrelatedBodyCostsPerRowWhatItCostsWithNoOuterRow() {
    final long outerAlone = recordsReadBy("MATCH (n:P) RETURN n.name AS c");
    final long outerRows = 2;

    for (final String body : PUSHED_DOWN_BODIES) {
      final long alone = recordsReadBy("RETURN COLLECT { " + body + " } AS c");
      final String correlated = "MATCH (n:P) RETURN COLLECT { " + body + " } AS c";
      assertThat(recordsReadBy(correlated)).as(correlated).isEqualTo(outerAlone + outerRows * alone);
    }
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  private static CypherStatement parse(final String query) {
    return new Cypher25AntlrParser().parse(query);
  }

  private static final String PLANTED = "plantedOuterVariable";

  private static Expression planted() {
    return new VariableExpression(PLANTED);
  }

  private static Expression literal() {
    return new LiteralExpression(1L, "1");
  }

  /** A pattern whose only node binds the planted name, for the composites that hold a pattern rather than a child. */
  private static PathPattern plantedPattern() {
    return new PathPattern(new NodePattern(PLANTED, List.of(), Map.of()));
  }

  /**
   * One instance per composite type, each carrying {@link #planted()} somewhere the walk is supposed to reach.
   * Where the type accepts several children, the planted one is put in the first, so a partial arm that walks only
   * some of them still fails.
   */
  private static Map<Class<?>, Object> plantedFixtures() {
    final Map<Class<?>, Object> fixtures = new HashMap<>();

    fixtures.put(AllReduceExpression.class,
        new AllReduceExpression("acc", planted(), "i", literal(), literal(), literal(), "all"));
    fixtures.put(ArithmeticExpression.class,
        new ArithmeticExpression(planted(), ArithmeticExpression.Operator.ADD, literal()));
    fixtures.put(BooleanWrapperExpression.class, new BooleanWrapperExpression(new BooleanCoercionExpression(planted())));
    fixtures.put(CaseExpression.class,
        new CaseExpression(planted(), List.of(new CaseAlternative(literal(), literal())), literal()));
    fixtures.put(ComparisonExpressionWrapper.class,
        new ComparisonExpressionWrapper(planted(), ComparisonExpression.Operator.EQUALS, literal()));
    fixtures.put(FunctionCallExpression.class, new FunctionCallExpression("abs", List.of(planted()), false));
    fixtures.put(ListComprehensionExpression.class,
        new ListComprehensionExpression("x", planted(), null, null, "[x IN p]"));
    fixtures.put(ListExpression.class, new ListExpression(List.of(planted()), "[p]"));
    fixtures.put(ListIndexExpression.class, new ListIndexExpression(planted(), literal()));
    fixtures.put(ListPredicateExpression.class,
        new ListPredicateExpression(ListPredicateExpression.PredicateType.ALL, "x", planted(), null, "all(x IN p)"));
    fixtures.put(ListSliceExpression.class, new ListSliceExpression(planted(), null, null));
    fixtures.put(MapExpression.class, new MapExpression(Map.of("k", planted()), "{k: p}"));
    // Both name-carrying and composite: the collector reads its base variable, and the walk still has to reach the
    // expressions of its elements. The base is deliberately some other name, so only the descent can find the plant.
    fixtures.put(MapProjectionExpression.class, new MapProjectionExpression("base",
        List.of(new MapProjectionExpression.ProjectionElement("k", planted())), "base{k: p}"));
    fixtures.put(PatternComprehensionExpression.class,
        new PatternComprehensionExpression(null, plantedPattern(), null, literal(), "[(p) | 1]"));
    fixtures.put(ReduceExpression.class,
        new ReduceExpression("acc", planted(), "i", literal(), literal(), "reduce"));
    fixtures.put(ShortestPathExpression.class, new ShortestPathExpression(plantedPattern(), false, "shortestPath"));
    fixtures.put(TernaryLogicalExpression.class,
        new TernaryLogicalExpression(TernaryLogicalExpression.Operator.AND, planted(), literal()));

    fixtures.put(BooleanCoercionExpression.class, new BooleanCoercionExpression(planted()));
    fixtures.put(ComparisonExpression.class,
        new ComparisonExpression(planted(), ComparisonExpression.Operator.EQUALS, literal()));
    fixtures.put(InExpression.class, new InExpression(planted(), List.of(literal()), false));
    fixtures.put(IsNullExpression.class, new IsNullExpression(planted(), false));
    fixtures.put(IsTypedExpression.class, new IsTypedExpression(planted(), "STRING", null, false, false));
    fixtures.put(LabelCheckExpression.class, new LabelCheckExpression(planted(), "L", "p:L"));
    fixtures.put(LogicalExpression.class, new LogicalExpression(LogicalExpression.Operator.AND,
        new BooleanCoercionExpression(planted()), new BooleanCoercionExpression(literal())));
    fixtures.put(PatternPredicateExpression.class, new PatternPredicateExpression(plantedPattern(), false));
    fixtures.put(RegexExpression.class, new RegexExpression(planted(), literal()));
    fixtures.put(StringMatchExpression.class,
        new StringMatchExpression(planted(), literal(), StringMatchExpression.MatchType.CONTAINS));

    return fixtures;
  }

  /** Walks one fixture the way the collector does, and reports whether the planted name was reached. */
  private static boolean collectPlanted(final Object fixture) {
    final Set<String> found = new HashSet<>();
    final CypherExpressionWalker.Visitor visitor = new CypherExpressionWalker.Visitor() {
      @Override
      public void visit(final Expression expression) {
        if (expression instanceof VariableExpression variable)
          found.add(variable.getVariableName());
      }

      @Override
      public void visitPattern(final PathPattern pattern) {
        for (final NodePattern node : pattern.getNodes())
          found.add(node.getVariable());
      }
    };

    if (fixture instanceof Expression expression)
      CypherExpressionWalker.walk(expression, visitor);
    else
      CypherExpressionWalker.walk((BooleanExpression) fixture, visitor);

    return found.contains(PLANTED);
  }

  /**
   * Whether {@code type} holds an expression, a predicate or a pattern anywhere in its fields - directly, inside a
   * collection, or through a helper of the AST such as {@code CaseAlternative}. A type that holds none is a leaf and
   * needs no arm on the walker.
   * <p>
   * It reads declared types, so an expression stashed in a raw {@code Object} field is invisible to it and the type
   * would be taken for a leaf. That is the one blind spot in this guard. The single place the AST does that today -
   * inline pattern properties, held as {@code Map<String, Object>} - the collector refuses to reason about at all
   * ({@code getPropertiesParameterName} and the explicit incompleteness in {@code visitPattern}), so nothing
   * currently relies on the gap being closed.
   */
  private static boolean nestsAnExpression(final Class<?> type, final Set<Class<?>> visited) {
    if (!visited.add(type))
      return false;

    for (final Field field : type.getDeclaredFields())
      if (!Modifier.isStatic(field.getModifiers()) && holdsAnExpression(field.getGenericType(), visited))
        return true;
    return false;
  }

  private static boolean holdsAnExpression(final Type type, final Set<Class<?>> visited) {
    if (type instanceof ParameterizedType parameterized) {
      for (final Type argument : parameterized.getActualTypeArguments())
        if (holdsAnExpression(argument, visited))
          return true;
      return false;
    }

    if (!(type instanceof Class<?> raw))
      return false;
    if (Expression.class.isAssignableFrom(raw) || BooleanExpression.class.isAssignableFrom(raw)
        || PathPattern.class.isAssignableFrom(raw))
      return true;

    // A helper of the AST - CaseAlternative, say - carrying expressions of its own.
    return raw.getPackageName().equals(Expression.class.getPackageName()) && nestsAnExpression(raw, visited);
  }

  /**
   * Every concrete {@link Expression} or {@link BooleanExpression} the AST package declares, read off the compiled
   * classes rather than off a list that would go stale exactly when it matters.
   * <p>
   * Scoped to that one package, which is where all of them live today. A subtype introduced elsewhere would escape
   * both this and the classification guard, so it would have to be added here by hand.
   */
  private static List<Class<?>> astExpressionTypes() {
    final String packageName = Expression.class.getPackageName();
    // Located from the interface's own class file rather than from the package name: a test class sharing the
    // package would otherwise resolve the directory to target/test-classes, which holds none of these.
    final URL anchor = Expression.class.getResource("Expression.class");
    assertThat(anchor).as("the AST package has to be readable to be walked").isNotNull();
    assertThat(anchor.getProtocol()).as("this walk reads compiled classes from a directory").isEqualTo("file");

    final List<Class<?>> types = new ArrayList<>();
    try (final Stream<Path> files = Files.list(Path.of(anchor.toURI()).getParent())) {
      for (final Path file : files.toList()) {
        final String fileName = file.getFileName().toString();
        if (!fileName.endsWith(".class"))
          continue;

        final Class<?> type = Class.forName(packageName + "." + fileName.substring(0, fileName.length() - 6));
        if (type.isInterface() || Modifier.isAbstract(type.getModifiers()))
          continue;
        if (Expression.class.isAssignableFrom(type) || BooleanExpression.class.isAssignableFrom(type))
          types.add(type);
      }
    } catch (final IOException | URISyntaxException | ClassNotFoundException e) {
      // Not a condition to assert on: the package could not be read at all, so no guard ran.
      throw new IllegalStateException("the AST package could not be walked, so nothing was guarded", e);
    }

    assertThat(types).as("the walk found no expression type, so it is not walking what it thinks").isNotEmpty();
    return types;
  }

  private static CypherReferencedVariables referencedBy(final String query) {
    return CypherReferencedVariables.of(parse(query));
  }

  private static Set<String> namesOf(final String query) {
    final CypherReferencedVariables referenced = referencedBy(query);
    assertThat(referenced.isComplete()).as(query).isTrue();
    return referenced.getNames();
  }

  /**
   * The names of {@code body} as the engine sees it: parsed as the body of an expression under an outer
   * {@code MATCH (n:P)}, which is what puts {@code n} in scope, and read back off the parsed subquery.
   */
  private static Set<String> namesOfBody(final String body) {
    final CypherStatement outer = parse("MATCH (n:P) RETURN COLLECT { " + body + " } AS c");
    final Expression projected = outer.getReturnClause().getReturnItems().get(0).getExpression();
    final CypherStatement parsedBody = ((CollectExpression) projected).getParsedSubquery();
    assertThat(parsedBody).as(body).isNotNull();

    final CypherReferencedVariables referenced = CypherReferencedVariables.of(parsedBody);
    assertThat(referenced.isComplete()).as(body).isTrue();
    return referenced.getNames();
  }

  /** The records the query materialized: zero when a count was answered from the counter or the CSR arrays. */
  private long recordsReadBy(final String query) {
    final long before = readRecords();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
    return readRecords() - before;
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  private List<Long> collectOfLongs(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        for (final Object value : (List<Object>) row.getProperty("c"))
          values.add(((Number) value).longValue());
      }
    }
    return values;
  }
}
