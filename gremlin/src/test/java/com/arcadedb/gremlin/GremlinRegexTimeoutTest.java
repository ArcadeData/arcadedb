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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.TimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The regular expressions of a Gremlin traversal ({@code TextP.regex()} and {@code TextP.notRegex()}, wherever a predicate stands)
 * are evaluated within {@code arcadedb.command.regexTimeout}, as the ones of the other query languages are: the time a statement
 * spends inside them is one budget for the statement, and an expression that runs past it fails with a {@link TimeoutException}.
 * <p>
 * {@link #SLOW} against {@link #INPUT} is the shape the engine tests of the bound use ({@code TimeBoundRegexTest}): at 26
 * characters one search of it runs for seconds, far past the limit the tests set.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinRegexTimeoutTest {
  private static final String PATH       = "target/databases/GremlinRegexTimeoutTest";
  private static final String SLOW       = "(.*a){20}$";
  private static final String INPUT      = "a".repeat(26) + "!";
  private static final String SETTING    = "arcadedb.command.regexTimeout";
  private static final long   TIMEOUT_MS = 100L;

  private DatabaseFactory factory;
  private Database        database;
  private ArcadeGraph     graph;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      database.newVertex("Person").set("name", INPUT).set("age", 41).save();
      database.newVertex("Person").set("name", "alice").set("age", 30).save();
      database.newVertex("Person").set("name", "bob").set("age", 25).save();
    });
    graph = ArcadeGraph.openShared(database);
    regexTimeout(TIMEOUT_MS);
  }

  @AfterEach
  void tearDown() {
    graph.close();
    database.drop();
    factory.close();
  }

  @Test
  void aSlowPatternInHasStopsAtTheTimeout() {
    assertTimesOut(() -> g().V().has("name", TextP.regex(SLOW)).toList());
    assertTimesOut(() -> g().V().hasLabel("Person").has("name", TextP.regex(SLOW)).toList());
    assertTimesOut(() -> g().V().has("name", TextP.notRegex(SLOW)).toList());
  }

  @Test
  void aSlowPatternInIsStopsAtTheTimeout() {
    assertTimesOut(() -> g().inject(INPUT).is(TextP.regex(SLOW)).toList());
    assertTimesOut(() -> g().inject(INPUT).is(TextP.notRegex(SLOW)).toList());
    assertTimesOut(() -> g().V().values("name").is(TextP.regex(SLOW)).toList());
  }

  @Test
  void aSlowPatternInsideAPredicateCombinationStopsAtTheTimeout() {
    assertTimesOut(() -> g().V().has("name", TextP.regex(SLOW).and(TextP.containing("a"))).toList());
    assertTimesOut(() -> g().V().has("name", TextP.containing("zzz").or(TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().has("name", P.not(TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().has("name", TextP.regex(SLOW).negate()).toList());
    assertTimesOut(() -> g().V().has("name", TextP.containing("a").and(TextP.containing("z").or(TextP.regex(SLOW)))).toList());
    assertTimesOut(() -> g().inject(INPUT).is(TextP.containing("zzz").or(TextP.notRegex(SLOW))).toList());
  }

  @Test
  void aSlowPatternInANestedTraversalStopsAtTheTimeout() {
    assertTimesOut(() -> g().V().where(__.has("name", TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().filter(__.has("name", TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().not(__.has("name", TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().local(__.has("name", TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().union(__.has("name", TextP.regex(SLOW)), __.has("name", "bob")).toList());
    assertTimesOut(() -> g().V().choose(__.has("name", TextP.regex(SLOW)), __.constant(1), __.constant(2)).toList());
    assertTimesOut(() -> g().V().repeat(__.identity()).until(__.has("name", TextP.regex(SLOW))).toList());
    assertTimesOut(() -> g().V().and(__.has("name", TextP.regex(SLOW)), __.has("age")).toList());
  }

  @Test
  void aSlowPatternInAChoiceStopsAtTheTimeout() {
    // THE PICK OF AN option() IS KEPT BY A TRAVERSAL OF ITS OWN, WITH NO STEP
    assertTimesOut(() -> g().inject(INPUT).choose(__.identity()).option(TextP.regex(SLOW), __.constant(1)).toList());
    assertTimesOut(() -> g().inject(INPUT).choose(__.identity()).option(TextP.notRegex(SLOW), __.constant(1)).toList());
    assertTimesOut(
        () -> g().inject(INPUT).choose(__.identity()).option(TextP.regex(SLOW).and(TextP.containing("a")), __.constant(1)).toList());
    assertTimesOut(() -> g().inject(INPUT).choose(__.identity()).option(P.not(TextP.regex(SLOW)), __.constant(1)).toList());
    assertTimesOut(() -> g().inject(INPUT).choose(TextP.regex(SLOW), __.constant(1), __.constant(2)).toList());
    assertTimesOut(() -> g().inject(INPUT).choose(TextP.regex(SLOW), __.constant(1)).toList());

    final String statement = "g.inject('" + INPUT + "').choose(__.identity()).option(TextP.regex('" + SLOW + "'), __.constant(1))";
    assertThatThrownBy(() -> graph.gremlin(statement).execute().stream().count()).hasStackTraceContaining(SETTING);
  }

  @Test
  void aSlowPatternInAWherePredicateStopsAtTheTimeout() {
    // where(P) names the label of the value it compares: here the one the traverser itself carries
    assertTimesOut(() -> g().inject(INPUT).as(SLOW).where(TextP.regex(SLOW)).toList());
    assertTimesOut(() -> g().inject(INPUT).as("a", SLOW).where("a", TextP.regex(SLOW)).toList());
    assertTimesOut(() -> g().inject(INPUT).as(SLOW, "a").where(TextP.regex(SLOW).and(TextP.containing("a"))).toList());
  }

  @Test
  void aSlowPatternOverAnyAllAndNoneStopsAtTheTimeout() {
    final List<String> values = List.of("alice", INPUT);
    assertTimesOut(() -> g().inject(values).any(TextP.regex(SLOW)).toList());
    assertTimesOut(() -> g().inject(values).all(TextP.notRegex(SLOW)).toList());
    assertTimesOut(() -> g().inject(values).none(TextP.regex(SLOW)).toList());
  }

  @Test
  void aSlowPatternInAStatementStopsAtTheTimeout() {
    final String statement = "g.V().has('name', TextP.regex('" + SLOW + "'))";
    assertThatThrownBy(() -> graph.gremlin(statement).execute().stream().count()).hasStackTraceContaining(SETTING);
    assertThatThrownBy(() -> database.query("gremlin", statement).stream().count()).hasStackTraceContaining(SETTING);

    final String filter = "g.inject('" + INPUT + "').is(TextP.regex('" + SLOW + "'))";
    assertThatThrownBy(() -> graph.gremlin(filter).execute().stream().count()).hasStackTraceContaining(SETTING);
    assertThatThrownBy(() -> database.query("gremlin", filter).stream().count()).hasStackTraceContaining(SETTING);
  }

  @Test
  void theBoundCannotBeRemovedFromATraversalSource() {
    final GraphTraversalSource source = graph.traversal().withoutStrategies(ArcadeRegexStrategy.class);
    assertThat(source.getStrategies().getStrategy(ArcadeRegexStrategy.class)).isPresent();
    assertTimesOut(() -> source.V().has("name", TextP.regex(SLOW)).toList());

    final String statement = "g.withoutStrategies(ArcadeRegexStrategy).V().has('name', TextP.regex('" + SLOW + "'))";
    assertThatThrownBy(() -> graph.gremlin(statement).execute().stream().count()).hasStackTraceContaining(SETTING);
  }

  @Test
  void theTimeBelongsToTheStatementNotToEachSearch() {
    // EACH SEARCH OF THESE TAKES A FRACTION OF THE LIMIT: IT IS THEIR SUM THAT PASSES IT. THE VALUES DIFFER, OR TINKERPOP MERGES THEM
    // INTO ONE TRAVERSER AND SEARCHES ONCE
    regexTimeout(400L);
    final Object[] inputs = IntStream.range(0, 100).mapToObj(i -> "a".repeat(22) + "!" + i).toArray();
    assertTimesOut(() -> g().inject(inputs).is(TextP.regex(SLOW)).toList());
  }

  @Test
  void aTimeoutOfZeroLeavesTheSearchUnbounded() {
    regexTimeout(0L);
    assertThat(g().inject("a".repeat(22) + "!").is(TextP.regex(SLOW)).toList()).isEmpty();
  }

  @Test
  void ordinaryPatternsStillAnswer() {
    regexTimeout(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong());

    assertThat(g().V().has("name", TextP.regex("^al")).values("name").toList()).containsExactly("alice");
    assertThat(g().V().has("name", TextP.regex("^(alice|bob)$")).values("name").toList()).containsExactlyInAnyOrder("alice", "bob");
    assertThat(g().V().has("name", TextP.notRegex("^a")).values("name").toList()).containsExactly("bob");
    assertThat(g().V().has("name", TextP.regex("^a").negate()).values("name").toList()).containsExactly("bob");
    assertThat(g().V().has("name", P.not(TextP.regex("^a"))).values("name").toList()).containsExactly("bob");
    assertThat(g().V().has("name", TextP.regex("^a").and(TextP.regex("e$"))).values("name").toList()).containsExactly("alice");
    assertThat(g().V().has("name", TextP.regex("^b").or(TextP.regex("e$"))).values("name").toList()).containsExactlyInAnyOrder("alice", "bob");
    assertThat(g().V().has("name", TextP.regex("^b")).has("age", P.lt(30)).values("name").toList()).containsExactly("bob");
    assertThat(g().V().values("name").is(TextP.regex("^b")).toList()).containsExactly("bob");
    assertThat(g().V().values("name").is(TextP.notRegex("^(a|b)")).toList()).hasSize(0);
    assertThat(g().V().where(__.has("name", TextP.regex("^b"))).values("age").toList()).containsExactly(25);
    assertThat(g().V().local(__.has("name", TextP.regex("^al"))).values("age").toList()).containsExactly(30);
    assertThat(g().V().filter(__.values("name").is(TextP.regex("ob$"))).values("age").toList()).containsExactly(25);
    assertThat(g().V().has("age", 25).values("name").is(TextP.regex("^b")).toList()).containsExactly("bob");
    assertThat(g().inject("abc").as("b").where(TextP.regex("b")).toList()).containsExactly("abc");
    assertThat(g().inject("abc").as("a", "c").where("a", TextP.notRegex("c")).toList()).isEmpty();
    assertThat(g().inject("abc").as("a", "c").where("a", TextP.regex("c")).toList()).containsExactly("abc");
    assertThat(g().V().values("name").choose(__.identity()).option(TextP.regex("^al"), __.constant("A"))
        .option(TextP.regex("^b"), __.constant("B")).toList()).containsExactlyInAnyOrder(INPUT, "A", "B");
    assertThat(g().inject("alice", "bob", "carol").choose(TextP.regex("^al"), __.constant("yes"), __.constant("no")).toList())
        .containsExactlyInAnyOrder("yes", "no", "no");
    assertThat(g().inject("alice", "bob").choose(TextP.regex("^al"), __.constant("yes")).toList()).containsExactlyInAnyOrder("yes", "bob");
    assertThat(g().inject(List.of("alice", "bob")).any(TextP.regex("^b")).toList()).hasSize(1);
    assertThat(g().inject(List.of("alice", "bob")).all(TextP.regex("^b")).toList()).isEmpty();
    assertThat(g().inject(List.of("alice", "bob")).none(TextP.regex("^c")).toList()).hasSize(1);
  }

  @Test
  void theSamePredicateObjectServesSeveralTraversals() {
    // THE BUDGET IS THE STATEMENT'S: A PREDICATE THE CALLER KEEPS AND USES AGAIN STARTS EVERY STATEMENT WITH A FULL ONE
    regexTimeout(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong());
    final TextP regex = TextP.regex("^al");
    for (int i = 0; i < 3; i++)
      assertThat(g().V().has("name", regex).values("name").toList()).containsExactly("alice");
    assertThat(regex.getBiPredicate().getClass().getName()).startsWith("org.apache.tinkerpop");
  }

  @Test
  void aRegexOverAValueThatIsNoStringKeepsItsError() {
    regexTimeout(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong());
    assertThatThrownBy(() -> g().V().has("age", TextP.regex("^4")).toList()).isInstanceOf(ClassCastException.class);
  }

  @Test
  void aRegexOverANullKeepsItsError() {
    regexTimeout(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong());
    // THE ERROR OF A TRAVERSAL THAT MEETS A VALUE IT IS NOT DEFINED FOR IS A CLIENT ERROR, WHOEVER BUILT THE PREDICATE
    assertThatThrownBy(() -> g().inject((Object) null).is(TextP.regex("a")).toList()).isInstanceOf(NullPointerException.class)
        .satisfies(error -> assertThat(GremlinTraversalErrors.asClientError(error)).isNotNull());
  }

  @Test
  void manyRowsAreAnsweredWithinTheDefaultLimit() {
    regexTimeout(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong());
    database.transaction(() -> {
      for (int i = 0; i < 5_000; i++)
        database.newVertex("Person").set("name", "user" + i).set("age", i).save();
    });
    assertThat(g().V().has("name", TextP.regex("^user\\d*7$")).count().next()).isEqualTo(500L);
  }

  private GraphTraversalSource g() {
    return graph.traversal();
  }

  private void regexTimeout(final long millis) {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, millis);
  }

  private static void assertTimesOut(final Runnable traversal) {
    assertThatThrownBy(traversal::run).isInstanceOf(TimeoutException.class).hasMessageContaining(SETTING);
  }
}
