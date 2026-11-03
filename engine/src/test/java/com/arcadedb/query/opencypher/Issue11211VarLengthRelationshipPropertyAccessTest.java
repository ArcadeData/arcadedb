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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11211: {@code r.weight} on the variable of a variable-length relationship - a list of relationships - failed
 * while the rows were serialized, as a plain {@code CommandExecutionException} the server answered with 500. Neo4j refuses
 * the query before it runs ({@code Type mismatch: expected Map, Node, Relationship, ... but was List<Relationship>}), and a
 * type error found only once a query runs is still the caller's ({@code Neo.ClientError.Statement.TypeError}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11211VarLengthRelationshipPropertyAccessTest extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})-[:R {weight: 7}]->(:N {id: 2})"));
  }

  @Test
  void propertyAccessOnAVariableLengthRelationshipIsRefusedBeforeTheQueryRuns() {
    final Throwable thrown = refusal("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN r.weight AS w");

    assertThat(thrown).isInstanceOf(CommandParsingException.class);
    assertThat(thrown.getMessage()).contains("List<Relationship>").contains("'r.weight'");
    assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.PARSING);
  }

  @Test
  void theRefusalDoesNotDependOnWhatTheQueryMatches() {
    // Neo4j refuses it at compile time: a query that matches nothing is refused exactly the same way
    assertThat(refusal("MATCH (n:Nothing)<-[r:R*1..3]-(m:Nothing) RETURN r.weight AS w"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void everyClauseThatReadsThePropertyIsRefused() {
    assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) WHERE r.weight > 1 RETURN n")).isInstanceOf(CommandParsingException.class);
    assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) RETURN n ORDER BY r.weight")).isInstanceOf(CommandParsingException.class);
    // the kind survives a projection that carries the variable, renamed or not
    assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) WITH r RETURN r.weight AS w")).isInstanceOf(CommandParsingException.class);
    assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) WITH r AS rels RETURN rels.weight AS w"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void theListOfRelationshipsIsStillReadable() {
    assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN size(r) AS v")).containsExactly(1L);
    assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN [rel IN r | rel.weight] AS v")).containsExactly(List.of(7));
    assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN r[0].weight AS v")).containsExactly(7);
    assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) UNWIND r AS rel RETURN rel.weight AS v")).containsExactly(7);
    // a fixed-length relationship is one relationship, and its property is read as before
    assertThat(values("MATCH (n:N)<-[r:R]-(m:N) RETURN r.weight AS v")).containsExactly(7);
    // a variable that names something else after a projection is not a list of relationships any more
    assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) WITH {weight: 3} AS r RETURN r.weight AS v")).containsExactly(3L);
  }

  @Test
  void aTypeErrorKnownOnlyAtRuntimeIsTheCallersNotTheServers() {
    // UNWIND says nothing about the kind of its variable, so the list is met only once the query runs
    final Throwable thrown = refusal("UNWIND [[1, 2]] AS l RETURN l.x AS v");

    assertThat(thrown).isInstanceOf(TypeMismatchException.class);
    // named with the language's type names, not with a Java class
    assertThat(thrown.getMessage()).contains("TypeError: Cannot access property 'x' on a LIST<ANY> value")
        .doesNotContain("ArrayList");
    assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);
  }

  @Test
  void aChainedPropertyAccessOnAListIsTheSameTypeError() {
    final Throwable thrown = refusal("WITH {inner: [1, 2]} AS m RETURN m.inner.x AS v");
    assertThat(thrown).isInstanceOf(TypeMismatchException.class);
    assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);
  }

  @Test
  void anIndexIntoAValueThatIsNoListIsTheSameTypeError() {
    final Throwable thrown = refusal("UNWIND [42] AS v RETURN v[0] AS x");
    assertThat(thrown).isInstanceOf(TypeMismatchException.class);
    assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);

    assertThat(refusal("UNWIND [1.5] AS i RETURN [1, 2][i] AS x")).isInstanceOf(TypeMismatchException.class);
  }

  @Test
  void anUnstorableValueIsStillATypeErrorOfItsOwnKind() {
    final Throwable thrown = catchThrowable(
        () -> database.transaction(() -> database.command("opencypher", "CREATE (:N {m: {a: 1}})")));
    assertThat(thrown).isInstanceOf(InvalidPropertyTypeException.class).isInstanceOf(TypeMismatchException.class);
    assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);
  }

  private Throwable refusal(final String cypher) {
    final Throwable thrown = catchThrowable(() -> values(cypher));
    assertThat(thrown).as("must be refused: %s", cypher).isNotNull();
    return thrown;
  }

  private List<Object> values(final String cypher) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty(row.getPropertyNames().iterator().next()));
      }
    }
    return values;
  }
}
