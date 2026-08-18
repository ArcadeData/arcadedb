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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.ToIntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6363: {@code labels()} decided a vertex's labels from its supertypes, a rule written for
 * the synthetic {@code A~B} composite and wrong for every other shape, so a vertex of a type declared with
 * {@code EXTENDS} reported a label list that did not contain its own type - contradicting the very predicate that
 * matched it - and a {@code SET n:Extra} rebuilt the composite from that wrong list, relocating the vertex out of its
 * own subtype.
 * <p>
 * The invariant under test is Neo4j's: {@code L IN labels(n)} and {@code n:L} answer the same question, before and
 * after a label write. Type inheritance is an ArcadeDB extension - Neo4j has no subtypes - so the reference behaviour
 * to match is that a label a node answers to is a label it reports.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelsInheritanceIssue6363Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-labels-inheritance-6363");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Employee");
      database.command("sql", "CREATE VERTEX TYPE Manager EXTENDS Employee");
      database.command("sql", "INSERT INTO Manager SET k = 'm1'");
      database.command("sql", "INSERT INTO Employee SET k = 'e1'");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void labelsReportsTheVertexOwnTypeAlongsideItsAncestors() {
    // Used to return ["Employee"]: the subtype's own name was dropped the moment the type had a supertype.
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Manager");
    assertThat(labels("MATCH (n {k:'e1'}) RETURN labels(n) AS l")).containsExactly("Employee");
  }

  @Test
  void everyReportedLabelIsAlsoMatchedAndEveryMatchedLabelIsReported() {
    // The contradiction the issue is about: the engine matched :Manager twice over and then reported a label
    // list without it.
    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n) WHERE n:Manager RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:Employee) RETURN count(n) AS c")).isEqualTo(2);
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).contains("Manager");
  }

  @Test
  void aTypeExtendingACompositeReportsTheLabelsItEncodesAndNotTheSyntheticName() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Author:Topic {k:'b1'})"));
    database.command("sql", "CREATE VERTEX TYPE Special EXTENDS `Author~Topic`");
    database.transaction(() -> database.command("sql", "INSERT INTO Special SET k = 'sp1'"));

    // Used to return ["Author~Topic"], leaking an internal encoding and reporting neither label it encodes.
    assertThat(labels("MATCH (n {k:'sp1'}) RETURN labels(n) AS l")).containsExactly("Author", "Special", "Topic");
    // The plain composite is unchanged: its own name is the implementation detail, its supertypes are the labels.
    assertThat(labels("MATCH (n {k:'b1'}) RETURN labels(n) AS l")).containsExactly("Author", "Topic");
  }

  @Test
  void addingALabelToASubtypeVertexDoesNotCostItItsOwnType() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:Manager) SET n:Extra"));

    // Was 0: the vertex had been moved to a freshly invented Employee~Extra and every :Manager query lost it.
    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:Employee) RETURN count(n) AS c")).isEqualTo(2);
    assertThat(count("MATCH (n:Extra) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Extra", "Manager");
  }

  @Test
  void addingALabelTheVertexAlreadyAnswersToChangesNothing() {
    final String typeBefore = typeOf("m1");
    database.transaction(() -> database.command("opencypher", "MATCH (n:Manager) SET n:Employee"));

    assertThat(typeOf("m1")).isEqualTo(typeBefore);
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Manager");
    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void removingAnOwnLabelFromASubtypeVertexKeepsTheInheritedOnes() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:Manager) SET n:Extra"));
    database.transaction(() -> database.command("opencypher", "MATCH (n:Extra) REMOVE n:Extra"));

    assertThat(count("MATCH (n:Extra) RETURN count(n) AS c")).isEqualTo(0);
    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Manager");
  }

  @Test
  void removingALabelTheVertexDoesNotHaveIsANoOp() {
    // The counterpart of the refusal below, and the line between them: a label the vertex does not answer to at all
    // is simply absent, so removing it does nothing and reports nothing - as in Neo4j. Only a label it DOES answer
    // to, and would keep answering to, is refused. Covers both an unknown label and one whose type exists but this
    // vertex is not of.
    database.transaction(() -> database.command("opencypher", "CREATE (:Extra {k:'x9'})"));
    final String typeBefore = typeOf("m1");

    assertThat(labelsRemoved("MATCH (n:Manager) REMOVE n:NotPresentAtAll")).isEqualTo(0);
    assertThat(labelsRemoved("MATCH (n:Manager) REMOVE n:Extra")).isEqualTo(0);

    assertThat(typeOf("m1")).isEqualTo(typeBefore);
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Manager");
    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
    // The absent label did not get a type invented for it on the way past.
    assertThat(database.getSchema().getTypeOrNull("NotPresentAtAll")).isNull();
  }

  @Test
  void namingACompositeTypeItselfAsALabelToRemoveChangesNothing() {
    // The count asks instanceOf, which says yes to a vertex's own composite name, while that name is not one of
    // the labels the reduced type is rebuilt from - so it removes nothing. The unchanged-type guard is what keeps
    // that from being reported as a removal, and this pins the pairing so a refactor of either one cannot quietly
    // start counting a no-op as a change.
    database.transaction(() -> database.command("opencypher", "CREATE (:Author:Topic {k:'b1'})"));

    assertThat(labelsRemoved("MATCH (n {k:'b1'}) REMOVE n:`Author~Topic`")).isEqualTo(0);

    assertThat(typeOf("b1")).isEqualTo("Author~Topic");
    assertThat(labels("MATCH (n {k:'b1'}) RETURN labels(n) AS l")).containsExactly("Author", "Topic");
    assertThat(count("MATCH (n:Author) RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void removingAnInheritedLabelIsRefusedRatherThanSilentlyLyingAboutIt() {
    // Manager IS-A Employee in the schema: no type the vertex could be moved to answers 'no' to :Employee while
    // still answering 'yes' to :Manager. Saying so beats a no-op that leaves n:Employee true after REMOVE n:Employee.
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (n:Manager) REMOVE n:Employee")))
        .hasMessageContaining("Employee")
        .hasMessageContaining("Manager");

    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:Employee) RETURN count(n) AS c")).isEqualTo(2);
  }

  @Test
  void aPlainCompositeStillBehavesExactlyAsBefore() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Author:Topic {k:'b1'})"));
    assertThat(labels("MATCH (n {k:'b1'}) RETURN labels(n) AS l")).containsExactly("Author", "Topic");

    database.transaction(() -> database.command("opencypher", "MATCH (n {k:'b1'}) SET n:Reader"));
    assertThat(labels("MATCH (n {k:'b1'}) RETURN labels(n) AS l")).containsExactly("Author", "Reader", "Topic");
    assertThat(typeOf("b1")).isEqualTo("Author~Reader~Topic");

    database.transaction(() -> database.command("opencypher", "MATCH (n {k:'b1'}) REMOVE n:Author"));
    assertThat(labels("MATCH (n {k:'b1'}) RETURN labels(n) AS l")).containsExactly("Reader", "Topic");
    assertThat(typeOf("b1")).isEqualTo("Reader~Topic");
    assertThat(count("MATCH (n:Author) RETURN count(n) AS c")).isEqualTo(0);
  }

  @Test
  void mergeOnCreateSetOfALabelKeepsTheSubtypeToo() {
    database.transaction(() -> database.command("opencypher", "MERGE (n:Manager {k:'m1'}) ON MATCH SET n:Extra"));

    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(labels("MATCH (n {k:'m1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Extra", "Manager");
  }

  @Test
  void aLabelThatHappensToBeSpelledLikeTheBaseVertexTypeIsStillALabel() {
    // 'V' is the type an unlabelled node lands in, and it is also a label a query may write - the openCypher TCK
    // writes it, in (b:U:V:W:X:Y:Z). Only the node's OWN type answers "no labels"; a supertype called V is a label
    // the node was given and answers to.
    database.transaction(() -> database.command("opencypher", "CREATE (:U:V:W {k:'v1'})"));

    assertThat(labels("MATCH (n {k:'v1'}) RETURN labels(n) AS l")).containsExactly("U", "V", "W");
    assertThat(count("MATCH (n:V) RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void aUserTypeWhoseNameMerelyContainsTheSeparatorKeepsIt() {
    // Whether a type is a composite is decided structurally - its name is exactly the sorted, joined names of its
    // own supertypes - and not by looking for a tilde. Under the name heuristic alone, a type somebody created and
    // called 'a~b' would have lost its own name from labels() AND from the set a relabelling rebuilds it out of,
    // which would have moved the vertex out of it on the next SET.
    database.command("sql", "CREATE VERTEX TYPE `a~b` EXTENDS Employee");
    database.transaction(() -> database.command("sql", "INSERT INTO `a~b` SET k = 'x1'"));

    assertThat(labels("MATCH (n {k:'x1'}) RETURN labels(n) AS l")).containsExactly("Employee", "a~b");

    database.transaction(() -> database.command("opencypher", "MATCH (n {k:'x1'}) SET n:Extra"));
    assertThat(count("MATCH (n:`a~b`) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(labels("MATCH (n {k:'x1'}) RETURN labels(n) AS l")).containsExactly("Employee", "Extra", "a~b");
  }

  @Test
  void aLabelNamedTwiceInOneClauseIsCountedOnce() {
    // A label is set membership, not a count: naming it twice adds or removes it once, and the reported
    // labels-added / labels-removed have to say so. ArcadeDB already deduplicates the type the write lands on
    // (Labels.ensureCompositeType), so only the counters could disagree.
    database.transaction(() -> database.command("opencypher", "MATCH (n:Manager) SET n:Extra:Extra"));
    assertThat(labelsAdded("MATCH (n:Manager) SET n:Another:Another")).isEqualTo(1);

    assertThat(labelsRemoved("MATCH (n:Manager) REMOVE n:Extra:Extra")).isEqualTo(1);
    assertThat(count("MATCH (n:Extra) RETURN count(n) AS c")).isEqualTo(0);
    assertThat(count("MATCH (n:Manager) RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void anUnlabelledVertexStillReportsNoLabels() {
    database.transaction(() -> database.command("opencypher", "CREATE ({k:'u1'})"));
    assertThat(labels("MATCH (n {k:'u1'}) RETURN labels(n) AS l")).isEmpty();
  }

  @SuppressWarnings("unchecked")
  private List<String> labels(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return (List<String>) rs.next().getProperty("l");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private int labelsAdded(final String command) {
    return writeStatistic(command, QueryStatistics::getLabelsAdded);
  }

  private int labelsRemoved(final String command) {
    return writeStatistic(command, QueryStatistics::getLabelsRemoved);
  }

  private int writeStatistic(final String command, final ToIntFunction<QueryStatistics> reader) {
    final int[] value = new int[] { -1 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", command)) {
        while (rs.hasNext())
          rs.next();
        value[0] = rs.getStatistics().map(reader::applyAsInt).orElse(-1);
      }
    });
    return value[0];
  }

  private String typeOf(final String key) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n {k:'" + key + "'}) RETURN n AS n")) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().<Document>getProperty("n").getTypeName();
    }
  }
}
