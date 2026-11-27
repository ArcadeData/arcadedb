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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12425: a graph statement refused for a mistake of the caller has to say so with a typed exception the servers
 * answer as a client error (HTTP 400, Postgres 42804, Bolt VALIDATION), not with a plain {@code CommandExecutionException}
 * (HTTP 500) and never with the raw {@link ClassCastException} of a record cast. A refused statement writes nothing.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12425CallerMistakesInGraphStatementsTest extends TestHelper {
  private RID vertexRid;
  private RID otherVertexRid;
  private RID documentRid;
  private RID edgeRid;

  @Test
  void aDocumentRidLiteralAsAnEndpointIsRefusedAndNamed() {
    createFixtures();

    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM " + documentRid + " TO " + vertexRid, "document", documentRid);
    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM " + vertexRid + " TO " + documentRid, "document", documentRid);
    assertThat(linkCount()).isZero();
  }

  @Test
  void aDocumentRidGivenAsAStringIsRefusedAndNamedNotAnsweredAsMissing() {
    createFixtures();

    // the record exists: a "record not found" would be a wrong answer, the endpoint is the wrong kind of record
    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM ? TO ?", "document", documentRid, documentRid.toString(),
        vertexRid.toString());
    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM ? TO ?", "document", documentRid, vertexRid.toString(),
        documentRid.toString());
    assertThat(linkCount()).isZero();
  }

  @Test
  void anEdgeAsAnEndpointIsRefusedAndNamed() {
    createFixtures();

    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM " + edgeRid + " TO " + vertexRid, "edge", edgeRid);
    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM ? TO ?", "edge", edgeRid, vertexRid.toString(), edgeRid.toString());
    assertThat(linkCount()).isZero();
  }

  @Test
  void aQueryThatReturnsADocumentOrAnEdgeAsAnEndpointIsRefusedAndNamed() {
    createFixtures();

    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM (SELECT FROM Doc12425) TO (SELECT FROM V12425)", "document", documentRid);
    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM (SELECT FROM V12425) TO (SELECT FROM Doc12425)", "document", documentRid);
    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM (SELECT FROM Wire12425) TO (SELECT FROM V12425)", "edge", edgeRid);
    assertThat(linkCount()).isZero();
  }

  @Test
  void aProjectedRidOfADocumentIsRefusedAndNamed() {
    createFixtures();

    assertRefusedAsNotAVertex("CREATE EDGE Link12425 FROM (SELECT @rid AS rid FROM Doc12425) TO (SELECT FROM V12425)", "document",
        documentRid);
    assertThat(linkCount()).isZero();
  }

  @Test
  void aDocumentAmongSeveralEndpointsLeavesNoEdgeOfTheOnesBeforeIt() {
    createFixtures();

    // the endpoints are read one at a time: the edge to the first vertex is built before the document is reached, and the
    // refused statement must not leave it behind once its transaction ends
    database.begin();
    try {
      assertThatThrownBy(() -> drainCommand("CREATE EDGE Link12425 FROM " + vertexRid + " TO [" + otherVertexRid + ", " + documentRid + "]"))//
          .isExactlyInstanceOf(TypeMismatchException.class)//
          .hasMessageContaining("the document " + documentRid + " is not a vertex");
    } finally {
      database.rollback();
    }

    assertThat(linkCount()).isZero();
  }

  @Test
  void anEndpointThatIsNotARecordAtAllIsRefused() {
    createFixtures();

    assertThatThrownBy(() -> inTransaction("CREATE EDGE Link12425 FROM ? TO ?", "nothing", vertexRid.toString()))//
        .isExactlyInstanceOf(TypeMismatchException.class)//
        .hasMessageContaining("Invalid vertex for edge creation: nothing");
    assertThat(linkCount()).isZero();
  }

  @Test
  void aRidThatNamesNothingIsStillRecordNotFound() {
    createFixtures();

    // not a mistake of the kind above: the RID is well-formed and the record is simply not there
    assertThatThrownBy(
        () -> inTransaction("CREATE EDGE Link12425 FROM ? TO ?", "#" + documentRid.getBucketId() + ":99999", vertexRid.toString()))//
        .isInstanceOf(RecordNotFoundException.class);
    assertThat(linkCount()).isZero();
  }

  @Test
  void validEndpointsStillMakeTheEdge() {
    createFixtures();

    inTransaction("CREATE EDGE Link12425 FROM " + vertexRid + " TO " + otherVertexRid);
    inTransaction("CREATE EDGE Link12425 FROM ? TO ?", otherVertexRid.toString(), vertexRid.toString());
    inTransaction("CREATE EDGE Link12425 FROM (SELECT FROM V12425 WHERE k = 'a') TO (SELECT @rid FROM V12425 WHERE k = 'b')");

    assertThat(linkCount()).isEqualTo(3L);
  }

  @ParameterizedTest
  @ValueSource(strings = { "garbage", "#1", "#1:2:3", "#a:b", "#1:", "#:1" })
  void aRidThatIsNotARidIsRefusedAsATypeMismatch(final String text) {
    createFixtures();

    assertThatThrownBy(() -> {
      try (final ResultSet resultSet = database.query("sql", "MATCH {rid: '" + text + "', as: p} RETURN p")) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    })//
        .isExactlyInstanceOf(TypeMismatchException.class)//
        .hasMessageContaining("Cannot convert to RID: " + text);
  }

  private void assertRefusedAsNotAVertex(final String sql, final String kind, final RID offender, final Object... parameters) {
    assertThatThrownBy(() -> inTransaction(sql, parameters))//
        .isExactlyInstanceOf(TypeMismatchException.class)//
        .as("the refusal is a typed one, with no Java cast exception under it: %s", sql)//
        .hasNoCause()//
        .hasMessage("Invalid vertex for edge creation: the " + kind + " " + offender + " is not a vertex");
  }

  private void inTransaction(final String sql, final Object... parameters) {
    database.transaction(() -> drainCommand(sql, parameters));
  }

  /** Runs the command to its end: the statements are lazy, and one that is not pulled does nothing. */
  private void drainCommand(final String sql, final Object... parameters) {
    try (final ResultSet resultSet = database.command("sql", sql, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  /** The count of the stored links, read by a scan: the counter {@code count(*)} answers from is not ground truth. */
  private long linkCount() {
    try (final ResultSet resultSet = database.query("sql", "SELECT count(@rid) AS n FROM Link12425")) {
      return resultSet.next().<Number>getProperty("n").longValue();
    }
  }

  private void createFixtures() {
    database.getSchema().createVertexType("V12425");
    database.getSchema().createEdgeType("Link12425");
    database.getSchema().createEdgeType("Wire12425");
    database.getSchema().createDocumentType("Doc12425");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V12425").set("k", "a").save();
      final MutableVertex b = database.newVertex("V12425").set("k", "b").save();
      vertexRid = a.getIdentity();
      otherVertexRid = b.getIdentity();
      documentRid = database.newDocument("Doc12425").set("k", "d").save().getIdentity();
      edgeRid = a.newEdge("Wire12425", b).getIdentity();
    });
  }
}
