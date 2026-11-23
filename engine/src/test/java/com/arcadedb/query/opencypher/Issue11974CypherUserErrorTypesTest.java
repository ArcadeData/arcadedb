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
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.ExternalResourceException;
import com.arcadedb.exception.ObjectInUseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11974: three openCypher user errors were raised as a plain {@code CommandExecutionException}, which no rule classifies,
 * so every protocol answered them as a server fault (Bolt {@code Neo.DatabaseError.General.UnknownError}, HTTP 500). Each is
 * now raised as the engine type its category comes from: a DELETE of a node that still has relationships as an
 * {@link ObjectInUseException}, a use of a node deleted earlier in the statement as a {@link DeletedEntityException}, and a
 * {@code LOAD CSV} URL that cannot be opened as an {@link ExternalResourceException}.
 */
class Issue11974CypherUserErrorTypesTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/Issue11974CypherUserErrorTypesTest").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:A {x: 1})-[:R]->(:B), (:C {x: 1})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void deletingANodeThatStillHasRelationshipsIsObjectInUse() {
    final Throwable error = fail("MATCH (n:A) DELETE n");
    assertThat(CauseChain.find(error, ObjectInUseException.class)).isNotNull();
    assertThat(error).hasMessageContaining("DeleteConnectedNode");
    assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.VALIDATION);
  }

  @Test
  void usingANodeDeletedEarlierInTheStatementIsADeletedEntity() {
    for (final String statement : new String[] { "MATCH (n:C) DELETE n RETURN n.x AS x", "MATCH (n:C) DELETE n SET n.y = 1",
        "MATCH (n:C) DELETE n CREATE (n)-[:R]->(:B)" }) {
      final Throwable error = fail(statement);
      assertThat(CauseChain.find(error, DeletedEntityException.class)).as(statement).isNotNull();
      assertThat(error).as(statement).hasMessageContaining("DeletedEntityAccess");
      assertThat(ErrorCategory.of(error)).as(statement).isEqualTo(ErrorCategory.NOT_FOUND);
    }
    // the statement failed as a whole: the node it deleted is still there
    assertThat(database.countType("C", false)).isEqualTo(1);
  }

  @Test
  void aLoadCsvFileThatDoesNotExistIsAnExternalResource() {
    final Throwable error = fail("LOAD CSV FROM 'file:///Issue11974-no-such-file.csv' AS l RETURN l");
    assertThat(CauseChain.find(error, ExternalResourceException.class)).isNotNull();
    assertThat(error).hasMessageContaining("Error opening CSV file");
    assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.VALIDATION);
  }

  private Throwable fail(final String statement) {
    // the rows are pulled, as a client does: a LOAD CSV opens its file at the first row
    final Throwable error = catchThrowable(() -> database.transaction(() -> database.command("opencypher", statement).stream().count()));
    assertThat(error).as(statement).isNotNull();
    return error;
  }
}
