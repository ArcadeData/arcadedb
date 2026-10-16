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
package com.arcadedb.bolt;

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10153: a {@code CREATE} on a taken name reached a Bolt client as {@code Neo.ClientError.Statement.SemanticError} -
 * documented here as "the schema does not define this", the opposite of the refusal - or as the generic DatabaseError a driver
 * treats as a server fault. It is the title Neo4j answers with: the name is held by an index, by a constraint, or by a schema
 * object Neo4j has no word of its own for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10153BoltTakenNameCodeTest {

  @Test
  void anIndexOrAConstraintHoldingTheNameIsNamedAsNeo4jNamesIt() {
    assertThat(code(AlreadyExistsException.Kind.INDEX)).isEqualTo("Neo.ClientError.Schema.IndexWithNameAlreadyExists");
    assertThat(code(AlreadyExistsException.Kind.CONSTRAINT)).isEqualTo("Neo.ClientError.Schema.ConstraintWithNameAlreadyExists");
  }

  @Test
  void anyOtherTakenNameIsAnEquivalentSchemaRule() {
    for (final AlreadyExistsException.Kind kind : new AlreadyExistsException.Kind[] { AlreadyExistsException.Kind.TYPE,
        AlreadyExistsException.Kind.PROPERTY, AlreadyExistsException.Kind.BUCKET, null })
      assertThat(code(kind)).as("%s", kind).isEqualTo("Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists");
  }

  @Test
  void theCodeIsAClientErrorThroughAWrapperAndAMissingTypeKeepsItsOwn() {
    final String wrapped = BoltNetworkExecutor.classifyExecutionError(
        new CommandExecutionException("Error on executing the statement", new AlreadyExistsException(AlreadyExistsException.Kind.TYPE,
            "Type 'X' already exists")), BoltErrorCodes.DATABASE_ERROR);
    assertThat(wrapped).startsWith("Neo.ClientError.Schema.");
    assertThat(BoltNetworkExecutor.classifyExecutionError(new SchemaException("Type 'X' was not found"), BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
  }

  private static String code(final AlreadyExistsException.Kind kind) {
    return BoltNetworkExecutor.classifyExecutionError(new AlreadyExistsException(kind, "taken"), BoltErrorCodes.DATABASE_ERROR);
  }
}
