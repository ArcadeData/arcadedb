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

import com.arcadedb.database.RID;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.EquivalentIndexException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13664: the Neo4j 5.26 title of each "rule that exists" refusal, by what the engine says about it - whether the name asked
 * for holds the rule asked for, and whether the rule asked for holds another name.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13664BoltSchemaRuleCodeTest {

  @Test
  void theSameRuleUnderTheSameNameIsAnEquivalentSchemaRule() {
    assertThat(code(new AlreadyExistsException(AlreadyExistsException.Kind.INDEX, "taken", true)))
        .isEqualTo(BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR);
    assertThat(code(new AlreadyExistsException(AlreadyExistsException.Kind.CONSTRAINT, "taken", true)))
        .isEqualTo(BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR);
    assertThat(code(new EquivalentIndexException("equivalent", AlreadyExistsException.Kind.INDEX, false)))
        .isEqualTo(BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR);
    // the plain form, as a client rebuilds it from an answer that carried nothing else
    assertThat(code(new EquivalentIndexException("equivalent"))).isEqualTo(BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR);
  }

  @Test
  void anotherRuleUnderTheSameNameIsATakenName() {
    assertThat(code(new AlreadyExistsException(AlreadyExistsException.Kind.INDEX, "taken", false)))
        .isEqualTo("Neo.ClientError.Schema.IndexWithNameAlreadyExists");
    assertThat(code(new AlreadyExistsException(AlreadyExistsException.Kind.CONSTRAINT, "taken", false)))
        .isEqualTo("Neo.ClientError.Schema.ConstraintWithNameAlreadyExists");
  }

  @Test
  void theSameRuleUnderAnotherNameIsAnIndexOrAConstraintThatExists() {
    assertThat(code(new EquivalentIndexException("equivalent", AlreadyExistsException.Kind.INDEX, true)))
        .isEqualTo("Neo.ClientError.Schema.IndexAlreadyExists");
    assertThat(code(new EquivalentIndexException("equivalent", AlreadyExistsException.Kind.CONSTRAINT, true)))
        .isEqualTo("Neo.ClientError.Schema.ConstraintAlreadyExists");
    // through the wrapper a query engine puts around every failure of a statement
    assertThat(code(new CommandExecutionException("Error executing Cypher command",
        new EquivalentIndexException("equivalent", AlreadyExistsException.Kind.CONSTRAINT, true))))
        .isEqualTo("Neo.ClientError.Schema.ConstraintAlreadyExists");
  }

  @Test
  void aUniquenessTextIsLeftAloneWithoutADatabaseOrForAnotherCode() {
    final DuplicatedKeyException duplicate = new DuplicatedKeyException("P[a]", "[1]", new RID(1, 0));
    assertThat(BoltNetworkExecutor.uniquenessViolationText(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR, "engine text", duplicate, null))
        .isEqualTo("engine text");
    assertThat(BoltNetworkExecutor.uniquenessViolationText(BoltErrorCodes.DATABASE_ERROR, "engine text", duplicate, null))
        .isEqualTo("engine text");
  }

  private static String code(final Throwable error) {
    return BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR);
  }
}
