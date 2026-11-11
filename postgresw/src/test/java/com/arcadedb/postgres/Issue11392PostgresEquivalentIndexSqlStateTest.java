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
package com.arcadedb.postgres;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.EquivalentIndexException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11392: an index equivalent to one the type already has under another name reached a Postgres client as {@code 22023}
 * invalid_parameter_value, where the same-name duplicate answers {@code 42P07} (#10153). An index is a relation, so the
 * equivalent one is {@code 42P07} too; another definition on the same properties stays the argument error it is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11392PostgresEquivalentIndexSqlStateTest {

  @Test
  void anEquivalentIndexIsDuplicateTable() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new EquivalentIndexException("equivalent"))).isEqualTo("42P07");
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandExecutionException("Error on executing the statement", new EquivalentIndexException("equivalent"))))
        .isEqualTo("42P07");
  }

  @Test
  void anotherDefinitionStaysAnInvalidParameter() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Drop it first if the definition has to change")))
        .isEqualTo("22023");
  }
}
