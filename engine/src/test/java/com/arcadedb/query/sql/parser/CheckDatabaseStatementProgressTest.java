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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The CHECK DATABASE statement must retire its {@link OperationProgressRegistry} entry even when the check
 * itself fails mid-run (issue #5372): a leaked entry would make pollers show a phantom operation forever.
 * Uses the statement's package-private checker seam to inject a checker that throws.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseStatementProgressTest extends TestHelper {

  @Test
  void operationIsRetiredWhenTheCheckFails() {
    final CommandContext context = new BasicCommandContext();
    ((BasicCommandContext) context).setDatabase(database);

    final CheckDatabaseStatement statement = new CheckDatabaseStatement(-1) {
      @Override
      DatabaseChecker createChecker(final CommandContext ctx) {
        return new DatabaseChecker(ctx.getDatabase().getWrappedDatabaseInstance()) {
          @Override
          public Map<String, Object> check() {
            throw new RuntimeException("simulated mid-check failure");
          }
        };
      }
    };

    assertThatThrownBy(() -> statement.executeSimple(context))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("simulated mid-check failure");

    // THE FINALLY BLOCK MUST HAVE RETIRED THE OPERATION DESPITE THE FAILURE.
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }
}
