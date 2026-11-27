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
package com.arcadedb.server.ai;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.AdmissionSlotConformance;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The statement a tool of the AI Assistant runs, which gives its admission slot back before a wait, finds the slot on the thread
 * that runs it: the tool admits the statement and runs it on the same thread. See {@link AdmissionSlotConformance}.
 * <p>
 * The tool only reads, so the statement that reaches the hook is a query calling a function that creates the record.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class AdmissionSlotConformanceAiTest extends BaseGraphServerTest {
  private static final String TYPE     = "ConformanceAiTool";
  private static final String FUNCTION = "createConformanceAiRecord";

  /** Creates one record of {@link #TYPE}, in a transaction of its own, on the thread that runs the query. */
  private static final class CreatesARecord extends SQLFunctionAbstract {
    CreatesARecord() {
      super(FUNCTION);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      final Database database = context.getDatabase();
      database.transaction(() -> database.newDocument(TYPE).set("id", 1).save());
      return 1;
    }

    @Override
    public String getSyntax() {
      return FUNCTION + "()";
    }
  }

  @Test
  void aToolStatementGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType(TYPE);
    final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
    engine.getFunctionFactory().register(new CreatesARecord());
    try {
      final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0),
          getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null), getDatabaseName());

      AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "AI Assistant query_database", () -> {
        final JSONObject answer = new JSONObject(dispatcher.execute("query_database",
            new JSONObject().put("language", "sql").put("command", "SELECT " + FUNCTION + "() AS created")));
        assertThat(answer.has("error")).as(answer.toString()).isFalse();
        return null;
      });
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    } finally {
      engine.getFunctionFactory().unregister(FUNCTION);
    }
  }
}
