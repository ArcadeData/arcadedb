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
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.antlr.SQLASTBuilder;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.Statement;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4594: {@code LetExpressionStep} and {@code LetQueryStep} cast the upstream row to
 * {@code ResultInternal} with {@code (ResultInternal) source.next()}. When the upstream emits a {@code Result} that is
 * not a {@code ResultInternal} (e.g. a wrapper {@code Result}), that cast throws a {@code ClassCastException}.
 * <p>
 * The fix replaces the unconditional cast with an {@code instanceof} guard: per-row metadata is only set when the row
 * is a {@code ResultInternal}, while the context variable is always set so {@code $varName} keeps resolving.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LetStepNonResultInternalTest extends TestHelper {

  @Test
  void letExpressionStepHandlesNonResultInternalUpstream() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final ResultInternal inner = new ResultInternal(database);
    inner.setProperty("name", "Alice");

    final LetExpressionStep step = new LetExpressionStep(new Identifier("$a"), parseExpression("name"), context);
    step.setPrevious(new SingleRowStep(context, new WrapperResult(inner)));

    final ResultSet rs = step.syncPull(context, 100);
    assertThat(rs.hasNext()).isTrue();

    // Before the fix this line threw ClassCastException inside next().
    final Result returned = rs.next();
    assertThat(returned).isInstanceOf(WrapperResult.class);

    // The wrapper cannot carry per-row metadata, but the LET value is still exposed through the context variable.
    assertThat(context.getVariable("$a")).isEqualTo("Alice");
    rs.close();
  }

  @Test
  void letQueryStepHandlesNonResultInternalUpstream() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final ResultInternal inner = new ResultInternal(database);
    inner.setProperty("name", "Bob");

    final Statement subQuery = parseStatement("SELECT 42 as answer");
    final LetQueryStep step = new LetQueryStep(new Identifier("$a"), subQuery, context);
    step.setPrevious(new SingleRowStep(context, new WrapperResult(inner)));

    final ResultSet rs = step.syncPull(context, 100);
    assertThat(rs.hasNext()).isTrue();

    // Before the fix this line threw ClassCastException inside next().
    final Result returned = rs.next();
    assertThat(returned).isInstanceOf(WrapperResult.class);

    final Object value = context.getVariable("$a");
    assertThat(value).isInstanceOf(List.class);
    assertThat((List<?>) value).hasSize(1);
    rs.close();
  }

  private Expression parseExpression(final String expr) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final SQLParser parser = new SQLParser(tokens);
    return new SQLASTBuilder().visitParseExpression(parser.parseExpression());
  }

  private Statement parseStatement(final String sql) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(sql));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final SQLParser parser = new SQLParser(tokens);
    return new SQLASTBuilder().visitParse(parser.parse());
  }

  /**
   * Minimal execution step that emits exactly one row.
   */
  private static class SingleRowStep extends AbstractExecutionStep {
    private final Result row;

    SingleRowStep(final CommandContext context, final Result row) {
      super(context);
      this.row = row;
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
      return new ResultSet() {
        private boolean served = false;

        @Override
        public boolean hasNext() {
          return !served;
        }

        @Override
        public Result next() {
          served = true;
          return row;
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "SingleRowStep";
    }
  }

  /**
   * A {@code Result} that is intentionally NOT a {@code ResultInternal}, delegating to a wrapped instance. Represents
   * the wrapper {@code Result} types described in issue #4594.
   */
  private static class WrapperResult implements Result {
    private final Result delegate;

    WrapperResult(final Result delegate) {
      this.delegate = delegate;
    }

    @Override
    public <T> T getProperty(final String name) {
      return delegate.getProperty(name);
    }

    @Override
    public <T> T getProperty(final String name, final Object defaultValue) {
      return delegate.getProperty(name, defaultValue);
    }

    @Override
    public Record getElementProperty(final String name) {
      return delegate.getElementProperty(name);
    }

    @Override
    public Set<String> getPropertyNames() {
      return delegate.getPropertyNames();
    }

    @Override
    public Optional<RID> getIdentity() {
      return delegate.getIdentity();
    }

    @Override
    public boolean isElement() {
      return delegate.isElement();
    }

    @Override
    public Optional<Document> getElement() {
      return delegate.getElement();
    }

    @Override
    public Document toElement() {
      return delegate.toElement();
    }

    @Override
    public Optional<Record> getRecord() {
      return delegate.getRecord();
    }

    @Override
    public boolean isProjection() {
      return delegate.isProjection();
    }

    @Override
    public Object getMetadata(final String key) {
      return delegate.getMetadata(key);
    }

    @Override
    public Set<String> getMetadataKeys() {
      return delegate.getMetadataKeys();
    }

    @Override
    public Database getDatabase() {
      return delegate.getDatabase();
    }

    @Override
    public boolean hasProperty(final String varName) {
      return delegate.hasProperty(varName);
    }

    @Override
    public Map<String, Object> toMap() {
      return delegate.toMap();
    }
  }
}
