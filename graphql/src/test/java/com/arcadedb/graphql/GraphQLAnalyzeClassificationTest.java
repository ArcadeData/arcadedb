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
package com.arcadedb.graphql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code GraphQLQueryEngine.analyze()} used to classify any query it could not parse as {@code READ} /
 * idempotent - a fail-open default for a value consumed elsewhere as a read-only gate (for example
 * {@code SampleRecordsTool} in the {@code mcp} module). It also let a StackOverflowError from a
 * pathologically nested query escape uncaught. See issue #5853.
 */
class GraphQLAnalyzeClassificationTest extends AbstractGraphQLTest {

  @Test
  void aValidReadOnlyQueryIsClassifiedAsIdempotentRead() {
    executeTest(database -> {
      defineTypes(database);

      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("graphql").analyze("{ bookById(id: \"book-1\") { name } }");

      assertThat(analyzed.isIdempotent()).isTrue();
      assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);

      return null;
    });
  }

  @Test
  void aMalformedQueryIsClassifiedAsNonIdempotentNotRead() {
    // Previously: a query that fails to parse was silently classified as READ / idempotent, a fail-open
    // default for a value used elsewhere as a read-only authorization gate.
    executeTest(database -> {
      defineTypes(database);

      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("graphql").analyze("mutation { createV(name: \"x\" { name } }");

      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);

      return null;
    });
  }

  @Test
  void garbageInputIsClassifiedAsNonIdempotentNotRead() {
    executeTest(database -> {
      defineTypes(database);

      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("graphql").analyze("{{{ this is not graphql");

      assertThat(analyzed.isIdempotent()).isFalse();
      assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);

      return null;
    });
  }

  @Test
  void analyzeOfAPathologicallyNestedQueryDoesNotThrowAStackOverflow() {
    executeTest(database -> {
      defineTypes(database);

      final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
      final String query = "{ " + "a { ".repeat(maxDepth + 1) + "b" + " }".repeat(maxDepth + 1) + " }";

      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("graphql").analyze(query);

      // Cannot be parsed within the configured depth bound, so the conservative (non-idempotent)
      // classification applies - and, above all, analyze() must not crash the calling thread.
      assertThat(analyzed.isIdempotent()).isFalse();

      return null;
    });
  }

  @Test
  void aPathologicallyNestedReadQueryFailsAsQueryNotIdempotentNotAStackOverflow() {
    executeTest(database -> {
      defineTypes(database);

      final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
      final String query = "{ " + "a { ".repeat(maxDepth + 1) + "b" + " }".repeat(maxDepth + 1) + " }";

      assertThatThrownBy(() -> database.query("graphql", query).close()).isInstanceOf(QueryNotIdempotentException.class);

      return null;
    });
  }

  @Test
  void aPathologicallyNestedCommandFailsAsANormalParseErrorNotAStackOverflow() {
    executeTest(database -> {
      defineTypes(database);

      final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
      final String query = "{ " + "a { ".repeat(maxDepth + 1) + "b" + " }".repeat(maxDepth + 1) + " }";

      // command() does not gate on analyze(); it goes straight to execution, where the same depth guard
      // must still turn the pathological nesting into a normal parse error rather than a crash.
      assertThatThrownBy(() -> database.command("graphql", query).close()).isInstanceOf(CommandParsingException.class);

      return null;
    });
  }
}
