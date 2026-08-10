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
package com.arcadedb.graphql.parser;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class GraphQLParserTest {
  @Test
  void basic() throws Exception {
    GraphQLParser.parse("{ hero { name friends { name } }}");
  }

  @Test
  void deeplyNestedSelectionSetIsRejectedAsAParseErrorNotAStackOverflow() {
    // Regression test for issue #5853: an ~18 KB query with deeply nested selection sets used to escape
    // as a StackOverflowError instead of a normal ParseException.
    final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
    final String query = "{ " + "a { ".repeat(maxDepth + 1) + "b" + " }".repeat(maxDepth + 1) + " }";

    final Throwable error = catchThrowable(() -> GraphQLParser.parse(query));

    assertThat(error).isInstanceOf(ParseException.class);
    assertThat(error).isNotInstanceOf(StackOverflowError.class);
    assertThat(error.getMessage()).contains("maxNestingDepth");
  }

  @Test
  void deeplyNestedListLiteralIsRejectedAsAParseErrorNotAStackOverflow() {
    // Regression test for issue #5853's follow-up finding: the depth guard originally counted only '{'/'}'
    // (selection sets), but a chain of nested list literals - '[' ... ']' via the mutually-recursive
    // Value/ListValue grammar rules - drives the exact same unbounded recursive-descent stack growth and
    // never contains a single brace, so it slipped past the guard entirely.
    final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
    final String query = "{ field(arg: " + "[".repeat(maxDepth + 1) + "]".repeat(maxDepth + 1) + ") }";

    final Throwable error = catchThrowable(() -> GraphQLParser.parse(query));

    assertThat(error).isInstanceOf(ParseException.class);
    assertThat(error).isNotInstanceOf(StackOverflowError.class);
    assertThat(error.getMessage()).contains("maxNestingDepth");
  }

  @Test
  void listNestingAtExactlyTheConfiguredLimitStillParses() throws Exception {
    final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
    final String query = "{ field(arg: " + "[".repeat(maxDepth - 1) + "]".repeat(maxDepth - 1) + ") }";

    final Document ast = GraphQLParser.parse(query);

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void nestingAtExactlyTheConfiguredLimitStillParses() throws Exception {
    // The guard must not reject legitimate queries that stay within the configured bound.
    final int maxDepth = GlobalConfiguration.GRAPHQL_MAX_NESTING_DEPTH.getValueAsInteger();
    final String query = "{ " + "a { ".repeat(maxDepth - 1) + "b" + " }".repeat(maxDepth - 1) + " }";

    final Document ast = GraphQLParser.parse(query);

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void aGenuinelyMalformedQueryStillThrowsAParseException() {
    assertThatThrownBy(() -> GraphQLParser.parse("{{{ this is not graphql")).isInstanceOf(ParseException.class);
  }

  @Test
  void theExactPayloadFromIssue5853NoLongerOverflowsTheStack() {
    // The literal reproduction from issue #5853: 3085 levels of "a { ... }" nesting (~18.5 KB) crashed the
    // calling thread with a StackOverflowError on a default JVM stack. It is now rejected as a ParseException
    // well before it gets anywhere near the native stack, since it is far past the default configured depth.
    final String query = "{ " + "a { ".repeat(3085) + "b" + " }".repeat(3085) + " }";

    final Throwable error = catchThrowable(() -> GraphQLParser.parse(query));

    assertThat(error).isInstanceOf(ParseException.class);
    assertThat(error).isNotInstanceOf(StackOverflowError.class);
  }

  @Test
  void lookup() throws Exception {
    final Document ast = GraphQLParser.parse("""
        { bookById(id: "book-1"){\
          id\
              name\
          pageCount\
          author {\
            firstName\
                lastName\
          }\
        }\
        }""");

    assertThat(ast.children.length > 0).isTrue();

    //ast.dump("-");
  }
}
