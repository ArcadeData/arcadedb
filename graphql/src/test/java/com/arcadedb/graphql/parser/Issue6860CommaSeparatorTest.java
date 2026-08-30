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
 */
package com.arcadedb.graphql.parser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6860: the comma is insignificant whitespace in GraphQL, but the grammar declared it as a
 * real token and then handled it inconsistently - {@code Arguments} required one between arguments, every other list
 * production rejected one outright. The form every GraphQL client emits for a multi-variable operation,
 * {@code query($a: String, $b: Int)}, therefore did not parse.
 */
class Issue6860CommaSeparatorTest {

  @Test
  void multipleVariableDefinitionsSeparatedByCommasParse() throws Exception {
    // The exact form reported in the issue.
    final Document ast = GraphQLParser.parse("query($t: String, $d: String) { bookByName(name: $t) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void multipleVariableDefinitionsWithoutCommasStillParse() throws Exception {
    // The only form accepted before the fix must keep working: the change is a widening, never a swap.
    final Document ast = GraphQLParser.parse("query($t: String $d: String) { bookByName(name: $t) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void argumentsSeparatedByCommasStillParse() throws Exception {
    // Arguments were the one production that required the comma; it must remain accepted.
    final Document ast = GraphQLParser.parse("{ bookBy(id: \"1\", name: \"a\", pageCount: 3) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void argumentsWithoutCommasParse() throws Exception {
    // The mirror image of the reported bug: Arguments used to *require* the comma, so the spec-legal comma-less
    // form was rejected there while every other list production rejected the comma.
    final Document ast = GraphQLParser.parse("{ bookBy(id: \"1\" name: \"a\" pageCount: 3) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void selectionSetSeparatedByCommasParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ hero { name, friends { name, id } } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void listValueSeparatedByCommasParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ books(ids: [1, 2, 3]) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void objectValueSeparatedByCommasParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ books(filter: {name: \"a\", pageCount: 3}) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void listAndObjectValuesWithVariablesSeparatedByCommasParse() throws Exception {
    final Document ast = GraphQLParser.parse(
        "query($a: Int, $b: Int) { books(ids: [$a, $b], filter: {min: $a, max: $b}) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void directiveArgumentsSeparatedByCommasParse() throws Exception {
    // The SDL form used throughout the module's own tests - it happened to work only because Arguments required
    // the comma, which is the other half of the same inconsistency.
    final Document ast = GraphQLParser.parse("""
        type Book {
          authors: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
        }""");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void schemaDefinitionListsSeparatedByCommasParse() throws Exception {
    // ArgumentsDefinition, FieldsDefinition, InputFieldsDefinition and EnumValuesDefinition all rejected the comma,
    // which is why every `type Query` in this module is written without one.
    final Document ast = GraphQLParser.parse("""
        type Query {
          books(inStock: Boolean, rating: Float): [Book],
          bookById(id: ID): Book
        }

        type Book {
          id: ID,
          name: String,
          pageCount: Int
        }

        input BookFilter {
          name: String,
          minPages: Int
        }

        enum Genre {
          FANTASY,
          SCIFI
        }""");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void redundantAndTrailingCommasAreIgnored() throws Exception {
    // The specification treats the comma as pure whitespace, so any number of them anywhere is legal.
    final Document ast = GraphQLParser.parse("query($a: String,,) { bookBy(id: $a,) { id, } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void singleDigitIntegerLiteralsParse() throws Exception {
    // Same root cause, different token: DIGIT and NON_ZERO_DIGIT were declared as real tokens even though they only
    // ever appear inside INTEGER_LITERAL/FLOAT_LITERAL. A one-character integer matched DIGIT exactly as far as it
    // matched INTEGER_LITERAL and DIGIT was declared first, so "3" was a syntax error while "42" parsed.
    final Document ast = GraphQLParser.parse("{ books(pageCount: 3, rating: 4.5, offset: 0) { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void emptyCommentLineIsSkipped() throws Exception {
    // And again: SKIP_COMMENT is HASH followed by zero or more characters, so an empty comment is exactly as long as
    // the bare HASH token that was declared before it, and a comment line with nothing on it failed to lex.
    final Document ast = GraphQLParser.parse("query {\n#\n  hero { name }\n}");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void commentsAreStillSkipped() throws Exception {
    final Document ast = GraphQLParser.parse("query { # pick the hero\n  hero { name }\n}");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void twoStringLiteralsSeparatedOnlyByWhitespaceAreNotJoined() throws Exception {
    // STRING_LITERAL carried an alternative that accepted any character - the double quote included - when whitespace
    // followed it, so the closing quote of the first literal was consumable as content and the longest-match rule
    // joined the two into one. That is what made a comma-less argument list carrying strings unlexable, and it also
    // fires between two separate directives.
    final Document ast = GraphQLParser.parse("{ bookBy(id: \"1\" name: \"a\") { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }

  @Test
  void escapedQuotesInsideStringLiteralsStillParse() throws Exception {
    // The escape alternatives are untouched: only the alternative that accepted an *unescaped* quote is gone.
    final Document ast = GraphQLParser.parse("{ noteByText(text: \"He said \\\"hi\\\" to me\") { id } }");

    assertThat(ast.children.length > 0).isTrue();
  }
}
