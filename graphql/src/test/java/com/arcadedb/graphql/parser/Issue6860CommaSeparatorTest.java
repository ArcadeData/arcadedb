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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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

    final VariableDefinitions definitions = findFirst(ast, VariableDefinitions.class);
    assertThat(definitions.getVariableDefinitions()).hasSize(2);
    assertThat(definitions.getVariableDefinitions().get(0).getVariableLiteral().getName()).isEqualTo("t");
    assertThat(definitions.getVariableDefinitions().get(1).getVariableLiteral().getName()).isEqualTo("d");
  }

  @Test
  void multipleVariableDefinitionsWithoutCommasStillParse() throws Exception {
    // The only form accepted before the fix must keep working: the change is a widening, never a swap.
    final Document ast = GraphQLParser.parse("query($t: String $d: String) { bookByName(name: $t) { id } }");

    assertThat(findFirst(ast, VariableDefinitions.class).getVariableDefinitions()).hasSize(2);
  }

  @Test
  void argumentsSeparatedByCommasStillParse() throws Exception {
    // Arguments were the one production that required the comma; it must remain accepted.
    final Document ast = GraphQLParser.parse("{ bookBy(id: \"1\", name: \"a\", pageCount: 3) { id } }");

    assertThat(argumentNames(ast)).containsExactly("id", "name", "pageCount");
  }

  @Test
  void argumentsWithoutCommasParse() throws Exception {
    // The mirror image of the reported bug: Arguments used to *require* the comma, so the spec-legal comma-less
    // form was rejected there while every other list production rejected the comma.
    final Document ast = GraphQLParser.parse("{ bookBy(id: \"1\" name: \"a\" pageCount: 3) { id } }");

    assertThat(argumentNames(ast)).containsExactly("id", "name", "pageCount");
  }

  @Test
  void selectionSetSeparatedByCommasParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ hero { name, friends { name, id } } }");

    // The outermost selection set holds `hero` alone; the one below it must hold both selections, not one.
    assertThat(findFirst(ast, SelectionSet.class).getSelections()).hasSize(1);
    assertThat(findAll(ast, SelectionSet.class).get(2).getSelections()).hasSize(2);
  }

  @Test
  void listValueSeparatedByCommasParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ books(ids: [1, 2, 3]) { id } }");

    assertThat((List<?>) findFirst(ast, ListValueWithVariable.class).getValue()).hasSize(3);
  }

  @Test
  void objectValueSeparatedByCommasParses() throws Exception {
    final Document ast = GraphQLParser.parse("{ books(filter: {name: \"a\", pageCount: 3}) { id } }");

    assertThat((List<?>) findFirst(ast, ObjectValueWithVariable.class).getValue()).hasSize(2);
  }

  @Test
  void listAndObjectValuesWithVariablesSeparatedByCommasParse() throws Exception {
    final Document ast = GraphQLParser.parse(
        "query($a: Int, $b: Int) { books(ids: [$a, $b], filter: {min: $a, max: $b}) { id } }");

    assertThat(argumentNames(ast)).containsExactly("ids", "filter");
    assertThat((List<?>) findFirst(ast, ListValueWithVariable.class).getValue()).hasSize(2);
    assertThat((List<?>) findFirst(ast, ObjectValueWithVariable.class).getValue()).hasSize(2);
  }

  @Test
  void directiveArgumentsSeparatedByCommasParse() throws Exception {
    // The SDL form used throughout the module's own tests - it happened to work only because Arguments required
    // the comma, which is the other half of the same inconsistency.
    final Document ast = GraphQLParser.parse("""
        type Book {
          authors: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
        }""");

    assertThat(argumentNames(ast)).containsExactly("type", "direction");
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

    // Two field definitions on Query - the first of which carries two argument definitions - three on Book, two on
    // the input type and two enum values: a comma quietly swallowing its neighbour would show up as a short list.
    assertThat(ast.getDefinitions()).hasSize(4);
    assertThat(findAll(ast, FieldDefinition.class)).hasSize(5);
    assertThat(findFirst(ast, ArgumentsDefinition.class).getInputValueDefinitions()).hasSize(2);
    assertThat(findAll(findFirst(ast, InputObjectTypeDefinition.class), InputValueDefinition.class)).hasSize(2);
    assertThat(findAll(findFirst(ast, EnumTypeDefinition.class), EnumValueDefinition.class)).hasSize(2);
  }

  @Test
  void redundantAndTrailingCommasAreIgnored() throws Exception {
    // The specification treats the comma as pure whitespace, so any number of them anywhere is legal.
    final Document ast = GraphQLParser.parse("query($a: String,,) { bookBy(id: $a,) { id, } }");

    assertThat(findFirst(ast, VariableDefinitions.class).getVariableDefinitions()).hasSize(1);
    assertThat(argumentNames(ast)).containsExactly("id");
    final List<SelectionSet> selectionSets = findAll(ast, SelectionSet.class);
    assertThat(selectionSets.get(selectionSets.size() - 1).getSelections()).hasSize(1);
  }

  @Test
  void singleDigitIntegerLiteralsParse() throws Exception {
    // Same root cause, different token: DIGIT and NON_ZERO_DIGIT were declared as real tokens even though they only
    // ever appear inside INTEGER_LITERAL/FLOAT_LITERAL. A one-character integer matched DIGIT exactly as far as it
    // matched INTEGER_LITERAL and DIGIT was declared first, so "3" was a syntax error while "42" parsed.
    final Document ast = GraphQLParser.parse("{ books(pageCount: 3, rating: 4.5, offset: 0) { id } }");

    assertThat(argumentNames(ast)).containsExactly("pageCount", "rating", "offset");
  }

  @Test
  void emptyCommentLineIsSkipped() throws Exception {
    // And again: SKIP_COMMENT is HASH followed by zero or more characters, so an empty comment is exactly as long as
    // the bare HASH token that was declared before it, and a comment line with nothing on it failed to lex.
    final Document ast = GraphQLParser.parse("query {\n#\n  hero { name }\n}");

    assertThat(findAll(ast, SelectionSet.class).get(0).getSelections()).hasSize(1);
  }

  @Test
  void commentsAreStillSkipped() throws Exception {
    final Document ast = GraphQLParser.parse("query { # pick the hero\n  hero { name }\n}");

    assertThat(findAll(ast, SelectionSet.class).get(0).getSelections()).hasSize(1);
  }

  @Test
  void twoStringLiteralsSeparatedOnlyByWhitespaceAreNotJoined() throws Exception {
    // STRING_LITERAL carried an alternative that accepted any character - the double quote included - when whitespace
    // followed it, so the closing quote of the first literal was consumable as content and the longest-match rule
    // joined the two into one. That is what made a comma-less argument list carrying strings unlexable, and it also
    // fires between two separate directives.
    final Document ast = GraphQLParser.parse("{ bookBy(id: \"1\" name: \"a\") { id } }");

    // Asserting on the argument list rather than on "it parsed": if the two literals were joined again the call
    // would still be a valid one-argument call carrying the joined string, and a non-empty-AST check would pass.
    final Arguments arguments = findFirst(ast, Arguments.class);
    assertThat(arguments.getList()).hasSize(2);
    assertThat(arguments.getList().get(0).getName()).isEqualTo("id");
    assertThat(stringValueOf(arguments.getList().get(0))).isEqualTo("1");
    assertThat(arguments.getList().get(1).getName()).isEqualTo("name");
    assertThat(stringValueOf(arguments.getList().get(1))).isEqualTo("a");
  }

  @Test
  void escapedQuotesInsideStringLiteralsStillParse() throws Exception {
    // The escape alternatives are untouched: only the alternative that accepted an *unescaped* quote is gone.
    final Document ast = GraphQLParser.parse("{ noteByText(text: \"He said \\\"hi\\\" to me\") { id } }");

    assertThat(stringValueOf(findFirst(ast, Arguments.class).getList().get(0))).isEqualTo("He said \"hi\" to me");
  }

  @Test
  void unescapedBackslashInsideAStringLiteralIsRejected() {
    // The removed alternative accepted an unescaped backslash as readily as an unescaped quote, as long as
    // whitespace followed it. GraphQL requires the backslash to be escaped, so this has to be a lexer error now
    // rather than a literal that silently swallows whatever comes next.
    final Throwable error = catchThrowable(() -> GraphQLParser.parse("{ noteByText(text: \"a\\ b\") { id } }"));

    assertThat(error).isInstanceOfAny(ParseException.class, TokenMgrException.class);
  }

  @Test
  void escapedBackslashInsideAStringLiteralIsKept() throws Exception {
    final Document ast = GraphQLParser.parse("{ noteByText(text: \"C:\\\\temp\") { id } }");

    assertThat(stringValueOf(findFirst(ast, Arguments.class).getList().get(0))).isEqualTo("C:\\temp");
  }

  private static List<String> argumentNames(final Document ast) {
    return findFirst(ast, Arguments.class).getList().stream().map(Argument::getName).toList();
  }

  private static String stringValueOf(final Argument argument) {
    return ((StringValue) argument.getValueWithVariable().getValue()).getValue();
  }

  private static <T> T findFirst(final Node node, final Class<T> type) {
    final List<T> found = findAll(node, type);
    assertThat(found).as("no %s node in the parsed document", type.getSimpleName()).isNotEmpty();
    return found.get(0);
  }

  private static <T> List<T> findAll(final Node node, final Class<T> type) {
    final List<T> found = new ArrayList<>();
    collect(node, type, found);
    return found;
  }

  private static <T> void collect(final Node node, final Class<T> type, final List<T> found) {
    if (type.isInstance(node))
      found.add(type.cast(node));
    for (int i = 0; i < node.jjtGetNumChildren(); i++)
      collect(node.jjtGetChild(i), type, found);
  }
}
