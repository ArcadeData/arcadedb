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

import com.arcadedb.exception.CommandParsingException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11451: every kind of the specification's Value grammar (Variable, IntValue, FloatValue, StringValue - quoted and block -,
 * BooleanValue, NullValue, EnumValue, ListValue, ObjectValue) parses, in an argument and in a default value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11451ValueGrammarTest {
  @Test
  void everyValueKindOfTheSpecificationParsesAsAnArgument() throws Exception {
    final Map<String, AbstractValue> values = arguments("""
        { f(variable: $v int: 42 long: 1700000000000 negative: -1700000000000 huge: 123456789012345678901234567890
            float: 1.5e3 string: "s" block: \"""b\""" yes: true no: false nothing: null enum: RED
            list: [1, null, "x"] object: { k: null } ) }""");

    assertThat(values.get("variable")).isInstanceOf(VariableLiteral.class);
    assertThat(values.get("int").getValue()).isEqualTo(42);
    assertThat(values.get("long").getValue()).isEqualTo(1_700_000_000_000L);
    assertThat(values.get("negative").getValue()).isEqualTo(-1_700_000_000_000L);
    assertThat(values.get("huge").getValue()).isEqualTo(new BigInteger("123456789012345678901234567890"));
    assertThat(values.get("float").getValue()).isEqualTo(new BigDecimal("1.5e3"));
    assertThat(values.get("string").getValue()).isEqualTo("s");
    assertThat(values.get("block").getValue()).isEqualTo("b");
    assertThat(values.get("yes").getValue()).isEqualTo(true);
    assertThat(values.get("no").getValue()).isEqualTo(false);
    assertThat(values.get("nothing")).isInstanceOf(NullValue.class);
    assertThat(values.get("nothing").getValue()).isNull();
    assertThat(values.get("enum").getValue()).isEqualTo("RED");
    assertThat(values.get("list")).isInstanceOf(ListValueWithVariable.class);
    assertThat(values.get("object")).isInstanceOf(ObjectValueWithVariable.class);
  }

  @Test
  void nullIsADefaultValue() throws Exception {
    final Document document = GraphQLParser.parse("query q($n: String = null, $l: [String] = [null]) { f(n: $n) }");
    final OperationDefinition operation = (OperationDefinition) document.getDefinitions().getFirst();
    final List<VariableDefinition> definitions = operation.getVariableDefinitions().getVariableDefinitions();
    assertThat(definitions.get(0).getDefaultValue().getValue().getValue()).isInstanceOf(NullValue.class);
    assertThat(definitions.get(1).getDefaultValue().getValue().getListValue().getValues().getFirst().getValue()).isInstanceOf(
        NullValue.class);

    // and in a schema
    GraphQLParser.parse("type Query { f(a: String = null, b: [Int] = [1, null]): String }");
  }

  @Test
  void blockStringFollowsTheSpecificationsBlockStringValue() throws Exception {
    // the common indentation of the lines after the first is removed, and so are the leading and trailing blank lines
    assertThat(block("\"\"\"\n    Hello,\n      World!\n\n    Yours,\n      GraphQL.\n  \"\"\"")).isEqualTo(
        "Hello,\n  World!\n\nYours,\n  GraphQL.");
    // escapes other than \""" are kept as written: a block string is raw
    assertThat(block("\"\"\"a\\nb \\u0041 \\\\\"\"\"")).isEqualTo("a\\nb \\u0041 \\\\");
    assertThat(block("\"\"\"say \\\"\"\" hi\"\"\"")).isEqualTo("say \"\"\" hi");
    // single and double quotes are content
    assertThat(block("\"\"\"a \"b\" \"\"c\"\" d\"\"\"")).isEqualTo("a \"b\" \"\"c\"\" d");
    assertThat(block("\"\"\"\"\"\"")).isEqualTo("");
    // CRLF and CR line terminators are lines too
    assertThat(block("\"\"\"\r\n  a\r\n  b\r\n\"\"\"")).isEqualTo("a\nb");
    // the first line keeps its indentation: it is not part of the common one
    assertThat(block("\"\"\"  first\n    second\"\"\"")).isEqualTo("  first\nsecond");
  }

  @Test
  void blockStringsNextToEachOtherStayTwoValues() throws Exception {
    final Map<String, AbstractValue> values = arguments("{ f(a: \"\"\"x\"\"\" b: \"\"\"y\"\"\") }");
    assertThat(values.get("a").getValue()).isEqualTo("x");
    assertThat(values.get("b").getValue()).isEqualTo("y");

    // an escaped closing quote does not end the literal
    final Map<String, AbstractValue> escaped = arguments("{ f(a: \"\"\"x\\\"\"\"\"\"\" b: \"\"\"y\"\"\") }");
    assertThat(escaped.get("a").getValue()).isEqualTo("x\"\"\"");
    assertThat(escaped.get("b").getValue()).isEqualTo("y");
  }

  @Test
  void numberLiteralsLongerThanAnyScalarCanTakeAreRefusedBeforeTheyAreParsed() throws Exception {
    // the widest literal taken is a signed 128-bit value
    final String widest = "1".repeat(IntValue.MAX_DIGITS);
    assertThat(arguments("{ f(a: " + widest + ") }").get("a").getValue()).isEqualTo(new BigInteger(widest));
    assertThat(arguments("{ f(a: -" + widest + ") }").get("a").getValue()).isEqualTo(new BigInteger("-" + widest));

    // a document of digits must not buy the superlinear cost of BigInteger and BigDecimal parsing
    final AbstractValue tooWide = arguments("{ f(a: " + "9".repeat(100_000) + ") }").get("a");
    assertThatThrownBy(tooWide::getValue).isInstanceOf(CommandParsingException.class).hasMessageContaining("100000 digits");

    final AbstractValue tooLongFloat = arguments("{ f(a: 1." + "5".repeat(100_000) + ") }").get("a");
    assertThatThrownBy(tooLongFloat::getValue).isInstanceOf(CommandParsingException.class).hasMessageContaining("Float");
    assertThat(arguments("{ f(a: 1.5e308) }").get("a").getValue()).isEqualTo(new BigDecimal("1.5e308"));
  }

  @Test
  void unterminatedBlockStringIsALexicalError() {
    assertThatThrownBy(() -> GraphQLParser.parse("{ f(a: \"\"\"x) }")).isInstanceOfAny(ParseException.class,
        TokenMgrException.class);
  }

  private static String block(final String literal) throws Exception {
    return (String) arguments("{ f(a: " + literal + ") }").get("a").getValue();
  }

  private static Map<String, AbstractValue> arguments(final String document) throws Exception {
    final OperationDefinition operation = (OperationDefinition) GraphQLParser.parse(document).getDefinitions().getFirst();
    final Map<String, AbstractValue> values = new LinkedHashMap<>();
    for (final Argument argument : operation.getSelectionSet().getSelections().getFirst().getArguments().getList())
      values.put(argument.getName(), argument.getValueWithVariable().getValue());
    return values;
  }
}
