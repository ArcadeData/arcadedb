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
package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A JavaScript user function is declared from its name, its parameter names and its body, and the declaration must
 * only ever declare: the body is compiled on its own, and the name and the parameters must be plain identifiers.
 * Nothing of the definition runs when the function is declared, only when it is called.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class JavascriptFunctionDeclarationTest extends TestHelper {
  /** A body that ends the function early and carries statements of its own, then opens a function to take the closing brace. */
  private static final String BODY_ENDING_EARLY = "} globalThis.declaredMarker = 1; function z() {";

  /** The same, for a declaration that is wrapped in parentheses. */
  private static final String BODY_ENDING_INTO_AN_EXPRESSION = "}), (globalThis.declaredMarker = 1, function() {";

  @Test
  void bodyIsCompiledOnItsOwn() {
    final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "decl");
    library.registerFunction(new JavascriptFunctionDefinition("probe", "return typeof declaredMarker;"));

    for (final String body : new String[] { BODY_ENDING_EARLY, BODY_ENDING_INTO_AN_EXPRESSION, "}", "return 1; }", "{" }) {
      assertThatThrownBy(() -> library.registerFunction(new JavascriptFunctionDefinition("ending", body, "a")))
          .as("body '%s'", body)
          .isInstanceOf(FunctionExecutionException.class);

      assertThat(library.hasFunction("ending")).isFalse();
    }
    // THE LIBRARY STAYS USABLE AND NOTHING OF THE REFUSED BODIES RAN WHILE THE LIBRARY WAS REBUILT
    assertThat(library.getFunction("probe").execute()).isEqualTo("undefined");
  }

  @Test
  void bodyIsCompiledOnItsOwnThroughSql() {
    database.command("sql", "DEFINE FUNCTION decl.probe \"return typeof declaredMarker;\" LANGUAGE js");

    assertThatThrownBy(() -> database.command("sql",
        "DEFINE FUNCTION decl.ending \"" + BODY_ENDING_EARLY + "\" PARAMETERS [a] LANGUAGE js"))
        .isInstanceOf(FunctionExecutionException.class);

    assertThat(database.getSchema().getFunctionLibrary("decl").hasFunction("ending")).isFalse();
    assertThat(database.getSchema().getFunction("decl", "probe").execute()).isEqualTo("undefined");

    // NOT PERSISTED EITHER: THE LIBRARY COMES BACK AS IT WAS
    reopenDatabase();
    assertThat(database.getSchema().getFunctionLibrary("decl").hasFunction("ending")).isFalse();
    assertThat(database.getSchema().getFunction("decl", "probe").execute()).isEqualTo("undefined");
  }

  @Test
  void functionNameMustBeAnIdentifier() {
    for (final String name : new String[] { "z(){} globalThis.declaredMarker = 1; function y", "a b", "a-b", "a.b", "1a", "", " ",
        "a;", "a\n", "if", "class", "null", "́a", "a b", "a b", "a\u0000", "a'b", "a\"b", "a`b", "a/b", "a\\b" })
      assertThatThrownBy(() -> new JavascriptFunctionDefinition(name, "return 1;"))
          .as("function name '%s'", name)
          .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> new JavascriptFunctionDefinition(null, "return 1;")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void parameterNameMustBeAnIdentifier() {
    for (final String parameter : new String[] { "a){} globalThis.declaredMarker = 1; function y(b", "a b", "a,b", "a=1", "...a",
        "{a}", "", "1a", "a/*", "this" })
      assertThatThrownBy(() -> new JavascriptFunctionDefinition("f", "return 1;", "ok", parameter))
          .as("parameter '%s'", parameter)
          .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> new JavascriptFunctionDefinition("f", "return 1;", (String) null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void identifiersOfAnyScriptStillWork() {
    final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "decl");
    library.registerFunction(new JavascriptFunctionDefinition("$dollar", "return $a + _b;", "$a", "_b"));
    library.registerFunction(new JavascriptFunctionDefinition("_under_score9", "return x9;", "x9"));
    library.registerFunction(new JavascriptFunctionDefinition("città", "return perché;", "perché"));
    // A LETTER FOLLOWED BY A COMBINING MARK, IN THE NAME AND IN A PARAMETER
    library.registerFunction(new JavascriptFunctionDefinition("café", "return x́;", "x́"));
    library.registerFunction(new JavascriptFunctionDefinition("joined‍", "return a_‿;", "a_‿"));

    assertThat(library.getFunction("$dollar").execute(1, 2)).isEqualTo(3);
    assertThat(library.getFunction("_under_score9").execute("v")).isEqualTo("v");
    assertThat(library.getFunction("città").execute("ok")).isEqualTo("ok");
    assertThat(library.getFunction("café").execute("ok")).isEqualTo("ok");
    assertThat(library.getFunction("joined‍").execute("ok")).isEqualTo("ok");
  }

  @Test
  void parametersCannotBeChangedOnceTheDefinitionIsBuilt() {
    final String[] given = { "a", "b" };
    final JavascriptFunctionDefinition definition = new JavascriptFunctionDefinition("sum", "return a + b;", given);

    given[0] = "a=1";
    definition.getParameters()[1] = "b=2";

    assertThat(definition.getParameters()).containsExactly("a", "b");

    final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "decl");
    library.registerFunction(definition);
    assertThat(library.getFunction("sum").execute(1, 2)).isEqualTo(3);
    assertThat(library.toJSON().getJSONObject("functions").getJSONObject("sum").getJSONArray("parameters").toListOfStrings())
        .containsExactly("a", "b");
  }

  @Test
  void definitionsOfAnyBodyStillWork() {
    final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "decl");
    library.registerFunction(new JavascriptFunctionDefinition("fact", "return n <= 1 ? 1 : n * fact(n - 1);", "n"));
    library.registerFunction(new JavascriptFunctionDefinition("twiceFact", "return fact(n) * 2;", "n"));
    library.registerFunction(new JavascriptFunctionDefinition("withComment", "return 7; // trailing comment, no new line"));
    library.registerFunction(new JavascriptFunctionDefinition("strict",
        "'use strict'; try { undeclaredStrictVariable = 1; return 'sloppy'; } catch (e) { return 'strict'; }"));
    library.registerFunction(new JavascriptFunctionDefinition("args", "return arguments.length;"));
    library.registerFunction(new JavascriptFunctionDefinition("localFunction", "function inner(v) { return v + 1; } return inner(a);", "a"));
    library.registerFunction(new JavascriptFunctionDefinition("emptyBody", ""));

    assertThat(library.getFunction("fact").execute(5)).isEqualTo(120);
    assertThat(library.getFunction("twiceFact").execute(5)).isEqualTo(240);
    assertThat(library.getFunction("withComment").execute()).isEqualTo(7);
    assertThat(library.getFunction("strict").execute()).isEqualTo("strict");
    assertThat(library.getFunction("args").execute(1, 2, 3)).isEqualTo(3);
    assertThat(library.getFunction("localFunction").execute(1)).isEqualTo(2);
    assertThat(library.getFunction("emptyBody").execute()).isNull();
  }

  @Test
  void aFunctionNamedLikeABuiltInDoesNotBreakTheLibrary() {
    final JavascriptFunctionLibraryDefinition library = new JavascriptFunctionLibraryDefinition(database, "decl");
    library.registerFunction(new JavascriptFunctionDefinition("Function", "return 'mine';"));
    // EVERY FUNCTION IS DECLARED AGAIN WHEN ONE IS ADDED: A LATER ONE MUST NOT BE BUILT BY THE FUNCTION JUST DEFINED
    library.registerFunction(new JavascriptFunctionDefinition("other", "return 'other';"));
    library.registerFunction(new JavascriptFunctionDefinition("another", "return 'another';"));

    assertThat(library.getFunction("other").execute()).isEqualTo("other");
    assertThat(library.getFunction("another").execute()).isEqualTo("another");
    assertThat(library.getFunction("Function").execute()).isEqualTo("mine");
  }

  @Test
  void definitionsThroughSqlStillWork() {
    database.command("sql", "DEFINE FUNCTION decl.sum \"return a + b;\" PARAMETERS [a, b] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION decl.sumTwice \"return sum(a, b) * 2;\" PARAMETERS [a, b] LANGUAGE js");

    assertThat(database.getSchema().getFunction("decl", "sum").execute(1, 2)).isEqualTo(3);
    assertThat(database.getSchema().getFunction("decl", "sumTwice").execute(1, 2)).isEqualTo(6);

    reopenDatabase();

    assertThat(database.getSchema().getFunction("decl", "sumTwice").execute(2, 3)).isEqualTo(10);
  }

  @Test
  void quotedNamesAreCheckedThroughSql() {
    assertThatThrownBy(() -> database.command("sql",
        "DEFINE FUNCTION decl.`z(){} globalThis.declaredMarker = 1; function y` \"return 1;\" LANGUAGE js"))
        .isInstanceOf(CommandSQLParsingException.class);

    assertThatThrownBy(() -> database.command("sql",
        "DEFINE FUNCTION decl.f \"return 1;\" PARAMETERS [`a){} globalThis.declaredMarker = 1; function y(b`] LANGUAGE js"))
        .isInstanceOf(CommandSQLParsingException.class);

    assertThat(database.getSchema().hasFunctionLibrary("decl")).isFalse();
  }

  @Test
  void aStoredDefinitionThatDoesNotCompileIsNotLoaded() {
    final LocalSchema schema = database.getSchema().getEmbedded();

    // THE SCHEMA FILE OF A DATABASE, WITH ONE LIBRARY HOLDING A DEFINITION THAT CANNOT BE DECLARED, AND ONE THAT CAN
    final JSONObject schemaFile = new JSONObject().put("functions", new JSONObject()//
        .put("broken", library("probe", "return typeof declaredMarker;", "ending", BODY_ENDING_EARLY))//
        .put("brokenName", library("z(){} globalThis.declaredMarker = 1; function y", "return 1;", "ok", "return 2;"))//
        .put("good", library("probe", "return typeof declaredMarker;", "sum", "return 1 + 2;")));

    final int failures = schema.restoreSchemaMembersFromJSON(schemaFile, LocalSchema.SchemaMemberSource.SCHEMA_FILE);

    assertThat(failures).isEqualTo(2);
    assertThat(schema.hasFunctionLibrary("broken")).isFalse();
    assertThat(schema.hasFunctionLibrary("brokenName")).isFalse();
    assertThat(schema.hasFunctionLibrary("good")).isTrue();
    assertThat(schema.getFunction("good", "sum").execute()).isEqualTo(3);
    assertThat(schema.getFunction("good", "probe").execute()).isEqualTo("undefined");
  }

  private static JSONObject library(final String firstName, final String firstCode, final String secondName,
      final String secondCode) {
    return new JSONObject().put("language", "js").put("functions", new JSONObject()//
        .put(firstName, new JSONObject().put("code", firstCode).put("parameters", new JSONArray()))//
        .put(secondName, new JSONObject().put("code", secondCode).put("parameters", new JSONArray())));
  }
}
