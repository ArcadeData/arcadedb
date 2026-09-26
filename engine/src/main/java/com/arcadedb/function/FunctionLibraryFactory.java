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
package com.arcadedb.function;

import com.arcadedb.database.Database;
import com.arcadedb.function.cypher.CypherFunctionDefinition;
import com.arcadedb.function.cypher.CypherFunctionLibraryDefinition;
import com.arcadedb.function.polyglot.JavascriptFunctionDefinition;
import com.arcadedb.function.polyglot.JavascriptFunctionLibraryDefinition;
import com.arcadedb.function.sql.SQLFunctionDefinition;
import com.arcadedb.function.sql.SQLFunctionLibraryDefinition;

import java.util.Locale;

/**
 * Central factory that maps a language identifier ("js", "sql", "opencypher"/"cypher") to the concrete
 * {@link FunctionLibraryDefinition} / {@link FunctionDefinition} implementation. It is shared by the {@code DEFINE
 * FUNCTION} statement and by the schema loader that restores persisted function libraries after a restart (issue #5121),
 * so the language-to-class mapping lives in a single place.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FunctionLibraryFactory {
  private FunctionLibraryFactory() {
  }

  /**
   * Returns the canonical name of a function language, the one {@link FunctionLibraryDefinition#getLanguage()} reports
   * ({@code cypher} is an alias of {@code opencypher}), or {@code null} if the language is not supported.
   */
  public static String canonicalLanguage(final String language) {
    if (language == null)
      return null;
    // CASE-INSENSITIVE, AS EVERY OTHER KEYWORD OF THE STATEMENT: "LANGUAGE JS" USED TO BE REPORTED AS NOT SUPPORTED
    final String lower = language.toLowerCase(Locale.ENGLISH);
    return switch (lower) {
      case "js", "sql", "opencypher" -> lower;
      case "cypher" -> "opencypher";
      default -> null;
    };
  }

  /**
   * Refuses to add a function written in {@code language} to an existing library written in another one (issue
   * #8423). A library executes, persists and restores every function it holds as its own language, so a mismatch can
   * never work: it used to surface as a raw {@link ClassCastException} from the library instead of a message naming
   * both languages. A library with no language is one registered programmatically (e.g. a Java class library), which
   * {@code DEFINE FUNCTION} cannot add to either.
   *
   * @throws IllegalArgumentException if the library does not accept functions written in {@code language}
   */
  public static void checkLibraryLanguage(final FunctionLibraryDefinition library, final String language) {
    final String libraryLanguage = library.getLanguage();
    if (libraryLanguage == null)
      throw new IllegalArgumentException(
          "Cannot define a function in library '" + library.getName() + "': it is not a user-defined function library");

    final String canonical = languageOrFail(language);
    if (!libraryLanguage.equals(canonical))
      throw new IllegalArgumentException(
          "Cannot define a '" + language + "' function in library '" + library.getName() + "': it is a '" + libraryLanguage
              + "' library, and a library holds functions of one language only");
  }

  private static String languageOrFail(final String language) {
    final String canonical = canonicalLanguage(language);
    if (canonical == null)
      throw new IllegalArgumentException("Error on function creation: language '" + language + "' not supported");
    return canonical;
  }

  public static FunctionLibraryDefinition createLibrary(final Database database, final String libraryName, final String language) {
    return switch (languageOrFail(language)) {
      case "js" -> new JavascriptFunctionLibraryDefinition(database, libraryName);
      case "sql" -> new SQLFunctionLibraryDefinition(database, libraryName);
      case "opencypher" -> new CypherFunctionLibraryDefinition(database, libraryName);
      default -> throw new IllegalStateException("Unreachable language '" + language + "'");
    };
  }

  public static FunctionDefinition createFunction(final Database database, final String language, final String functionName,
      final String code, final String[] parameters) {
    return switch (languageOrFail(language)) {
      case "js" -> new JavascriptFunctionDefinition(functionName, code, parameters);
      case "sql" -> new SQLFunctionDefinition(database, functionName, code, parameters);
      case "opencypher" -> new CypherFunctionDefinition(database, functionName, code, parameters);
      default -> throw new IllegalStateException("Unreachable language '" + language + "'");
    };
  }
}
