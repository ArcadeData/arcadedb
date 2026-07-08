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

  public static FunctionLibraryDefinition createLibrary(final Database database, final String libraryName, final String language) {
    return switch (language) {
      case "js" -> new JavascriptFunctionLibraryDefinition(database, libraryName);
      case "sql" -> new SQLFunctionLibraryDefinition(database, libraryName);
      case "opencypher", "cypher" -> new CypherFunctionLibraryDefinition(database, libraryName);
      default -> throw new IllegalArgumentException("Error on function creation: language '" + language + "' not supported");
    };
  }

  public static FunctionDefinition createFunction(final Database database, final String language, final String functionName,
      final String code, final String[] parameters) {
    return switch (language) {
      case "js" -> new JavascriptFunctionDefinition(functionName, code, parameters);
      case "sql" -> new SQLFunctionDefinition(database, functionName, code, parameters);
      case "opencypher", "cypher" -> new CypherFunctionDefinition(database, functionName, code, parameters);
      default -> throw new IllegalArgumentException("Error on function creation: language '" + language + "' not supported");
    };
  }
}
