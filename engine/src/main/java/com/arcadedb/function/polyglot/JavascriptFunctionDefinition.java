package com.arcadedb.function.polyglot;/*
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

import com.arcadedb.function.FunctionExecutionException;
import org.graalvm.polyglot.Value;

import java.io.*;

public class JavascriptFunctionDefinition implements PolyglotFunctionDefinition {
  private final String                            functionName;
  private final String                            implementation;
  private final String[]                          parameters;
  private       PolyglotFunctionLibraryDefinition library;

  public JavascriptFunctionDefinition(final String functionName, final String implementation, final String... parameters) {
    this.functionName = functionName;
    this.implementation = implementation;
    this.parameters = parameters;
  }

  @Override
  public void init(final PolyglotFunctionLibraryDefinition library) {
    this.library = library;
    library.execute((polyglotEngine) -> {
      try {
        // DECLARE THE FUNCTION
        String declaration = "function " + functionName + "( ";
        for (int i = 0; i < parameters.length; i++) {
          if (i > 0)
            declaration += ", ";
          declaration += parameters[i];
        }
        declaration += " ) { ";
        declaration += implementation;
        declaration += " }";

        return polyglotEngine.eval(declaration);
      } catch (IOException e) {
        throw new FunctionExecutionException("Error on definition of function '" + functionName + "'");
      }
    });
  }

  @Override
  public String getName() {
    return functionName;
  }

  @Override
  public Object execute(final Object... parameters) {
    return library.execute((polyglotEngine) -> {
      try {
        String declaration = functionName + "( ";
        for (int i = 0; i < parameters.length; i++) {
          if (i > 0)
            declaration += ", ";

          final boolean isString = parameters[i] instanceof String;
          if (isString)
            declaration += "'";

          declaration += parameters[i];

          if (isString)
            declaration += "'";
        }
        declaration += ")";

        final Value result = polyglotEngine.eval(declaration);

        if (result.isHostObject())
          return result.asHostObject();
        else if (result.isString())
          return result.asString();
        else if (result.isBoolean())
          return result.asBoolean();
        else if (result.isNumber()) {
          if (result.fitsInInt())
            return result.asInt();
          else if (result.fitsInLong())
            return result.asLong();
          else if (result.fitsInFloat())
            return result.asFloat();
          else
            return result.asFloat();
        } else if (result.isNull())
          return null;

        // UNKNOWN OR NOT SUPPORTED
        return null;

      } catch (IOException e) {
        throw new FunctionExecutionException("Error on definition of function '" + functionName + "'");
      }
    });
  }
}
