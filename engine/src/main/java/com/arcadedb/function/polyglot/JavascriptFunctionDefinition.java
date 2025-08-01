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
import java.util.*;

/**
 * Javascript implementation of a function. To define the function, pass the function name, code and optional parameters in the constructor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavascriptFunctionDefinition implements PolyglotFunctionDefinition {
  private final String                            functionName;
  private final String                            implementation;
  private final String[]                          parameters;
  private       PolyglotFunctionLibraryDefinition library;

  /**
   * Creates the function with its name, implementation in form of text and optional parameters.
   *
   * @param functionName   Name of the function
   * @param implementation Implementation code as string
   * @param parameters     optional positional parameter names
   */
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
      } catch (final Exception e) {
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

        return getValue(result);

      } catch (final IOException e) {
        throw new FunctionExecutionException("Error on definition of function '" + functionName + "'");
      }
    });
  }

  public static Object getValue(final Value result) {
    if (result == null)
      return null;
    else if (result.isHostObject()) {
      Object v = result.asHostObject();
      if (v instanceof Value value)
        v = getValue(value);
      else if (v instanceof List) {
        for (int i = 0; i < ((List<?>) v).size(); ++i) {
          Object elem = ((List) v).get(i);
          if (elem instanceof Value value)
            ((List) v).set(i, getValue(value));
          else if (elem instanceof Map map && !(elem instanceof HashMap))
            ((List) v).set(i, new HashMap(map));
        }
      }
      return v;
    } else if (result.isString())
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
        return result.asDouble();
    } else if (result.hasArrayElements()) {
      final long size = result.getArraySize();
      final List<Object> array = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        final Object elem = getValue(result.getArrayElement(i));
        array.add(elem);
      }
      return array;
    } else if (result.isNull())
      return null;
    else if (result.hasMembers()) {
      final Map<String, Object> map = new HashMap<>();
      final Set<String> keys = result.getMemberKeys();
      for (final String key : keys) {
        final Object elem = getValue(result.getMember(key));
        map.put(key, elem);
      }
      return map;
    } else
      // UNKNOWN OR NOT SUPPORTED
      return null;
  }
}
