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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.log.LogManager;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Javascript implementation of a function. To define the function, pass the function name, code and optional parameters in the constructor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavascriptFunctionDefinition implements PolyglotFunctionDefinition {
  /**
   * Words that cannot name a function or a parameter in any script. The words that are reserved in strict mode or in
   * modules only ({@code let}, {@code static}, {@code yield}, {@code await}...) are names in a plain script, so a
   * definition that used one keeps working.
   */
  private static final Set<String> RESERVED_WORDS = Set.of("break", "case", "catch", "class", "const", "continue", "debugger",
      "default", "delete", "do", "else", "enum", "export", "extends", "false", "finally", "for", "function", "if", "import", "in",
      "instanceof", "new", "null", "return", "super", "switch", "this", "throw", "true", "try", "typeof", "var", "void", "while",
      "with");

  /**
   * Reads the language's own {@code Function} constructor from a function literal, never from the global scope: a user
   * function named {@code Function} replaces that global, and the next declaration must not be built by it.
   */
  private static final String FUNCTION_CONSTRUCTOR_SOURCE = "(function() {}).constructor";

  private final String                            functionName;
  private final String                            implementation;
  private final String[]                          parameters;
  private       PolyglotFunctionLibraryDefinition library;

  /**
   * Creates the function with its name, implementation in form of text and optional parameters.
   *
   * @param functionName   Name of the function, a plain identifier
   * @param implementation Implementation code as string, the body of the function
   * @param parameters     optional positional parameter names, plain identifiers
   *
   * @throws IllegalArgumentException if the name or a parameter is not a plain identifier
   */
  public JavascriptFunctionDefinition(final String functionName, final String implementation, final String... parameters) {
    if (!isPlainIdentifier(functionName))
      throw new IllegalArgumentException(
          "The name of a function must be a plain identifier: letters, digits, '$' and '_', not starting with a digit");

    // A COPY, SO WHAT WAS CHECKED HERE IS WHAT IS DECLARED
    this.parameters = parameters != null ? parameters.clone() : new String[0];
    for (int i = 0; i < this.parameters.length; i++)
      if (!isPlainIdentifier(this.parameters[i]))
        throw new IllegalArgumentException("The parameter " + (i + 1) + " of function '" + functionName
            + "' must be a plain identifier: letters, digits, '$' and '_', not starting with a digit");

    this.functionName = functionName;
    this.implementation = implementation != null ? implementation : "";
  }

  /**
   * Tells if {@code name} can be used as it is for the name of a function or of a parameter: a non empty sequence of
   * the characters the language allows in an identifier (letters, {@code $} and {@code _} to start with, then also
   * combining marks, digits and connector punctuation) that is not a reserved word. No other character is accepted, so
   * a name never holds an operator, a quote, a bracket, a space or a line break.
   */
  private static boolean isPlainIdentifier(final String name) {
    if (name == null || name.isEmpty() || RESERVED_WORDS.contains(name))
      return false;

    final int length = name.length();
    for (int i = 0; i < length; ) {
      final int codePoint = name.codePointAt(i);
      if (!(isIdentifierStart(codePoint) || (i > 0 && isIdentifierContinuation(codePoint))))
        return false;
      i += Character.charCount(codePoint);
    }
    return true;
  }

  private static boolean isIdentifierStart(final int codePoint) {
    if (codePoint == '$' || codePoint == '_')
      return true;

    return switch (Character.getType(codePoint)) {
      case Character.UPPERCASE_LETTER, Character.LOWERCASE_LETTER, Character.TITLECASE_LETTER, Character.MODIFIER_LETTER,
           Character.OTHER_LETTER, Character.LETTER_NUMBER -> true;
      default -> false;
    };
  }

  private static boolean isIdentifierContinuation(final int codePoint) {
    // ZERO WIDTH NON-JOINER AND ZERO WIDTH JOINER
    if (codePoint == 0x200C || codePoint == 0x200D)
      return true;

    return switch (Character.getType(codePoint)) {
      case Character.NON_SPACING_MARK, Character.COMBINING_SPACING_MARK, Character.DECIMAL_DIGIT_NUMBER,
           Character.CONNECTOR_PUNCTUATION -> true;
      default -> false;
    };
  }

  /**
   * Declares the function in the engine of the library. The body is compiled on its own, as the body of a new function
   * made by the language's {@code Function} constructor, and bound under the name of the function: no text is built from
   * the definition, so declaring a function only declares it and nothing of the definition runs until it is called.
   */
  @Override
  public void init(final PolyglotFunctionLibraryDefinition library) {
    this.library = library;
    library.execute(polyglotEngine -> {
      try {
        final Value functionConstructor = polyglotEngine.eval(FUNCTION_CONSTRUCTOR_SOURCE);

        // THE PARAMETER NAMES, THEN THE BODY
        final Object[] constructorArguments = new Object[parameters.length + 1];
        System.arraycopy(parameters, 0, constructorArguments, 0, parameters.length);
        constructorArguments[parameters.length] = implementation;

        // DECLARE THE FUNCTION
        polyglotEngine.context.getBindings("js").putMember(functionName, functionConstructor.newInstance(constructorArguments));
        return null;
      } catch (final Exception e) {
        final String detail = e.getMessage() != null ? e.getMessage() : e.toString();
        throw new FunctionExecutionException("Error on definition of function '" + functionName + "': " + detail, e);
      }
    });
  }

  @Override
  public String getName() {
    return functionName;
  }

  @Override
  public String getImplementation() {
    return implementation;
  }

  @Override
  public String[] getParameters() {
    // A COPY, SO THE NAMES THAT WERE CHECKED CANNOT BE CHANGED THROUGH THE ARRAY OF A CALLER
    return parameters.clone();
  }

  @Override
  public Object execute(final Object... parameters) {
    return library.execute(polyglotEngine -> {
      try {
        final Value fn = polyglotEngine.context.getBindings("js").getMember(functionName);
        if (fn == null)
          throw new FunctionExecutionException("Function '" + functionName + "' is not defined");
        final Object[] args = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++)
          args[i] = toJsArg(parameters[i]);
        return jsValueToJava(fn.execute(args));
      } catch (final FunctionExecutionException e) {
        throw e;
      } catch (final Exception e) {
        throw new FunctionExecutionException("Error on execution of function '" + functionName + "'", e);
      }
    });
  }

  private static Object toJsArg(final Object value) {
    if (value instanceof Map<?, ?> map)
      return toDeepProxyObject(normalizeMapKeys(map));
    if (value instanceof List<?> list)
      return toDeepProxyList(normalizeListValues(list));
    return value;
  }

  private static Map<String, Object> normalizeMapKeys(final Map<?, ?> map) {
    final Map<String, Object> result = new LinkedHashMap<>(map.size());
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final String key = entry.getKey() instanceof String s ? s : String.valueOf(entry.getKey());
      result.put(key, normalizeValue(entry.getValue()));
    }
    return result;
  }

  private static List<Object> normalizeListValues(final List<?> list) {
    final List<Object> result = new ArrayList<>(list.size());
    for (final Object item : list)
      result.add(normalizeValue(item));
    return result;
  }

  private static Object normalizeValue(final Object value) {
    if (value instanceof Map<?, ?> map)
      return normalizeMapKeys(map);
    if (value instanceof List<?> list)
      return normalizeListValues(list);
    return value;
  }

  public static List<?> jsArrayToJava(final ProxyArray array) {
    if (array == null)
      return null;

    final List<Object> list = new ArrayList<>();
    for (int i = 0; i < array.getSize(); ++i)
      list.add(jsAnyToJava(array.get(i)));
    return list;
  }

  public static Object jsObjectToJava(final ProxyObject result) {
    if (result == null)
      return null;

    final Map<String, Object> map = new HashMap<>();

    final Object keys = result.getMemberKeys();

    final Iterable<String> iterableKeys;
    if (keys instanceof ProxyArray proxyArray) {
      List<String> list = new ArrayList<>();
      for (int i = 0; i < proxyArray.getSize(); ++i) {
        final Object key = proxyArray.get(i);
        if (key instanceof String s)
          list.add(s);
      }
      iterableKeys = list;
    } else
      iterableKeys = (Iterable<String>) keys;

    for (final String key : iterableKeys) {
      final Object value = jsAnyToJava(result.getMember(key));
      if (value != null)
        map.put(key, value);
    }

    return map;
  }

  public static Object jsAnyToJava(final Object value) {
    switch (value) {
    case null -> {
      return null;
    }
    case ProxyObject proxyObject -> {
      return jsObjectToJava(proxyObject);
    }
    case ProxyArray proxyArray -> {
      return jsArrayToJava(proxyArray);
    }
    case Value result -> {
      return jsValueToJava(result);
    }
    case Function<?, ?> fx -> {
      // NOT SUPPORTED
      LogManager.instance().log(JavascriptFunctionDefinition.class, Level.WARNING,
          "Conversion of a js function '%s' is not supported, it will be ignored", value);
      return null;
    }
    case List list -> {
      final List newList = new ArrayList<>();

      for (int i = 0; i < list.size(); ++i) {
        Object elem = list.get(i);
        if (elem instanceof Function<?, ?>) {
          // NOT SUPPORTED
          LogManager.instance()
              .log(JavascriptFunctionDefinition.class, Level.WARNING, "Skip function member %d in list '%s' (class=%s)", i, list,
                  list.getClass().getName());
        } else
          newList.add(jsAnyToJava(elem));
      }
      return newList;
    }
    case Map<?, ?> map -> {
      final Map<String, Object> newMap = new HashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        final Object key = entry.getKey();
        Object valueEntry = entry.getValue();
        if (key instanceof String keyStr) {
          if (valueEntry instanceof Function<?, ?>) {
            // NOT SUPPORTED
            LogManager.instance()
                .log(JavascriptFunctionDefinition.class, Level.WARNING, "Skip function member '%s' in map '%s' (class=%s)", keyStr,
                    map, map.getClass().getName());
          } else
            valueEntry = jsAnyToJava(valueEntry);

          if (valueEntry != null)
            newMap.put(keyStr, valueEntry);
        }
      }
      return newMap;
    }
    default -> {
      return value;
    }
    }
  }

  public static Object jsValueToJava(final Value result) {
    if (result == null)
      return null;
    else if (result.isHostObject()) {
      return jsAnyToJava(result.asHostObject());
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
        final Object elem = jsValueToJava(result.getArrayElement(i));
        array.add(elem);
      }
      return array;
    } else if (result.isNull())
      return null;
    else if (result.hasMembers()) {
      // GraalVM exposes Java Map/List interface methods (get, put, remove, clear, entrySet, …)
      // as JS members of a Value that wraps a host collection but for which isHostObject()
      // returns false — typically after JS code has mutated the collection (e.g.
      // `m.id = 'x'; delete m.notes`) or after the collection passes through a JS Array.
      // Without this guard, getMemberKeys() returns BOTH the data keys AND every Map method
      // name; downstream serialization writes them as garbage properties on the parent record,
      // surfacing later as "Skipping corrupted property 'remove'/'get'/'values'/…" warnings
      // from BinarySerializer. Try to peel off the underlying Java collection identity first;
      // if it is a Map or List, route through jsAnyToJava which iterates entrySet() / size()
      // instead of member keys.
      try {
        final Object hosted = result.as(Object.class);
        if (hosted instanceof Map<?, ?> || hosted instanceof List<?>)
          return jsAnyToJava(hosted);
      } catch (final Exception ignored) {
        // Not a hosted Java collection — fall through to the JS-object member enumeration below.
      }

      final Map<String, Object> map = new HashMap<>();
      final Set<String> keys = result.getMemberKeys();
      for (final String key : keys) {
        final Object elem = jsValueToJava(result.getMember(key));
        map.put(key, elem);
      }
      return map;
    } else
      // UNKNOWN OR NOT SUPPORTED
      return null;
  }

  /**
   * Recursively converts a Java Map into a ProxyObject suitable for JS.
   * It handles nested Maps and Lists.
   *
   * @param map The Java Map to convert.
   *
   * @return A ProxyObject representing the deep structure.
   */
  public static ProxyObject toDeepProxyObject(final Map<String, Object> map) {
    // Use a new map to avoid modifying the original
    final Map<String, Object> processedMap = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      final Object value = entry.getValue();
      if (value instanceof Map subMap) {
        // If the value is a map, recurse
        processedMap.put(entry.getKey(), toDeepProxyObject(subMap));
      } else if (value instanceof List list)
        // If the value is a list, process its elements
        processedMap.put(entry.getKey(), toDeepProxyList(list));
      else
        // Otherwise, just put the primitive/simple value
        processedMap.put(entry.getKey(), value);
    }
    return ProxyObject.fromMap(processedMap);
  }

  /**
   * Helper method to recursively convert elements within a List.
   */
  public static ProxyArray toDeepProxyList(final List<?> list) {
    return ProxyArray.fromList(list.stream().map(item -> {
      if (item instanceof Map subMap)
        return toDeepProxyObject(subMap);
      else if (item instanceof List)
        return toDeepProxyList((List<?>) item);
      return item;
    }).collect(Collectors.toList()));
  }
}
