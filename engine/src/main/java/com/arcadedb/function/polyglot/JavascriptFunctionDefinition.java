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
import com.arcadedb.log.LogManager;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;

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

        return jsValueToJava(result);

      } catch (final IOException e) {
        throw new FunctionExecutionException("Error on definition of function '" + functionName + "'");
      }
    });
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
      for (int i = 0; i < list.size(); ++i) {
        Object elem = list.get(i);
        if (elem instanceof Function<?, ?>) {
          // NOT SUPPORTED
          LogManager.instance()
              .log(JavascriptFunctionDefinition.class, Level.WARNING, "Skip function member %d in list '%s' (class=%s)", i, list,
                  list.getClass().getName());
        } else
          list.set(i, jsAnyToJava(elem));
      }
      return list;
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
