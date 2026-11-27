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

import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;

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
        final ArgumentCopy argumentCopy = new ArgumentCopy();
        final Object[] args = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++)
          args[i] = argumentCopy.toJs(parameters[i]);
        return jsValueToJava(fn.execute(args), new ResultCopy(library, functionName));
      } catch (final FunctionExecutionException | HeapLimitExceededException e) {
        throw e;
      } catch (final StackOverflowError e) {
        // A VALUE NESTED TOO DEEPLY, OR THAT HOLDS ITSELF, IS COPIED BY RECURSION: THE STACK IS UNWOUND TO THIS FRAME
        throw new FunctionExecutionException("Error on execution of function '" + functionName + "': a value is nested too deeply", e);
      } catch (final Exception e) {
        throw new FunctionExecutionException("Error on execution of function '" + functionName + "'", e);
      }
    });
  }

  public static List<?> jsArrayToJava(final ProxyArray array) {
    return jsArrayToJava(array, ResultCopy.UNBOUNDED);
  }

  private static List<?> jsArrayToJava(final ProxyArray array, final ResultCopy copy) {
    if (array == null)
      return null;

    final long size = array.getSize();
    copy.add(size);

    final List<Object> list = new ArrayList<>();
    for (long i = 0; i < size; ++i)
      list.add(jsAnyToJava(array.get(i), copy));
    return list;
  }

  public static Object jsObjectToJava(final ProxyObject result) {
    return jsObjectToJava(result, ResultCopy.UNBOUNDED);
  }

  private static Object jsObjectToJava(final ProxyObject result, final ResultCopy copy) {
    if (result == null)
      return null;

    final Map<String, Object> map = new HashMap<>();

    final Object keys = result.getMemberKeys();

    final Iterable<String> iterableKeys;
    if (keys instanceof ProxyArray proxyArray) {
      final long size = proxyArray.getSize();
      copy.add(size);
      final List<String> list = new ArrayList<>();
      for (long i = 0; i < size; ++i) {
        final Object key = proxyArray.get(i);
        if (key instanceof String s)
          list.add(s);
      }
      iterableKeys = list;
    } else
      iterableKeys = (Iterable<String>) keys;

    for (final String key : iterableKeys) {
      final Object value = jsAnyToJava(result.getMember(key), copy);
      if (value != null)
        map.put(key, value);
    }

    return map;
  }

  public static Object jsAnyToJava(final Object value) {
    return jsAnyToJava(value, ResultCopy.UNBOUNDED);
  }

  private static Object jsAnyToJava(final Object value, final ResultCopy copy) {
    switch (value) {
    case null -> {
      return null;
    }
    case ProxyObject proxyObject -> {
      return jsObjectToJava(proxyObject, copy);
    }
    case ProxyArray proxyArray -> {
      return jsArrayToJava(proxyArray, copy);
    }
    case Value result -> {
      return jsValueToJava(result, copy);
    }
    case Function<?, ?> fx -> {
      // NOT SUPPORTED
      LogManager.instance().log(JavascriptFunctionDefinition.class, Level.WARNING,
          "Conversion of a js function '%s' is not supported, it will be ignored", value);
      return null;
    }
    case List list -> {
      copy.add(list.size());
      final List newList = new ArrayList<>();

      for (int i = 0; i < list.size(); ++i) {
        Object elem = list.get(i);
        if (elem instanceof Function<?, ?>) {
          // NOT SUPPORTED
          LogManager.instance()
              .log(JavascriptFunctionDefinition.class, Level.WARNING, "Skip function member %d in list '%s' (class=%s)", i, list,
                  list.getClass().getName());
        } else
          newList.add(jsAnyToJava(elem, copy));
      }
      return newList;
    }
    case Map<?, ?> map -> {
      copy.add(map.size());
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
            valueEntry = jsAnyToJava(valueEntry, copy);

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
    return jsValueToJava(result, ResultCopy.UNBOUNDED);
  }

  private static Object jsValueToJava(final Value result, final ResultCopy copy) {
    if (result == null)
      return null;
    else if (result.isHostObject()) {
      return jsAnyToJava(result.asHostObject(), copy);
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
      // THE LENGTH OF A SCRIPT ARRAY IS ITS HIGHEST INDEX PLUS ONE, NOT THE NUMBER OF ELEMENTS IT HOLDS: IT IS COUNTED BEFORE THE
      // ELEMENTS ARE READ
      final long size = result.getArraySize();
      copy.add(size);
      final List<Object> array = new ArrayList<>();
      for (long i = 0; i < size; i++) {
        final Object elem = jsValueToJava(result.getArrayElement(i), copy);
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
          return jsAnyToJava(hosted, copy);
      } catch (final HeapLimitExceededException e) {
        throw e;
      } catch (final Exception ignored) {
        // Not a hosted Java collection — fall through to the JS-object member enumeration below.
      }

      final Map<String, Object> map = new HashMap<>();
      final Set<String> keys = result.getMemberKeys();
      copy.add(keys.size());
      for (final String key : keys) {
        final Object elem = jsValueToJava(result.getMember(key), copy);
        map.put(key, elem);
      }
      return map;
    } else
      // UNKNOWN OR NOT SUPPORTED
      return null;
  }

  /**
   * Recursively converts a Java Map into a ProxyObject suitable for JS.
   * It handles nested Maps and Lists. A Map or List that is referred to more than once is converted once.
   *
   * @param map The Java Map to convert.
   *
   * @return A ProxyObject representing the deep structure.
   */
  public static ProxyObject toDeepProxyObject(final Map<String, Object> map) {
    return (ProxyObject) new ArgumentCopy().toJs(map);
  }

  /**
   * Helper method to recursively convert elements within a List. A Map or List that is referred to more than once is converted
   * once.
   */
  public static ProxyArray toDeepProxyList(final List<?> list) {
    return (ProxyArray) new ArgumentCopy().toJs(list);
  }

  /**
   * Copies the lists and maps handed to a function into the proxies the language reads, with the keys of a map as strings. A list
   * or map that is referred to more than once, as a value that is built from shared parts is, is copied once and every reference
   * reads the same proxy: the copy holds as many lists and maps as the arguments do, not as many as there are paths to them. A
   * collection that holds itself is copied the same way.
   * <p>
   * The proxies are copies, so the function cannot change the values it was given. One instance serves one call.
   */
  private static final class ArgumentCopy {
    // THE LIST OR MAP THAT WAS GIVEN -> ITS PROXY, BY IDENTITY. CREATED WHEN THE FIRST LIST OR MAP IS MET
    private IdentityHashMap<Object, Object> proxies;

    Object toJs(final Object value) {
      if (!(value instanceof Map) && !(value instanceof List))
        return value;

      if (proxies == null)
        proxies = new IdentityHashMap<>(8);
      else {
        final Object known = proxies.get(value);
        if (known != null)
          return known;
      }

      if (value instanceof Map<?, ?> map) {
        final Map<String, Object> copy = LinkedHashMap.newLinkedHashMap(map.size());
        final ProxyObject proxy = ProxyObject.fromMap(copy);
        // REGISTERED BEFORE ITS VALUES ARE COPIED: A VALUE THAT LEADS BACK TO THIS MAP FINDS IT
        proxies.put(value, proxy);
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
          final Object key = entry.getKey();
          copy.put(key instanceof String s ? s : String.valueOf(key), toJs(entry.getValue()));
        }
        return proxy;
      }

      final List<?> list = (List<?>) value;
      final List<Object> copy = new ArrayList<>(list.size());
      final ProxyArray proxy = ProxyArray.fromList(copy);
      proxies.put(value, proxy);
      for (final Object item : list)
        copy.add(toJs(item));
      return proxy;
    }
  }

  /**
   * The elements that the conversion of what a function answered has copied so far, held to the cap of elements that one operation
   * of a query keeps in the heap, as the rows of a function written in SQL are. The size of an array or of an object is counted
   * before its elements are read, so an array whose length is far beyond what it holds is refused at once, and a value that
   * leads to the same array by several paths is counted at every path. One instance serves one conversion.
   */
  private static final class ResultCopy {
    // FOR A VALUE THAT IS NOT THE ANSWER OF A FUNCTION OF A DATABASE
    static final ResultCopy UNBOUNDED = new ResultCopy(null, null);

    private final PolyglotFunctionLibraryDefinition<?> library;
    private final String                               functionName;
    private       OperationHeapLimit                   limit;
    private       long                                 copied;

    ResultCopy(final PolyglotFunctionLibraryDefinition<?> library, final String functionName) {
      this.library = library;
      this.functionName = functionName;
    }

    void add(final long elements) {
      if (library == null)
        return;

      if (limit == null)
        limit = OperationHeapLimit.of(library.database, "JavaScript function '" + library.getName() + "." + functionName + "'");
      copied += elements;
      limit.check(copied);
    }
  }
}
