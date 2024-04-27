/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.opencypher.gremlin.translation.Tokens;
import org.opencypher.gremlin.translation.exception.ConstraintException;
import org.opencypher.gremlin.translation.exception.CypherExceptions;
import org.opencypher.gremlin.translation.exception.TypeException;

import java.util.ArrayList;
import java.util.*;
import java.util.function.*;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.*;

/**
 * Override Open-Cypher-Gremlin's default CustomFunctions to manage NPE with parameters.
 * <p>
 * When the Gremlin team fixes the issue we can just remove this class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings({ "unchecked", "WeakerAccess", "ArraysAsListWithZeroOrOneArgument" })
public final class ArcadeCustomFunctions {
  private ArcadeCustomFunctions() {
  }

  public static Function<Traverser, Object> cypherToString() {
    return traverser -> {
      Object arg = tokenToNull(traverser.get());
      boolean valid = arg == null ||
          arg instanceof Boolean ||
          arg instanceof Number ||
          arg instanceof String;
      if (!valid) {
        String className = arg.getClass().getName();
        throw new TypeException("Cannot convert " + className + " to string");
      }

      return Optional.ofNullable(arg)
          .map(String::valueOf)
          .orElse(Tokens.NULL);
    };
  }

  public static Function<Traverser, Object> cypherToBoolean() {
    return traverser -> {
      Object arg = tokenToNull(traverser.get());
      boolean valid = arg == null ||
          arg instanceof Boolean ||
          arg instanceof String;
      if (!valid) {
        String className = arg.getClass().getName();
        throw new TypeException("Cannot convert " + className + " to boolean");
      }

      return Optional.ofNullable(arg)
          .map(String::valueOf)
          .map(v -> {
            switch (v.toLowerCase()) {
            case "true":
              return true;
            case "false":
              return false;
            default:
              return Tokens.NULL;
            }
          })
          .orElse(Tokens.NULL);
    };
  }

  public static Function<Traverser, Object> cypherToInteger() {
    return traverser -> {
      Object arg = tokenToNull(traverser.get());
      boolean valid = arg == null ||
          arg instanceof Number ||
          arg instanceof String;
      if (!valid) {
        String className = arg.getClass().getName();
        throw new TypeException("Cannot convert " + className + " to integer");
      }

      return nullToToken(
          Optional.ofNullable(arg)
              .map(String::valueOf)
              .map(v -> {
                try {
                  return Long.valueOf(v);
                } catch (NumberFormatException e1) {
                  try {
                    return Double.valueOf(v).longValue();
                  } catch (NumberFormatException e2) {
                    return null;
                  }
                }
              })
              .orElse(null));
    };
  }

  public static Function<Traverser, Object> cypherToFloat() {
    return traverser -> {
      Object arg = tokenToNull(traverser.get());
      boolean valid = arg == null ||
          arg instanceof Number ||
          arg instanceof String;
      if (!valid) {
        String className = arg.getClass().getName();
        throw new TypeException("Cannot convert " + className + " to float");
      }

      return nullToToken(
          Optional.ofNullable(arg)
              .map(String::valueOf)
              .map(v -> {
                try {
                  return Double.valueOf(v);
                } catch (NumberFormatException e) {
                  return null;
                }
              })
              .orElse(null));
    };
  }

  public static Function<Traverser, Object> cypherRound() {
    return cypherFunction(a -> (Math.round((Double) a.getFirst())), Double.class);
  }

  public static Function<Traverser, Object> cypherProperties() {
    return traverser -> {
      Object argument = traverser.get();

      if (argument == Tokens.NULL) {
        return Tokens.NULL;
      }

      if (argument instanceof Map) {
        return argument;
      }
      Iterator<? extends Property<Object>> it = ((Element) argument).properties();
      Map<Object, Object> propertyMap = new HashMap<>();
      while (it.hasNext()) {
        Property<Object> property = it.next();
        propertyMap.putIfAbsent(property.key(), property.value());
      }
      return propertyMap;
    };
  }

  public static Function<Traverser, Object> cypherContainerIndex() {
    return traverser -> {
      List<?> args = (List<?>) traverser.get();
      Object container = args.getFirst();
      Object index = args.get(1);

      if (container == Tokens.NULL || index == Tokens.NULL) {
        return Tokens.NULL;
      }

      if (container instanceof List list) {
        int size = list.size();
        int i = normalizeContainerIndex(index, size);
        if (i < 0 || i > size) {
          return Tokens.NULL;
        }
        return list.get(i);
      }

      if (container instanceof Map map) {
        if (!(index instanceof String)) {
          String indexClass = index.getClass().getName();
          throw new IllegalArgumentException("Map element access by non-string: " + indexClass);
        }
        String key = (String) index;
        return map.getOrDefault(key, Tokens.NULL);
      }

      if (container instanceof Element element) {
        if (!(index instanceof String)) {
          String indexClass = index.getClass().getName();
          throw new IllegalArgumentException("Property access by non-string: " + indexClass);
        }
        String key = (String) index;
        return element.property(key).orElse(Tokens.NULL);
      }

      String containerClass = container.getClass().getName();
      if (index instanceof String) {
        throw new IllegalArgumentException("Invalid property access of " + containerClass);
      }
      throw new IllegalArgumentException("Invalid element access of " + containerClass);
    };
  }

  public static Function<Traverser, Object> cypherListSlice() {
    return traverser -> {
      List<?> args = (List<?>) traverser.get();
      Object container = args.getFirst();
      Object from = args.get(1);
      Object to = args.get(2);

      if (container == Tokens.NULL || from == Tokens.NULL || to == Tokens.NULL) {
        return Tokens.NULL;
      }

      if (container instanceof List list) {
        int size = list.size();
        int f = normalizeRangeIndex(from, size);
        int t = normalizeRangeIndex(to, size);
        if (f >= t) {
          return new ArrayList<>();
        }
        return new ArrayList<>(list.subList(f, t));
      }

      String containerClass = container.getClass().getName();
      throw new IllegalArgumentException(
          "Invalid element access of " + containerClass + " by range"
      );
    };
  }

  private static int normalizeContainerIndex(Object index, int containerSize) {
    if (!(index instanceof Number)) {
      String indexClass = index.getClass().getName();
      throw new IllegalArgumentException("List element access by non-integer: " + indexClass);
    }
    int i = ((Number) index).intValue();
    return (i >= 0) ? i : containerSize + i;
  }

  private static int normalizeRangeIndex(Object index, int size) {
    int i = normalizeContainerIndex(index, size);
    if (i < 0) {
      return 0;
    }
    if (i > size) {
      return size;
    }
    return i;
  }

  public static Function<Traverser, Object> cypherPercentileCont() {
    return percentileFunction(
        (data, percentile) -> {
          int last = data.size() - 1;
          double lowPercentile = Math.floor(percentile * last) / last;
          double highPercentile = Math.ceil(percentile * last) / last;
          if (lowPercentile == highPercentile) {
            return percentileNearest(data, percentile);
          }

          double scale = (percentile - lowPercentile) / (highPercentile - lowPercentile);
          double low = percentileNearest(data, lowPercentile).doubleValue();
          double high = percentileNearest(data, highPercentile).doubleValue();
          return (high - low) * scale + low;
        }
    );
  }

  public static Function<Traverser, Object> cypherPercentileDisc() {
    return percentileFunction(
        ArcadeCustomFunctions::percentileNearest
    );
  }

  private static Function<Traverser, Object> percentileFunction(BiFunction<List<Number>, Double, Number> percentileStrategy) {
    return traverser -> {
      List<?> args = (List<?>) traverser.get();

      double percentile = ((Number) args.get(1)).doubleValue();
      if (percentile < 0 || percentile > 1) {
        throw new IllegalArgumentException("Number out of range: " + percentile);
      }

      Collection<?> coll = (Collection<?>) args.getFirst();
      boolean invalid = coll.stream()
          .anyMatch(o -> !(o == null || o instanceof Number));
      if (invalid) {
        throw new IllegalArgumentException("Percentile function can only handle numerical values");
      }
      List<Number> data = coll.stream()
          .filter(Objects::nonNull)
          .map(o -> (Number) o)
          .sorted()
          .collect(toList());

      int size = data.size();
      if (size == 0) {
        return Tokens.NULL;
      } else if (size == 1) {
        return data.getFirst();
      }

      return percentileStrategy.apply(data, percentile);
    };
  }

  private static <T> T percentileNearest(List<T> sorted, double percentile) {
    int size = sorted.size();
    int index = (int) Math.ceil(percentile * size) - 1;
    if (index == -1) {
      index = 0;
    }
    return sorted.get(index);
  }

  public static Function<Traverser, Object> cypherSize() {
    return traverser -> traverser.get() instanceof String ?
        (long) ((String) traverser.get()).length() :
        (long) ((Collection) traverser.get()).size();
  }

  public static Function<Traverser, Object> cypherPlus() {
    return traverser -> {
      List<?> args = (List<?>) traverser.get();
      Object a = args.getFirst();
      Object b = args.get(1);

      if (a == Tokens.NULL || b == Tokens.NULL) {
        return Tokens.NULL;
      }

      if (a instanceof List || b instanceof List) {
        List<Object> objects = new ArrayList<>();
        if (a instanceof List list) {
          objects.addAll(list);
        } else {
          objects.add(a);
        }
        if (b instanceof List list) {
          objects.addAll(list);
        } else {
          objects.add(b);
        }
        return objects;
      }

      if (!(a instanceof String || a instanceof Number) ||
          !(b instanceof String || b instanceof Number)) {
        throw new TypeException("Illegal use of plus operator");
      }

      if (a instanceof Number number && b instanceof Number other) {
        if (a instanceof Double || b instanceof Double ||
            a instanceof Float || b instanceof Float) {
          return number.doubleValue() + other.doubleValue();
        } else {
          return number.longValue() + other.longValue();
        }
      } else {
        return String.valueOf(a) + String.valueOf(b);
      }
    };
  }

  public static Function<Traverser, Object> cypherReverse() {
    return traverser -> {
      Object o = traverser.get();
      if (o == Tokens.NULL) {
        return Tokens.NULL;
      } else if (o instanceof Collection collection) {
        ArrayList result = new ArrayList(collection);
        Collections.reverse(result);
        return result;
      } else if (o instanceof String string) {
        return new StringBuilder(string).reverse().toString();
      } else {
        throw new TypeException("Expected a string or list value for reverse, but got: %s(%s)".formatted(
          o.getClass().getSimpleName(), o));
      }
    };
  }

  public static Function<Traverser, Object> cypherSubstring() {
    return traverser -> {
      List<?> args = (List<?>) traverser.get();
      Object a = args.getFirst();
      Object b = args.get(1);

      if (a == Tokens.NULL) {
        return Tokens.NULL;
      } else if (!(a instanceof String) || (!(b instanceof Number))) {
        throw new TypeException("Expected substring(String, Integer, [Integer]), but got: (%s, %s)".formatted(
          a, b));
      } else if (args.size() == 3 && (!(args.get(2) instanceof Number))) {
        throw new TypeException("Expected substring(String, Integer, [Integer]), but got: (%s, %s, %s)".formatted(
          a, b, args.get(2)));
      } else if (args.size() == 3) {
        String s = (String) a;
        int endIndex = ((Number) b).intValue() + ((Number) args.get(2)).intValue();
        endIndex = endIndex > s.length() ? s.length() : endIndex;
        return s.substring(((Number) b).intValue(), endIndex);
      } else {
        return ((String) a).substring(((Number) b).intValue());
      }
    };
  }

  private static Function<Traverser, Object> cypherFunction(Function<List, Object> func, Class<?>... clazzes) {
    return traverser -> {
      List args = traverser.get() instanceof List ? ((List) traverser.get()) : asList(traverser.get());

      for (int i = 0; i < clazzes.length; i++) {
        if (args.get(i) == null || args.get(i) == Tokens.NULL) {
          return Tokens.NULL;
        }

        if (!clazzes[i].isInstance(args.get(i))) {
          throw new TypeException("Expected a %s value for <function1>, but got: %s(%s)".formatted(
            clazzes[i].getSimpleName(),
            args.get(i).getClass().getSimpleName(),
            args.get(i)));
        }
      }

      return func.apply(args);
    };
  }

  public static Function<Traverser, Object> cypherTrim() {
    return cypherFunction(a -> ((String) a.getFirst()).trim(), String.class);
  }

  public static Function<Traverser, Object> cypherToUpper() {
    return cypherFunction(a -> ((String) a.getFirst()).toUpperCase(), String.class);
  }

  public static Function<Traverser, Object> cypherToLower() {
    return cypherFunction(a -> ((String) a.getFirst()).toLowerCase(), String.class);
  }

  public static Function<Traverser, Object> cypherSplit() {
    return cypherFunction(a -> asList(((String) a.getFirst()).split((String) a.get(1))), String.class, String.class);
  }

  public static Function<Traverser, Object> cypherReplace() {
    return cypherFunction(a ->
            ((String) a.getFirst()).replace((String) a.get(1), (String) a.get(2)),
        String.class, String.class, String.class);
  }

  public static Function<Traverser, Object> cypherException() {
    return traverser -> {
      String message = CypherExceptions.messageByName(traverser.get());
      throw new ConstraintException(message);
    };
  }

  private static Object tokenToNull(Object maybeNull) {
    return Tokens.NULL.equals(maybeNull) ? null : maybeNull;
  }

  private static Object nullToToken(Object maybeNull) {
    return maybeNull == null ? Tokens.NULL : maybeNull;
  }

  private static <T> T cast(Object o, Class<T> clazz) {
    if (clazz.isInstance(o)) {
      return clazz.cast(o);
    } else {
      throw new TypeException("Expected %s to be %s, but it was %s".formatted(
        o, clazz.getSimpleName(), o.getClass().getSimpleName()));
    }
  }
}
