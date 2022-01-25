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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * This is able to invoke a static method using reflection. If contains more than one {@link Method} it tries
 * to pick the one that better fits the input parameters.
 *
 * @author Fabrizio Fortino
 */
public class SQLStaticReflectiveFunction extends SQLFunctionAbstract {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = new HashMap<>();

  static {
    PRIMITIVE_TO_WRAPPER.put(Boolean.TYPE, Boolean.class);
    PRIMITIVE_TO_WRAPPER.put(Byte.TYPE, Byte.class);
    PRIMITIVE_TO_WRAPPER.put(Character.TYPE, Character.class);
    PRIMITIVE_TO_WRAPPER.put(Short.TYPE, Short.class);
    PRIMITIVE_TO_WRAPPER.put(Integer.TYPE, Integer.class);
    PRIMITIVE_TO_WRAPPER.put(Long.TYPE, Long.class);
    PRIMITIVE_TO_WRAPPER.put(Double.TYPE, Double.class);
    PRIMITIVE_TO_WRAPPER.put(Float.TYPE, Float.class);
    PRIMITIVE_TO_WRAPPER.put(Void.TYPE, Void.TYPE);
  }

  private static final Map<Class<?>, Class<?>> WRAPPER_TO_PRIMITIVE = new HashMap<>();

  static {
    for (Map.Entry<Class<?>, Class<?>> entry : PRIMITIVE_TO_WRAPPER.entrySet()) {
      if (!entry.getKey().equals(entry.getValue())) {
        WRAPPER_TO_PRIMITIVE.put(entry.getValue(), entry.getKey());
      }
    }
  }

  private static final Map<Class<?>, Integer> PRIMITIVE_WEIGHT = new HashMap<>();

  static {
    PRIMITIVE_WEIGHT.put(boolean.class, 1);
    PRIMITIVE_WEIGHT.put(char.class, 2);
    PRIMITIVE_WEIGHT.put(byte.class, 3);
    PRIMITIVE_WEIGHT.put(short.class, 4);
    PRIMITIVE_WEIGHT.put(int.class, 5);
    PRIMITIVE_WEIGHT.put(long.class, 6);
    PRIMITIVE_WEIGHT.put(float.class, 7);
    PRIMITIVE_WEIGHT.put(double.class, 8);
    PRIMITIVE_WEIGHT.put(void.class, 9);
  }

  private final Method[] methods;

  public SQLStaticReflectiveFunction(String name, int minParams, int maxParams, Method... methods) {
    super(name);
    this.methods = methods;
    // we need to sort the methods by parameters type to return the closest overloaded method
    Arrays.sort(methods, (m1, m2) -> {
      Class<?>[] m1Params = m1.getParameterTypes();
      Class<?>[] m2Params = m2.getParameterTypes();

      int c = m1Params.length - m2Params.length;
      if (c == 0) {
        for (int i = 0; i < m1Params.length; i++) {
          if (m1Params[i].isPrimitive() && m2Params[i].isPrimitive() && !m1Params[i].equals(m2Params[i])) {
            c += PRIMITIVE_WEIGHT.get(m1Params[i]) - PRIMITIVE_WEIGHT.get(m2Params[i]);
          }
        }
      }

      return c;
    });
  }

  @Override
  public Object execute(Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, Object[] iParams, CommandContext iContext) {

    final Supplier<String> paramsPrettyPrint = () -> Arrays.stream(iParams).map(p -> p + " [ " + p.getClass().getName() + " ]")
        .collect(Collectors.joining(", ", "(", ")"));

    Method method = pickMethod(iParams);

    if (method == null) {
      throw new QueryParsingException("Unable to find a function for " + name + paramsPrettyPrint.get());
    }

    try {
      return method.invoke(null, iParams);
    } catch (ReflectiveOperationException e) {
      throw new QueryParsingException("Error executing function " + name + paramsPrettyPrint.get(), e);
    } catch (IllegalArgumentException x) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing function %s", x, name);

      return null; //if a function fails for given input, just return null to avoid breaking the query execution
    }

  }

  @Override
  public String getSyntax() {
    return this.getName();
  }

  private Method pickMethod(Object[] iParams) {
    for (Method m : methods) {
      Class<?>[] parameterTypes = m.getParameterTypes();
      if (iParams.length == parameterTypes.length) {
        boolean match = true;
        for (int i = 0; i < parameterTypes.length; i++) {
          if (iParams[i] != null && !isAssignable(iParams[i].getClass(), parameterTypes[i])) {
            match = false;
            break;
          }
        }

        if (match) {
          return m;
        }
      }
    }

    return null;
  }

  private static boolean isAssignable(final Class<?> iFromClass, final Class<?> iToClass) {
    // handle autoboxing
    final BiFunction<Class<?>, Class<?>, Class<?>> autoboxer = (from, to) -> {
      if (from.isPrimitive() && !to.isPrimitive()) {
        return PRIMITIVE_TO_WRAPPER.get(from);
      } else if (to.isPrimitive() && !from.isPrimitive()) {
        return WRAPPER_TO_PRIMITIVE.get(from);
      } else
        return from;
    };

    final Class<?> fromClass = autoboxer.apply(iFromClass, iToClass);

    if (fromClass == null) {
      return false;
    } else if (fromClass.equals(iToClass)) {
      return true;
    } else if (fromClass.isPrimitive()) {
      if (!iToClass.isPrimitive()) {
        return false;
      } else if (Integer.TYPE.equals(fromClass)) {
        return Long.TYPE.equals(iToClass) || Float.TYPE.equals(iToClass) || Double.TYPE.equals(iToClass);
      } else if (Long.TYPE.equals(fromClass)) {
        return Float.TYPE.equals(iToClass) || Double.TYPE.equals(iToClass);
      } else if (Boolean.TYPE.equals(fromClass)) {
        return false;
      } else if (Double.TYPE.equals(fromClass)) {
        return false;
      } else if (Float.TYPE.equals(fromClass)) {
        return Double.TYPE.equals(iToClass);
      } else if (Character.TYPE.equals(fromClass)) {
        return Integer.TYPE.equals(iToClass) || Long.TYPE.equals(iToClass) || Float.TYPE.equals(iToClass) || Double.TYPE.equals(iToClass);
      } else if (Short.TYPE.equals(fromClass)) {
        return Integer.TYPE.equals(iToClass) || Long.TYPE.equals(iToClass) || Float.TYPE.equals(iToClass) || Double.TYPE.equals(iToClass);
      } else if (Byte.TYPE.equals(fromClass)) {
        return Short.TYPE.equals(iToClass) || Integer.TYPE.equals(iToClass) || Long.TYPE.equals(iToClass) || Float.TYPE.equals(iToClass) || Double.TYPE.equals(
            iToClass);
      }
      // this should never happen
      return false;
    }
    return iToClass.isAssignableFrom(fromClass);
  }

}
