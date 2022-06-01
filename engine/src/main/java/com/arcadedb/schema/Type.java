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
package com.arcadedb.schema;

import com.arcadedb.database.*;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.MultiIterator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

/**
 * Generic representation of a type.<br>
 * allowAssignmentFrom accepts any class, but Array.class means that the type accepts generic Arrays.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public enum Type {
  BOOLEAN("Boolean", 0, BinaryTypes.TYPE_BOOLEAN, Boolean.class, new Class<?>[] { Number.class }),

  INTEGER("Integer", 1, BinaryTypes.TYPE_INT, Integer.class, new Class<?>[] { Number.class }),

  SHORT("Short", 2, BinaryTypes.TYPE_SHORT, Short.class, new Class<?>[] { Number.class }),

  LONG("Long", 3, BinaryTypes.TYPE_LONG, Long.class, new Class<?>[] { Number.class, }),

  FLOAT("Float", 4, BinaryTypes.TYPE_FLOAT, Float.class, new Class<?>[] { Number.class }),

  DOUBLE("Double", 5, BinaryTypes.TYPE_DOUBLE, Double.class, new Class<?>[] { Number.class }),

  DATETIME("Datetime", 6, BinaryTypes.TYPE_DATETIME, Date.class, new Class<?>[] { Date.class, Number.class }),

  STRING("String", 7, BinaryTypes.TYPE_STRING, String.class, new Class<?>[] { Enum.class }),

  BINARY("Binary", 8, BinaryTypes.TYPE_BINARY, byte[].class, new Class<?>[] { byte[].class }),

  LIST("List", 9, BinaryTypes.TYPE_LIST, List.class, new Class<?>[] { List.class, MultiIterator.class }),

  MAP("Map", 10, BinaryTypes.TYPE_MAP, Map.class, new Class<?>[] { Map.class }),

  LINK("Link", 11, BinaryTypes.TYPE_RID, Identifiable.class, new Class<?>[] { Identifiable.class, RID.class }),

  BYTE("Byte", 12, BinaryTypes.TYPE_BYTE, Byte.class, new Class<?>[] { Number.class }),

  DATE("Date", 13, BinaryTypes.TYPE_DATE, Date.class, new Class<?>[] { Number.class }),

  DECIMAL("Decimal", 14, BinaryTypes.TYPE_DECIMAL, BigDecimal.class, new Class<?>[] { BigDecimal.class, Number.class }),

  EMBEDDED("Embedded", 15, BinaryTypes.TYPE_EMBEDDED, Document.class,
      new Class<?>[] { EmbeddedDocument.class, ImmutableEmbeddedDocument.class, MutableEmbeddedDocument.class }),
  ;

  // Don't change the order, the type discover get broken if you change the order.
  private static final Type[] TYPES = new Type[] { LIST, MAP, LINK, STRING, DATETIME };

  private static final Type[]              TYPES_BY_ID       = new Type[17];
  // Values previously stored in javaTypes
  private static final Map<Class<?>, Type> TYPES_BY_USERTYPE = new HashMap<Class<?>, Type>();

  static {
    for (Type type : values()) {
      TYPES_BY_ID[type.id] = type;
    }
    // This is made by hand because not all types should be add.
    TYPES_BY_USERTYPE.put(Boolean.class, BOOLEAN);
    TYPES_BY_USERTYPE.put(Boolean.TYPE, BOOLEAN);
    TYPES_BY_USERTYPE.put(Integer.TYPE, INTEGER);
    TYPES_BY_USERTYPE.put(Integer.class, INTEGER);
    TYPES_BY_USERTYPE.put(BigInteger.class, INTEGER);
    TYPES_BY_USERTYPE.put(Short.class, SHORT);
    TYPES_BY_USERTYPE.put(Short.TYPE, SHORT);
    TYPES_BY_USERTYPE.put(Long.class, LONG);
    TYPES_BY_USERTYPE.put(Long.TYPE, LONG);
    TYPES_BY_USERTYPE.put(Float.TYPE, FLOAT);
    TYPES_BY_USERTYPE.put(Float.class, FLOAT);
    TYPES_BY_USERTYPE.put(Double.TYPE, DOUBLE);
    TYPES_BY_USERTYPE.put(Double.class, DOUBLE);
    TYPES_BY_USERTYPE.put(Date.class, DATETIME);
    TYPES_BY_USERTYPE.put(String.class, STRING);
    TYPES_BY_USERTYPE.put(Enum.class, STRING);
    TYPES_BY_USERTYPE.put(byte[].class, BINARY);
    TYPES_BY_USERTYPE.put(Byte.class, BYTE);
    TYPES_BY_USERTYPE.put(Byte.TYPE, BYTE);
    TYPES_BY_USERTYPE.put(Character.class, STRING);
    TYPES_BY_USERTYPE.put(Character.TYPE, STRING);
    TYPES_BY_USERTYPE.put(BigDecimal.class, DECIMAL);
    TYPES_BY_USERTYPE.put(List.class, LIST);
    TYPES_BY_USERTYPE.put(Map.class, MAP);
    TYPES_BY_USERTYPE.put(EmbeddedDocument.class, EMBEDDED);
    TYPES_BY_USERTYPE.put(ImmutableEmbeddedDocument.class, EMBEDDED);
    TYPES_BY_USERTYPE.put(MutableEmbeddedDocument.class, EMBEDDED);

    BYTE.castable.add(BOOLEAN);
    SHORT.castable.addAll(Arrays.asList(BOOLEAN, BYTE));
    INTEGER.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT));
    LONG.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER));
    FLOAT.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER));
    DOUBLE.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT));
    DECIMAL.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE));
  }

  protected final String     name;
  protected final int        id;
  protected final byte       binaryType;
  protected final Class<?>   javaDefaultType;
  protected final Class<?>[] allowAssignmentFrom;
  protected final Set<Type>  castable;

  Type(final String iName, final int iId, final byte binaryType, final Class<?> iJavaDefaultType, final Class<?>[] iAllowAssignmentBy) {
    this.name = iName.toUpperCase();
    this.id = iId;
    this.binaryType = binaryType;
    this.javaDefaultType = iJavaDefaultType;
    this.allowAssignmentFrom = iAllowAssignmentBy;
    this.castable = new HashSet<Type>();
    this.castable.add(this);
  }

  /**
   * Return the type by ID.
   *
   * @param iId The id to search
   *
   * @return The type if any, otherwise null
   */
  public static Type getById(final byte iId) {
    if (iId >= 0 && iId < TYPES_BY_ID.length)
      return TYPES_BY_ID[iId];
    return null;
  }

  public static void validateValue(final Object value) {
    if (value != null) {
      if (value instanceof String || value instanceof Number || value instanceof Map || value instanceof Collection)
        return;

      if (!TYPES_BY_USERTYPE.containsKey(value.getClass()))
        throw new IllegalArgumentException("Value '" + value + "' of class '" + value.getClass() + "' is not supported");
    }
  }

  /**
   * Get the identifier of the type. use this instead of {@link Enum#ordinal()} for guarantee a cross code version identifier.
   *
   * @return the identifier of the type.
   */
  public int getId() {
    return id;
  }

  /**
   * Return the correspondent type by checking the "assignability" of the class received as parameter.
   *
   * @param iClass Class to check
   *
   * @return OType instance if found, otherwise null
   */
  public static Type getTypeByClass(final Class<?> iClass) {
    if (iClass == null)
      return null;

    Type type = TYPES_BY_USERTYPE.get(iClass);
    if (type != null)
      return type;
    type = getTypeByClassInherit(iClass);

    return type;
  }

  private static Type getTypeByClassInherit(final Class<?> iClass) {
    if (iClass.isArray())
      return LIST;
    int priority = 0;
    boolean comparedAtLeastOnce;
    do {
      comparedAtLeastOnce = false;
      for (final Type type : TYPES) {
        if (type.allowAssignmentFrom.length > priority) {
          if (type.allowAssignmentFrom[priority].isAssignableFrom(iClass))
            return type;
          comparedAtLeastOnce = true;
        }
      }

      priority++;
    } while (comparedAtLeastOnce);
    return null;
  }

  public static Type getTypeByValue(Object value) {
    if (value == null)
      return null;
    Class<?> typez = value.getClass();
    Type type = TYPES_BY_USERTYPE.get(typez);
    if (type != null)
      return type;

    final Type byType = getTypeByClassInherit(typez);

    return byType;
  }

  public static Type getTypeByName(final String name) {
    return valueOf(name.toUpperCase());
  }

  private static boolean checkLinkCollection(Collection<?> toCheck) {
    boolean empty = true;
    for (Object object : toCheck) {
      if (object != null && !(object instanceof Identifiable))
        return false;
      else if (object != null)
        empty = false;
    }
    return !empty;
  }

  public static boolean isSimpleType(final Object iObject) {
    if (iObject == null)
      return false;

    final Class<? extends Object> iType = iObject.getClass();

    return iType.isPrimitive() || Number.class.isAssignableFrom(iType) || String.class.isAssignableFrom(iType) || Boolean.class.isAssignableFrom(iType)
        || Date.class.isAssignableFrom(iType) || (iType.isArray() && (iType.equals(byte[].class) || iType.equals(char[].class) || iType.equals(int[].class)
        || iType.equals(long[].class) || iType.equals(double[].class) || iType.equals(float[].class) || iType.equals(short[].class) || iType.equals(
        Integer[].class) || iType.equals(String[].class) || iType.equals(Long[].class) || iType.equals(Short[].class) || iType.equals(Double[].class)));

  }

  /**
   * Convert types based on the iTargetClass parameter.
   *
   * @param iValue       Value to convert
   * @param iTargetClass Expected class
   *
   * @return The converted value or the original if no conversion was applied
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Object convert(final Database database, final Object iValue, final Class<?> iTargetClass) {
    if (iValue == null)
      return null;

    if (iTargetClass == null)
      return iValue;

    if (iValue.getClass().equals(iTargetClass))
      // SAME TYPE: DON'T CONVERT IT
      return iValue;

    if (iTargetClass.isAssignableFrom(iValue.getClass()))
      // COMPATIBLE TYPES: DON'T CONVERT IT
      return iValue;

    try {
      if (iTargetClass.equals(String.class))
        return iValue.toString();
      else if (iValue instanceof Binary && iTargetClass.isAssignableFrom(byte[].class))
        return ((Binary) iValue).toByteArray();
      else if (byte[].class.isAssignableFrom(iValue.getClass())) {
        return iValue;
      } else if (iTargetClass.isEnum()) {
        if (iValue instanceof Number)
          return ((Class<Enum>) iTargetClass).getEnumConstants()[((Number) iValue).intValue()];
        return Enum.valueOf((Class<Enum>) iTargetClass, iValue.toString());
      } else if (iTargetClass.equals(Byte.TYPE) || iTargetClass.equals(Byte.class)) {
        if (iValue instanceof Byte)
          return iValue;
        else if (iValue instanceof String)
          return Byte.parseByte((String) iValue);
        else
          return ((Number) iValue).byteValue();

      } else if (iTargetClass.equals(Short.TYPE) || iTargetClass.equals(Short.class)) {
        if (iValue instanceof Short)
          return iValue;
        else if (iValue instanceof String)
          return ((String) iValue).isEmpty() ? 0 : Short.parseShort((String) iValue);
        else
          return ((Number) iValue).shortValue();

      } else if (iTargetClass.equals(Integer.TYPE) || iTargetClass.equals(Integer.class)) {
        if (iValue instanceof Integer)
          return iValue;
        else if (iValue instanceof String)
          return ((String) iValue).isEmpty() ? 0 : Integer.parseInt((String) iValue);
        else
          return ((Number) iValue).intValue();

      } else if (iTargetClass.equals(Long.TYPE) || iTargetClass.equals(Long.class)) {
        if (iValue instanceof Long)
          return iValue;
        else if (iValue instanceof String)
          return ((String) iValue).isEmpty() ? 0l : Long.parseLong((String) iValue);
        else if (iValue instanceof Date)
          return ((Date) iValue).getTime();
        else
          return ((Number) iValue).longValue();

      } else if (iTargetClass.equals(Float.TYPE) || iTargetClass.equals(Float.class)) {
        if (iValue instanceof Float)
          return iValue;
        else if (iValue instanceof String)
          return ((String) iValue).isEmpty() ? 0f : Float.parseFloat((String) iValue);
        else
          return ((Number) iValue).floatValue();

      } else if (iTargetClass.equals(BigDecimal.class)) {
        if (iValue instanceof String)
          return new BigDecimal((String) iValue);
        else if (iValue instanceof Number)
          return new BigDecimal(iValue.toString());

      } else if (iTargetClass.equals(Double.TYPE) || iTargetClass.equals(Double.class)) {
        if (iValue instanceof Double)
          return iValue;
        else if (iValue instanceof String)
          return ((String) iValue).isEmpty() ? 0d : Double.parseDouble((String) iValue);
        else if (iValue instanceof Float)
          // THIS IS NECESSARY DUE TO A BUG/STRANGE BEHAVIOR OF JAVA BY LOSING PRECISION
          return Double.parseDouble(iValue.toString());
        else
          return ((Number) iValue).doubleValue();

      } else if (iTargetClass.equals(Boolean.TYPE) || iTargetClass.equals(Boolean.class)) {
        if (iValue instanceof Boolean)
          return iValue;
        else if (iValue instanceof String) {
          if (((String) iValue).equalsIgnoreCase("true"))
            return Boolean.TRUE;
          else if (((String) iValue).equalsIgnoreCase("false"))
            return Boolean.FALSE;
          throw new IllegalArgumentException("Value is not boolean. Expected true or false but received '" + iValue + "'");
        } else if (iValue instanceof Number)
          return ((Number) iValue).intValue() != 0;

      } else if (Set.class.isAssignableFrom(iTargetClass)) {
        // The caller specifically wants a Set.  If the value is a collection
        // we will add all of the items in the collection to a set.  Otherwise
        // we will create a singleton set with only the value in it.
        if (iValue instanceof Collection<?>) {
            final Set<Object> set = new HashSet<Object>((Collection<? extends Object>) iValue);
          return set;
        } else {
          return Collections.singleton(iValue);
        }

      } else if (List.class.isAssignableFrom(iTargetClass)) {
        // The caller specifically wants a List.  If the value is a collection
        // we will add all of the items in the collection to a List.  Otherwise
        // we will create a singleton List with only the value in it.
        if (iValue instanceof Collection<?>) {
            final List<Object> list = new ArrayList<Object>((Collection<? extends Object>) iValue);
          return list;
        } else {
          return Collections.singletonList(iValue);
        }

      } else if (Collection.class.equals(iTargetClass)) {
        // The caller specifically wants a Collection of any type.
        // we will return a list if the value is a collection or
        // a singleton set if the value is not a collection.
        if (iValue instanceof Collection<?>) {
            final List<Object> set = new ArrayList<Object>((Collection<? extends Object>) iValue);
          return set;
        } else {
          return Collections.singleton(iValue);
        }

      } else if (iTargetClass.equals(Date.class)) {
        if (iValue instanceof Number)
          return new Date(((Number) iValue).longValue());
        if (iValue instanceof Calendar)
          return ((Calendar) iValue).getTime();
        if (iValue instanceof String) {
          if (FileUtils.isLong(iValue.toString()))
            return new Date(Long.parseLong(iValue.toString()));
          if (database != null)
            try {
              return new SimpleDateFormat(database.getSchema().getDateTimeFormat()).parse((String) iValue);
            } catch (ParseException ignore) {
              return new SimpleDateFormat(database.getSchema().getDateFormat()).parse((String) iValue);
            }
        }
      } else if (iTargetClass.equals(Identifiable.class)) {
        if (MultiValue.isMultiValue(iValue)) {
          List<Identifiable> result = new ArrayList<>();
          for (Object o : MultiValue.getMultiValueIterable(iValue)) {
            if (o instanceof Identifiable) {
              result.add((Identifiable) o);
            } else if (o instanceof String) {
              try {
                result.add(new RID(database, iValue.toString()));
              } catch (Exception e) {
                LogManager.instance().log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, iValue, iTargetClass);
              }
            }
          }
          return result;
        } else if (iValue instanceof String) {
          try {
            return new RID(database, (String) iValue);
          } catch (Exception e) {
            LogManager.instance().log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, iValue, iTargetClass);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      LogManager.instance().log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, iValue, iTargetClass);
      return null;
    }

    return iValue;
  }

  public static Number increment(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot increment a null value");

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() + b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.intValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.intValue() + b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() + b.shortValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.intValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.intValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.intValue()).add((BigDecimal) b);

    } else if (a instanceof Long) {
      if (b instanceof Integer)
        return Long.valueOf(a.longValue() + b.intValue());
      else if (b instanceof Long)
        return Long.valueOf(a.longValue() + b.longValue());
      else if (b instanceof Short)
        return Long.valueOf(a.longValue() + b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.longValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.longValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.longValue()).add((BigDecimal) b);

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() + b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.shortValue() + b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.shortValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.shortValue() + b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO INTEGER
          return Integer.valueOf(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.shortValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.shortValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.shortValue()).add((BigDecimal) b);

    } else if (a instanceof Float) {
      if (b instanceof Integer)
        return Float.valueOf(a.floatValue() + b.intValue());
      else if (b instanceof Long)
        return Float.valueOf(a.floatValue() + b.longValue());
      else if (b instanceof Short)
        return Float.valueOf(a.floatValue() + b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.floatValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.floatValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return BigDecimal.valueOf(a.floatValue()).add((BigDecimal) b);

    } else if (a instanceof Double) {
      if (b instanceof Integer)
        return Double.valueOf(a.doubleValue() + b.intValue());
      else if (b instanceof Long)
        return Double.valueOf(a.doubleValue() + b.longValue());
      else if (b instanceof Short)
        return Double.valueOf(a.doubleValue() + b.shortValue());
      else if (b instanceof Float)
        return Double.valueOf(a.doubleValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.doubleValue() + b.doubleValue());
      else if (b instanceof BigDecimal)
        return BigDecimal.valueOf(a.doubleValue()).add((BigDecimal) b);

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer)
        return ((BigDecimal) a).add(new BigDecimal(b.intValue()));
      else if (b instanceof Long)
        return ((BigDecimal) a).add(new BigDecimal(b.longValue()));
      else if (b instanceof Short)
        return ((BigDecimal) a).add(new BigDecimal(b.shortValue()));
      else if (b instanceof Float)
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.floatValue()));
      else if (b instanceof Double)
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.doubleValue()));
      else if (b instanceof BigDecimal)
        return ((BigDecimal) a).add((BigDecimal) b);

    }

    throw new IllegalArgumentException("Cannot increment value '" + a + "' (" + a.getClass() + ") with '" + b + "' (" + b.getClass() + ")");
  }

  public static Number decrement(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot decrement a null value");

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() - b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() - b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.intValue() - b.longValue());
      else if (b instanceof Short) {
        final int sum = a.intValue() - b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() - b.shortValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.intValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.intValue() - b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.intValue()).subtract((BigDecimal) b);

    } else if (a instanceof Long) {
      if (b instanceof Integer)
        return Long.valueOf(a.longValue() - b.intValue());
      else if (b instanceof Long)
        return Long.valueOf(a.longValue() - b.longValue());
      else if (b instanceof Short)
        return Long.valueOf(a.longValue() - b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.longValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.longValue() - b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.longValue()).subtract((BigDecimal) b);

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() - b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.shortValue() - b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.shortValue() - b.longValue());
      else if (b instanceof Short) {
        final int sum = a.shortValue() - b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO INTEGER
          return Integer.valueOf(a.intValue() - b.intValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.shortValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.shortValue() - b.doubleValue());
      else if (b instanceof BigDecimal)
        return new BigDecimal(a.shortValue()).subtract((BigDecimal) b);

    } else if (a instanceof Float) {
      if (b instanceof Integer)
        return Float.valueOf(a.floatValue() - b.intValue());
      else if (b instanceof Long)
        return Float.valueOf(a.floatValue() - b.longValue());
      else if (b instanceof Short)
        return Float.valueOf(a.floatValue() - b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.floatValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.floatValue() - b.doubleValue());
      else if (b instanceof BigDecimal)
        return BigDecimal.valueOf(a.floatValue()).subtract((BigDecimal) b);

    } else if (a instanceof Double) {
      if (b instanceof Integer)
        return Double.valueOf(a.doubleValue() - b.intValue());
      else if (b instanceof Long)
        return Double.valueOf(a.doubleValue() - b.longValue());
      else if (b instanceof Short)
        return Double.valueOf(a.doubleValue() - b.shortValue());
      else if (b instanceof Float)
        return Double.valueOf(a.doubleValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.doubleValue() - b.doubleValue());
      else if (b instanceof BigDecimal)
        return BigDecimal.valueOf(a.doubleValue()).subtract((BigDecimal) b);

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer)
        return ((BigDecimal) a).subtract(new BigDecimal(b.intValue()));
      else if (b instanceof Long)
        return ((BigDecimal) a).subtract(new BigDecimal(b.longValue()));
      else if (b instanceof Short)
        return ((BigDecimal) a).subtract(new BigDecimal(b.shortValue()));
      else if (b instanceof Float)
        return ((BigDecimal) a).subtract(BigDecimal.valueOf(b.floatValue()));
      else if (b instanceof Double)
        return ((BigDecimal) a).subtract(BigDecimal.valueOf(b.doubleValue()));
      else if (b instanceof BigDecimal)
        return ((BigDecimal) a).subtract((BigDecimal) b);

    }

    throw new IllegalArgumentException("Cannot decrement value '" + a + "' (" + a.getClass() + ") with '" + b + "' (" + b.getClass() + ")");
  }

  public static Number[] castComparableNumber(Number context, Number max) {
    // CHECK FOR CONVERSION
    if (context instanceof Short) {
      // SHORT
      if (max instanceof Integer)
        context = context.intValue();
      else if (max instanceof Long)
        context = context.longValue();
      else if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.intValue());
      else if (max instanceof Byte)
        context = context.byteValue();

    } else if (context instanceof Integer) {
      // INTEGER
      if (max instanceof Long)
        context = context.longValue();
      else if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.intValue());
      else if (max instanceof Short)
        max = max.intValue();
      else if (max instanceof Byte)
        max = max.intValue();

    } else if (context instanceof Long) {
      // LONG
      if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.longValue());
      else if (max instanceof Integer || max instanceof Byte || max instanceof Short)
        max = max.longValue();

    } else if (context instanceof Float) {
      // FLOAT
      if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = BigDecimal.valueOf(context.floatValue());
      else if (max instanceof Byte || max instanceof Short || max instanceof Integer || max instanceof Long)
        max = max.floatValue();

    } else if (context instanceof Double) {
      // DOUBLE
      if (max instanceof BigDecimal)
        context = BigDecimal.valueOf(context.doubleValue());
      else if (max instanceof Byte || max instanceof Short || max instanceof Integer || max instanceof Long || max instanceof Float)
        max = max.doubleValue();

    } else if (context instanceof BigDecimal) {
      // DOUBLE
      if (max instanceof Integer)
        max = new BigDecimal((Integer) max);
      else if (max instanceof Float)
        max = BigDecimal.valueOf((Float) max);
      else if (max instanceof Double)
        max = BigDecimal.valueOf((Double) max);
      else if (max instanceof Short)
        max = new BigDecimal((Short) max);
      else if (max instanceof Byte)
        max = new BigDecimal((Byte) max);
    } else if (context instanceof Byte) {
      if (max instanceof Short)
        context = context.shortValue();
      else if (max instanceof Integer)
        context = context.intValue();
      else if (max instanceof Long)
        context = context.longValue();
      else if (max instanceof Float)
        context = context.floatValue();
      else if (max instanceof Double)
        context = context.doubleValue();
      else if (max instanceof BigDecimal)
        context = new BigDecimal(context.intValue());
    }

    return new Number[] { context, max };
  }

  /**
   * Convert the input object to an integer.
   *
   * @param iValue Any type supported
   *
   * @return The integer value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public int asInt(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).intValue();
    else if (iValue instanceof String)
      return Integer.parseInt((String) iValue);
    else if (iValue instanceof Boolean)
      return ((Boolean) iValue) ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to int for type: " + name);
  }

  /**
   * Convert the input object to a long.
   *
   * @param iValue Any type supported
   *
   * @return The long value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public long asLong(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).longValue();
    else if (iValue instanceof String)
      return Long.parseLong((String) iValue);
    else if (iValue instanceof Boolean)
      return ((Boolean) iValue) ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to long for type: " + name);
  }

  /**
   * Convert the input object to a Float.
   *
   * @param iValue Any type supported
   *
   * @return The float value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public float asFloat(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).floatValue();
    else if (iValue instanceof String)
      return Float.parseFloat((String) iValue);

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to float for type: " + name);
  }

  /**
   * Convert the input object to a Double.
   *
   * @param iValue Any type supported
   *
   * @return The double value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public double asDouble(final Object iValue) {
    if (iValue instanceof Number)
      return ((Number) iValue).doubleValue();
    else if (iValue instanceof String)
      return Double.parseDouble((String) iValue);

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to double for type: " + name);
  }

  public byte getBinaryType() {
    return binaryType;
  }

  /**
   * Convert the input object to a string.
   *
   * @param iValue Any type supported
   *
   * @return The string if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  @Deprecated
  public String asString(final Object iValue) {
    return iValue.toString();
  }

  public boolean isMultiValue() {
    return this == LIST || this == MAP;
  }

  public boolean isLink() {
    return this == LINK;
  }

  public boolean isEmbedded() {
    return this == LIST || this == MAP;
  }

  public Class<?> getDefaultJavaType() {
    return javaDefaultType;
  }

  public Set<Type> getCastable() {
    return castable;
  }

  @Deprecated
  public Class<?>[] getJavaTypes() {
    return null;
  }

  public Object newInstance(Object value) {
    return convert(null, value, javaDefaultType);
  }
}
