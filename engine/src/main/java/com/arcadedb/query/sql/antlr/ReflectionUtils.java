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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.exception.CommandSQLParsingException;

import java.lang.reflect.Field;

/**
 * Utility class for reflection operations used in AST building.
 * Provides convenience methods to set/get protected fields in AST classes.
 */
public class ReflectionUtils {

  /**
   * Sets a field value on a target object using reflection.
   * Automatically handles setAccessible(true) for protected fields.
   *
   * @param target the object whose field to set
   * @param targetClass the class containing the field
   * @param fieldName the name of the field
   * @param value the value to set
   * @throws CommandSQLParsingException if reflection fails
   */
  public static void setField(final Object target, final Class<?> targetClass, final String fieldName, final Object value) {
    try {
      final Field field = targetClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (final Exception e) {
      throw new CommandSQLParsingException(
          "Failed to set field '" + fieldName + "' on " + targetClass.getSimpleName() + ": " + e.getMessage(), e);
    }
  }

  /**
   * Sets a field value on a target object using reflection.
   * Uses the target's class to find the field.
   *
   * @param target the object whose field to set
   * @param fieldName the name of the field
   * @param value the value to set
   * @throws CommandSQLParsingException if reflection fails
   */
  public static void setField(final Object target, final String fieldName, final Object value) {
    setField(target, target.getClass(), fieldName, value);
  }

  /**
   * Gets a field value from a target object using reflection.
   * Automatically handles setAccessible(true) for protected fields.
   *
   * @param target the object whose field to get
   * @param targetClass the class containing the field
   * @param fieldName the name of the field
   * @return the field value
   * @throws CommandSQLParsingException if reflection fails
   */
  public static Object getField(final Object target, final Class<?> targetClass, final String fieldName) {
    try {
      final Field field = targetClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(target);
    } catch (final Exception e) {
      throw new CommandSQLParsingException(
          "Failed to get field '" + fieldName + "' from " + targetClass.getSimpleName() + ": " + e.getMessage(), e);
    }
  }

  /**
   * Gets a field value from a target object using reflection.
   * Uses the target's class to find the field.
   *
   * @param target the object whose field to get
   * @param fieldName the name of the field
   * @return the field value
   * @throws CommandSQLParsingException if reflection fails
   */
  public static Object getField(final Object target, final String fieldName) {
    return getField(target, target.getClass(), fieldName);
  }

  /**
   * Gets a field object for later use (e.g., to set multiple objects with same field).
   * Automatically handles setAccessible(true) for protected fields.
   *
   * @param targetClass the class containing the field
   * @param fieldName the name of the field
   * @return the Field object
   * @throws CommandSQLParsingException if reflection fails
   */
  public static Field getFieldObject(final Class<?> targetClass, final String fieldName) {
    try {
      final Field field = targetClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field;
    } catch (final Exception e) {
      throw new CommandSQLParsingException(
          "Failed to get field '" + fieldName + "' from " + targetClass.getSimpleName() + ": " + e.getMessage(), e);
    }
  }
}
