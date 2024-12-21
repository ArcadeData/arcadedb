package com.arcadedb.function;/*
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

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * A function library manages executable functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface FunctionLibraryDefinition<T extends FunctionDefinition> {
  /**
   * Returns the name of the library.
   */
  String getName();

  /**
   * Returns an iterable of the defined functions.
   */
  Iterable<T> getFunctions();

  /**
   * Returns a function by its name
   *
   * @param functionName Name of the function to retrieve
   *
   * @throws IllegalArgumentException If the function was not defined
   */
  T getFunction(String functionName) throws IllegalArgumentException;

  /**
   * Returns true if the function was defined, otherwise false.
   *
   * @param functionName Name of the function to look up to
   */
  boolean hasFunction(String functionName);

  /**
   * Registers a new function in the library.
   *
   * @param registerFunction function object to register
   */
  FunctionLibraryDefinition<T> registerFunction(T registerFunction);

  /**
   * Unregister a function from the library by its name.
   *
   * @param functionName Name of the function to unregister
   */
  FunctionLibraryDefinition<T> unregisterFunction(String functionName);
}
