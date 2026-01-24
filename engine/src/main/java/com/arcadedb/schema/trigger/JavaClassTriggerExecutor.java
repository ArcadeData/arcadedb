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
package com.arcadedb.schema.trigger;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * Executor for Java class-based triggers.
 * <p>
 * This executor loads a user-provided Java class that implements the {@link JavaTrigger} interface
 * and delegates execution to it. This is the fastest trigger option as it executes native Java code
 * without any parsing or interpretation overhead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JavaClassTriggerExecutor implements TriggerExecutor {
  private final String triggerName;
  private final String className;
  private final JavaTrigger instance;

  /**
   * Create a new Java class trigger executor.
   *
   * @param triggerName The name of the trigger (for logging)
   * @param className   The fully qualified class name to instantiate
   * @throws TriggerExecutionException if the class cannot be loaded or instantiated
   */
  public JavaClassTriggerExecutor(final String triggerName, final String className) {
    this.triggerName = triggerName;
    this.className = className;

    try {
      // Load the class
      final Class<?> clazz = Class.forName(className);

      // Verify it implements JavaTrigger
      if (!JavaTrigger.class.isAssignableFrom(clazz)) {
        throw new TriggerExecutionException(
            "Class '" + className + "' must implement " + JavaTrigger.class.getName());
      }

      // Instantiate the class
      this.instance = (JavaTrigger) clazz.getDeclaredConstructor().newInstance();

    } catch (final ClassNotFoundException e) {
      throw new TriggerExecutionException(
          "Trigger class '" + className + "' not found. Make sure the class is on the classpath.", e);
    } catch (final NoSuchMethodException e) {
      throw new TriggerExecutionException(
          "Trigger class '" + className + "' must have a public no-argument constructor", e);
    } catch (final TriggerExecutionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TriggerExecutionException(
          "Failed to instantiate trigger class '" + className + "': " + e.getMessage(), e);
    }
  }

  @Override
  public boolean execute(final Database database, final Record record, final Record oldRecord) {
    try {
      return instance.execute(database, record, oldRecord);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Error executing Java trigger '%s' (class: %s): %s", e, triggerName, className, e.getMessage());
      throw new TriggerExecutionException(
          "Java trigger '" + triggerName + "' (class: " + className + ") failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void cleanup() {
    // No resources to cleanup for Java executor
  }

  /**
   * Get the Java class name for this trigger.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Get the loaded trigger instance.
   */
  public JavaTrigger getInstance() {
    return instance;
  }
}
