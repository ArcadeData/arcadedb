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
import com.arcadedb.query.polyglot.GraalPolyglotEngine;
import com.arcadedb.query.polyglot.PolyglotEngineManager;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import java.util.Arrays;
import java.util.logging.Level;

/**
 * Executor for JavaScript-based trigger actions using GraalVM Polyglot.
 * Uses a shared GraalVM Engine instance from PolyglotEngineManager for optimal performance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ScriptTriggerExecutor implements TriggerExecutor {
  private final String              triggerName;
  private final String              script;
  private       GraalPolyglotEngine scriptEngine;

  public ScriptTriggerExecutor(final String triggerName, final String script) {
    this.triggerName = triggerName;
    this.script = script;
  }

  @Override
  public boolean execute(final Database database, final Record record, final Record oldRecord) {
    try {
      // Create script engine if not already initialized (lazy initialization)
      // Uses the shared GraalVM Engine from PolyglotEngineManager for better performance
      if (scriptEngine == null) {
        scriptEngine = GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
            .setLanguage("js")
            .setAllowedPackages(Arrays.asList("java.lang.*", "java.util.*", "java.time.*", "java.math.*"))
            .build();
      }

      // Set context variables
      scriptEngine.setAttribute("record", record);
      scriptEngine.setAttribute("$record", record);
      if (oldRecord != null) {
        scriptEngine.setAttribute("oldRecord", oldRecord);
        scriptEngine.setAttribute("$oldRecord", oldRecord);
      }

      // Execute the script
      final Value result = scriptEngine.eval(script);

      // If script returns a boolean false, abort the operation
      if (result != null && result.isBoolean() && !result.asBoolean()) {
        return false;
      }

      return true;
    } catch (final PolyglotException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing JavaScript trigger '%s': %s", e, triggerName,
          GraalPolyglotEngine.endUserMessage(e, true));
      throw new TriggerExecutionException("JavaScript trigger '" + triggerName + "' failed: " +
          GraalPolyglotEngine.endUserMessage(e, true), e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing JavaScript trigger '%s': %s", e, triggerName,
          e.getMessage());
      throw new TriggerExecutionException("JavaScript trigger '" + triggerName + "' failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if (scriptEngine != null) {
      try {
        scriptEngine.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing script engine for trigger '%s'", e, triggerName);
      } finally {
        scriptEngine = null;
      }
    }
    // Note: The shared Engine is managed by PolyglotEngineManager and should not be closed here
  }
}
