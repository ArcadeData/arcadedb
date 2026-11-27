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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.polyglot.GraalPolyglotEngine;
import com.arcadedb.query.polyglot.HostClassLookupFilter;
import com.arcadedb.query.polyglot.PolyglotEngineManager;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Executor for JavaScript-based trigger actions using GraalVM Polyglot.
 * Uses a shared GraalVM Engine instance from PolyglotEngineManager for optimal performance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ScriptTriggerExecutor implements TriggerExecutor {
  /**
   * Host classes a trigger script may resolve through {@code Java.type(...)}. Entries are matched literally by
   * {@link HostClassLookupFilter}: {@code pkg.*} is that package alone and {@code pkg.**} includes subpackages.
   * <p>
   * {@code java.lang.*} is deliberately absent: it let a trigger reach
   * {@code Java.type("java.lang.Runtime").getRuntime().exec(...)} / {@code ProcessBuilder} / {@code System.exit} and
   * obtain OS command execution with only UPDATE_SCHEMA privileges (GHSA-x9f9-r4m8-9xc2). {@code allowCreateProcess(false)}
   * does not stop that, because {@code exec()} is a host method call reached through HostAccess and not GraalVM's guest
   * process API. JavaScript has native String/Math/Number, so dropping {@code java.lang.*} rarely affects real triggers.
   * <p>
   * {@code java.util} is restricted to its own level ({@code java.util.*}) plus the three subpackages made of pure
   * value types and functional interfaces. The recursive {@code java.util.**} would readmit {@code java.util.zip},
   * {@code java.util.jar}, {@code java.util.logging} and {@code java.util.concurrent}: the regex-matched
   * {@code "java.util.*"} used to do exactly that and handed a trigger an arbitrary host-file-read primitive through
   * {@code ZipFile}/{@code JarFile} (GHSA-wx28-2265-f788). The I/O-capable classes that live directly in
   * {@code java.util} - {@code Formatter}, {@code Scanner}, {@code Timer}, {@code ServiceLoader} - are rejected by
   * {@link HostClassLookupFilter#DENIED}, which is enforced on top of this list.
   */
  public static final List<String> ALLOWED_PACKAGES = List.of(//
      "java.util.*",                                        //
      "java.util.function.**",                              //
      "java.util.stream.**",                                //
      "java.util.regex.**",                                 //
      "java.time.**",                                       //
      "java.math.**");

  private final     String              triggerName;
  private final     String              script;
  /**
   * Serializes the runs of the script: the context is shared by every firing of the trigger, on any thread, so the
   * bindings of a run have to be set, used and removed before another run can touch them. Reentrant, because a script
   * that writes a record of its own type fires the trigger again on the same thread.
   */
  private final     Object              runLock    = new Object();
  /** Guards the build and the close of the context, never held while a script runs, so a close does not wait for it. */
  private final     Object              engineLock = new Object();
  private volatile  GraalPolyglotEngine scriptEngine;

  public ScriptTriggerExecutor(final String triggerName, final String script) {
    this.triggerName = triggerName;
    this.script = script;
  }

  @Override
  public boolean execute(final Database database, final Record record, final Record oldRecord) {
    try {
      // Context variables of this run only
      final Map<String, Object> bindings = oldRecord == null ?
          Map.of("record", record, "$record", record) :
          Map.of("record", record, "$record", record, "oldRecord", oldRecord, "$oldRecord", oldRecord);

      final Value result = run(database, bindings);

      // If script returns a boolean false, abort the operation
      if (result != null && result.isBoolean() && !result.asBoolean()) {
        return false;
      }

      return true;
    } catch (final QueryTerminatedException e) {
      // Stopped on request: not a fault of the trigger
      throw e;
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

  /**
   * A BEFORE READ trigger has no record to bind - see {@link TriggerExecutor#executeBeforeRead}. The identity is
   * bound instead, so the script can still address WHICH record is being read; {@code record}/{@code $record} are
   * deliberately not set, so a script that expects them fails visibly instead of silently seeing nothing.
   */
  @Override
  public boolean executeBeforeRead(final Database database, final RID rid) {
    try {
      final Value result = run(database, Map.of("rid", rid, "$rid", rid));

      return result == null || !result.isBoolean() || result.asBoolean();
    } catch (final QueryTerminatedException e) {
      // Stopped on request: not a fault of the trigger
      throw e;
    } catch (final PolyglotException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing JavaScript trigger '%s': %s", e, triggerName,
          GraalPolyglotEngine.endUserMessage(e, true));
      throw new TriggerExecutionException(
          "JavaScript trigger '" + triggerName + "' failed: " + GraalPolyglotEngine.endUserMessage(e, true), e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing JavaScript trigger '%s': %s", e, triggerName,
          e.getMessage());
      throw new TriggerExecutionException("JavaScript trigger '" + triggerName + "' failed: " + e.getMessage(), e);
    }
  }

  /**
   * Evaluates the script with {@code bindings} visible to it and to nobody else: they are bound, the script runs and they
   * are taken off again (what they displaced is put back, a nested firing on the same thread relies on it) within one
   * hold of {@link #runLock}.
   */
  private Value run(final Database database, final Map<String, Object> bindings) throws IOException {
    synchronized (runLock) {
      final GraalPolyglotEngine engine = engine(database);
      final Map<String, Value> displaced = engine.setAttributes(bindings);
      try {
        // Interrupted if the statement firing the trigger is terminated (issue #9689)
        return engine.runTerminable(RunningQuery.current(), "the trigger", () -> engine.eval(script));
      } finally {
        try {
          engine.restoreAttributes(displaced);
        } catch (final RuntimeException e) {
          LogManager.instance().log(this, Level.WARNING, "Error removing the bindings of trigger '%s': %s", e, triggerName,
              e.getMessage());
        }
      }
    }
  }

  /**
   * The script context, created on the first firing (lazy initialization) over the shared GraalVM Engine of
   * {@link PolyglotEngineManager}, for better performance. Built once, whatever the number of threads firing the trigger.
   */
  private GraalPolyglotEngine engine(final Database database) {
    GraalPolyglotEngine engine = scriptEngine;
    if (engine == null) {
      synchronized (engineLock) {
        engine = scriptEngine;
        if (engine == null) {
          engine = GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
              .setLanguage("js")
              .setAllowedPackages(ALLOWED_PACKAGES)
              .build();
          scriptEngine = engine;
        }
      }
    }
    return engine;
  }

  @Override
  public void close() {
    final GraalPolyglotEngine engine;
    synchronized (engineLock) {
      engine = scriptEngine;
      scriptEngine = null;
    }
    if (engine != null) {
      try {
        // Cancels a script still running on the context: closing does not wait for it
        engine.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing script engine for trigger '%s'", e, triggerName);
      }
    }
    // Note: The shared Engine is managed by PolyglotEngineManager and should not be closed here
  }
}
