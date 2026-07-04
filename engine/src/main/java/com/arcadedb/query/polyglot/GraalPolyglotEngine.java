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
package com.arcadedb.query.polyglot;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.EnvironmentAccess;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

/**
 * Polyglot script engine based on GraalVM.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraalPolyglotEngine implements AutoCloseable {
  public final         Database            database;
  public final         String              language;
  public final         List<String>        allowedPackages;
  public final         List<String>        restrictedPackages;
  public final         Context             context;
  private static volatile Set<String>      supportedLanguages;

  /**
   * Host-access policy for scripts. It starts from {@link HostAccess#ALL} (so the bound {@code database} object's public
   * API is usable from a script) but denies the reflection surface that would let a script escape the {@code allowedPackages}
   * whitelist by walking from an already-bound Java object to arbitrary classes - e.g.
   * {@code database.getClass().getClassLoader().loadClass("java.io.File")} (GHSA-48qw-824m-86pr). Denying
   * {@link Class}, {@link ClassLoader} and the {@link java.lang.reflect} member types (with their subclasses) closes every
   * step of that chain - {@code Class.getClassLoader()}, {@code Class.forName()}, {@code Class.getDeclaredConstructor()},
   * {@code ClassLoader.loadClass()} and {@code Method/Constructor/Field} reflective invocation - while leaving normal host
   * method calls on bound objects untouched. Explicit {@code Java.type(...)} lookups remain governed by
   * {@code allowHostClassLookup}/{@code allowedPackages}.
   */
  private static final HostAccess SANDBOXED_HOST_ACCESS = HostAccess.newBuilder(HostAccess.ALL)//
      .denyAccess(Class.class)//
      .denyAccess(ClassLoader.class)//
      .denyAccess(java.lang.reflect.AccessibleObject.class)//
      .denyAccess(java.lang.reflect.Member.class)//
      .build();

  private GraalPolyglotEngine(final Database database, final Engine engine, final String language, final OutputStream output,
      final List<String> allowedPackages, final List<String> restrictedPackages, final long maxExecutionTimeMs) {
    this.database = database;
    this.language = language;
    this.allowedPackages = allowedPackages == null ? Collections.emptyList() : allowedPackages;
    this.restrictedPackages = restrictedPackages;

    // DISABLED LIMIT BECAUSE THE CONTEXT IS INVOKED MULTIPLE TIMES
    //final ResourceLimits limits = ResourceLimits.newBuilder().statementLimit(10000, null).build();

    //final HostAccess hostAccess = HostAccess.newBuilder(HostAccess.ALL).targetTypeMapping(Double.class, Float.class, null, x -> x.floatValue()).build();

    final Context.Builder builder = Context.newBuilder().engine(engine).//
        //resourceLimits(limits).//
            allowHostAccess(SANDBOXED_HOST_ACCESS).//
            // IOAccess.NONE: deny built-in file/URL access so a script cannot use load(path|url) to read
            // host files or perform SSRF (e.g. fetching cloud metadata), which IOAccess.ALL permitted even
            // when host-class lookup was locked down (GHSA-vwjc-v7x7-cm6g / GHSA-48qw hardening).
            allowIO(IOAccess.NONE).//
            allowNativeAccess(false).//
            allowCreateProcess(false).//
            allowEnvironmentAccess(EnvironmentAccess.NONE).//
            allowCreateThread(false).//
            // PolyglotAccess.NONE: no cross-language eval, so a script cannot pivot into another GraalVM
            // language that might be on the classpath to escape this language's sandbox.
            allowPolyglotAccess(PolyglotAccess.NONE).//
            allowHostClassLookup(
            s -> this.allowedPackages.stream().map(e -> s.matches(e)).filter(f -> f).findFirst().isPresent());

    if (output != null)
      builder.out(output);
    else {
      // IGNORE THE OUTPUT
      builder.out(new OutputStream() {
        @Override
        public void write(final int b) {
        }
      });
    }

    context = builder.build();

    Value bindings = context.getBindings(language);

    bindings.putMember("database", database);

    bindings = context.getPolyglotBindings();
    bindings.putMember("database", database);
  }

  public static Builder newBuilder(final Database database, final Engine sharedEngine) {
    return new Builder(database, sharedEngine);
  }

  @Override
  public void close() {
    try {
      context.close(true);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on closing script context", e);
    }
  }

  public static Set<String> getSupportedLanguages() {
    Set<String> result = supportedLanguages;
    if (result == null) {
      synchronized (GraalPolyglotEngine.class) {
        result = supportedLanguages;
        if (result == null) {
          try {
            // Touching the shared Engine here loads Truffle and every GraalVM language jar on the
            // classpath. Done lazily so a server that never uses polyglot does not pay this cost.
            result = PolyglotEngineManager.getInstance().getSharedEngine().getLanguages().keySet();
          } catch (Throwable e) {
            LogManager.instance().log(GraalPolyglotEngine.class, Level.WARNING, "GraalVM Polyglot Engine: no languages found");
            result = Collections.emptySet();
          }
          supportedLanguages = result;
        }
      }
    }
    return result;
  }

  public static final class Builder {

    private final Database     database;
    private final Engine       engine;
    private       OutputStream output;
    private       List<String> allowedPackages;
    private       List<String> restrictedPackages;
    private       String       language           = "js";
    private       long         maxExecutionTimeMs = 1;

    protected Builder(final Database database, final Engine sharedEngine) {
      this.database = database;
      this.engine = sharedEngine;
    }

    public Builder setLanguage(final String language) {
      if (language != null) {
        this.language = language;
      }
      return this;
    }

    public Builder setMaxExecutionTimeMs(final long maxExecutionTimeMs) {
      this.maxExecutionTimeMs = maxExecutionTimeMs;
      return this;
    }

    public Builder setOutput(final OutputStream output) {
      this.output = output;
      return this;
    }

    public Builder setAllowedPackages(final List<String> allowedPackages) {
      this.allowedPackages = allowedPackages;
      return this;
    }

    public Builder setRestrictedPackages(final List<String> restrictedPackages) {
      this.restrictedPackages = restrictedPackages;
      return this;
    }

    public GraalPolyglotEngine build() {
      return new GraalPolyglotEngine(database, engine, language, output, allowedPackages, restrictedPackages, maxExecutionTimeMs);
    }
  }

  public Value eval(final String script) throws IOException {
    final Source source;
    source = Source.newBuilder(language, script, "src." + language).build();

    synchronized (this) {
      return context.eval(source);
    }
  }

  public void setAttribute(final String name, final Object value) {
    context.getBindings(language).putMember(name, value);
    context.getPolyglotBindings().putMember(name, value);
  }

  public static boolean isCriticalError(final PolyglotException e) {
    final String msg = e.getMessage();

    if (msg == null)
      return true;

    if (msg.startsWith("SyntaxError:"))
      return true;

    if (msg.startsWith("NameError:"))
      return true;

    if (msg.startsWith("TypeError:"))
      return true;

    if (msg.startsWith("ReferenceError:"))
      return true;

    return msg.contains("invalid literal");
  }

  public static String endUserMessage(final Throwable e, final boolean includePosition) {
    if (e == null)
      return "no message";

    String msg = e.getMessage();
    if (msg == null)
      return e.toString();

    final int posSrc = msg.indexOf("src.js:");
    if (posSrc > -1) {
      // STRIP SRC.JS

      final int pos = msg.indexOf(" ", posSrc + "src.js:".length());
      final String lineCol = msg.substring(posSrc + "src.js:".length(), pos);

      final String[] lineColPair = lineCol.split(":");

      if (includePosition)
        msg = msg.substring(0, posSrc) + "Line " + lineColPair[0] + " Column " + lineColPair[1] + ":" + msg.substring(pos);
      else
        msg = msg.substring(0, posSrc) + ":" + msg.substring(pos);
    }

    String function = null;
    final int pos = msg.indexOf("JavaObject[com.arcadedb.polyglot.");
    if (pos > -1) {
      final int pos2 = msg.indexOf("@", "JavaObject[com.arcadedb.polyglot.".length() + 1);
      if (pos2 > -1) {
        function = msg.substring(pos, pos2);
      }
    }

    String identifier = null;
    final int pos3 = msg.indexOf("Unknown identifier: ");
    if (pos3 > -1) {
      final int pos4 = msg.indexOf(" ", pos3 + "Unknown identifier: ".length());
      if (pos4 > -1)
        identifier = msg.substring(pos3, pos4);
      else
        identifier = msg.substring(pos3);
    }

    if (msg.startsWith("TypeError:")) {
      if (function != null && identifier != null)
        msg = "Type Error: " + function + "." + identifier;
      else if (identifier != null)
        msg = "Type Error: " + identifier;
    }

    return msg;
  }
}
