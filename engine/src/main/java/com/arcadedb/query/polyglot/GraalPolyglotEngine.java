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

import java.io.*;
import java.util.*;
import java.util.logging.*;

/**
 * Polyglot script engine based on GraalVM.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraalPolyglotEngine implements AutoCloseable {
  public final Database     database;
  public final String       language;
  public final List<String> allowedPackages;
  public final List<String> restrictedPackages;
  public final Context      context;

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
            allowHostAccess(HostAccess.ALL).//
            allowIO(true).//
            allowNativeAccess(false).//
            allowCreateProcess(false).//
            allowEnvironmentAccess(EnvironmentAccess.NONE).//
            allowCreateThread(false).//
            allowPolyglotAccess(PolyglotAccess.ALL).//
            allowHostClassLookup((s) -> allowedPackages.stream().map(e -> s.matches(e)).filter(f -> f).findFirst().isPresent());

    if (output != null)
      builder.out(output);
    else {
      // IGNORE THE OUTPUT
      builder.out(new OutputStream() {
        @Override
        public void write(int b) {
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
    } catch (Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on closing scrpit context", e);
    }
  }

  public Set<String> getSupportedLanguages() {
    return context.getEngine().getLanguages().keySet();
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

  public Value eval(final String script) throws Exception {
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

    if (msg.contains("invalid literal"))
      return true;

    return false;
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
      String lineCol = msg.substring(posSrc + "src.js:".length(), pos);

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
