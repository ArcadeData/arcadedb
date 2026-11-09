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
package com.arcadedb.census;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.event.ServerEventLog;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Never run: compiled only so the control of each census can read the shapes its scan must see, and the ones it must not
 * report, one nested class per census (#11288). The shapes are the ones a source-text census could not see: a call through a
 * local under any name, a method reference, a call split across lines.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ShapeProbes {
  private ShapeProbes() {
  }

  /** {@link NoDefaultLocaleCaseConversionCensusTest}. */
  static final class CaseConversion {
    static String plain(final String keyword) {
      return keyword
          .toUpperCase();
    }

    static List<String> methodReference(final List<String> keywords) {
      final Function<String, String> fold = String::toLowerCase;
      return keywords.stream().map(fold).toList();
    }

    static String withALocale(final String keyword) {
      return keyword.toUpperCase(Locale.ROOT);
    }

    static String namedInALiteral() {
      return "pass a Locale to .toUpperCase()";
    }
  }

  /** {@link ClientsHonourTheMarksCensusTest}: the source-text census saw only a receiver whose name ended in {@code server}. */
  static final class OperatorEntryPoint {
    private final ArcadeDBServer owner;

    OperatorEntryPoint(final ArcadeDBServer owner) {
      this.owner = owner;
    }

    ServerDatabase onAFieldNamedOwner(final String name) {
      return owner.getDatabaseForOperator(name);
    }

    Function<String, ServerDatabase> methodReference() {
      return owner::getOrCreateDatabase;
    }

    @SuppressWarnings("deprecation")
    ServerDatabase deprecated(final String name) {
      return owner.getDatabase(name);
    }

    ServerDatabase forAClient(final String name) {
      return owner.getDatabaseForClient(name);
    }

    ServerDatabase theOverload(final String name) {
      return owner.getDatabase(name, false, false);
    }
  }

  /** {@link NoRawJulLoggersCensusTest}. */
  static final class JulLogger {
    static Logger hoisted() {
      final String name = "probe";
      return Logger
          .getLogger(name);
    }

    static Supplier<Logger> global() {
      return Logger::getGlobal;
    }

    static Logger anonymous() {
      return Logger.getAnonymousLogger();
    }

    static String namedInALiteral() {
      return "never Logger.getLogger(name)";
    }
  }

  /** {@link LogMessageArityCensusTest}: the source-text census inspected only a call spelled {@code LogManager.instance().log(}. */
  static final class LogArity {
    static void oneShortThroughALocal(final Object value) {
      final LogManager log = LogManager.instance();
      log.log(ShapeProbes.class, Level.INFO, "from %s to %s", value);
    }

    static void oneShortAfterAThrowable(final Object a, final Throwable error) {
      LogManager.instance().log(ShapeProbes.class, Level.WARNING,
          "%s, %s and %s", error, a, a);
    }

    static void everyConversionSupplied(final Object a, final Object b) {
      LogManager.instance().log(ShapeProbes.class, Level.INFO, "from %s to %s", a, b);
    }

    static void noConversionAtAll() {
      LogManager.instance().log(ShapeProbes.class, Level.INFO, "100%% done%n");
    }
  }

  /** {@link ServerEventMessageLocaleCensusTest}: the source-text census read only the arguments of the reportEvent call. */
  static final class EventMessage {
    static void builtInAHelper(final ArcadeDBServer server, final double free) {
      server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "Probe", null, freeSpace(free));
    }

    static void concatenated(final ArcadeDBServer server, final double free) {
      server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "Probe", null, "Disk: " + "%.2f GB".formatted(free));
    }

    static void withALocale(final ArcadeDBServer server, final double free) {
      server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "Probe", null, String.format(Locale.ROOT, "%.2f", free));
    }

    private static String freeSpace(final double free) {
      return String.format("%.2f GB free", free);
    }
  }

  /** {@link ReflectionFreeCopyCensusTest}: the source-text census matched only {@code getClass().getConstructor(} on one line. */
  static final class ReflectiveCopy {
    Object throughALocal() throws ReflectiveOperationException {
      final Class<?> type = getClass();
      return type
          .getDeclaredConstructor().newInstance();
    }

    Object aNamedClass() throws ReflectiveOperationException {
      return StringBuilder.class.getConstructor().newInstance();
    }
  }

  /** {@link MissingObjectRefusalCensusTest}. */
  static final class MissingObject {
    static RuntimeException concatenated(final String name) {
      return new CommandExecutionException("Index '" + name +
          "' not found");
    }

    static RuntimeException throughALocal() {
      final String message = "Trigger does not exist";
      return new DatabaseOperationException(message);
    }

    // not offenders: the schema family, and a refusal that names no missing object
    static RuntimeException asASchemaFailure(final String name) {
      return new SchemaException("Index '" + name + "' not found");
    }

    static RuntimeException anotherFailure(final String name) {
      return new CommandExecutionException("Cannot read " + name);
    }
  }
}
