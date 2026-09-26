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
package com.arcadedb.log;

/**
 * Sends every record to a second destination, on top of the {@link Logger} the configuration chose.
 *
 * <p>Discovered with {@link java.util.ServiceLoader}, so the engine keeps no dependency on whatever
 * does the sending - the same arrangement the optional metrics and tracing plugins use. An
 * implementation lives in its own module and is only on the classpath when someone wants it.
 *
 * <p>It <strong>decorates</strong> rather than replaces: {@link #create} is handed the logger already
 * selected by {@code arcadedb.log.impl} and returns one that writes to both. Choosing where logs are
 * exported and choosing how they are written locally are separate decisions, so exporting does not
 * cost you the console.
 *
 * @author Rui Pereira
 */
public interface LogExporter {

  /**
   * The name this exporter answers to, matched against the configured destination.
   *
   * @return the name, for example {@code otlp}
   */
  String name();

  /**
   * Builds a logger that writes to {@code delegate} and exports as well.
   *
   * <p>May throw: {@link LogManager} treats a failure here as "export is not available", reports it on
   * {@code System.err} and keeps the undecorated logger, because a telemetry problem must never stop a
   * database from logging - or from starting.
   *
   * @param delegate the logger the configuration selected; never null, and must still receive every record
   * @param endpoint where to export to
   *
   * @return the decorating logger; never null
   */
  Logger create(Logger delegate, String endpoint);
}
