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
package com.arcadedb.server.grpc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7472: every gRPC failure this module answers with must go through the concealment-aware mapping, and a new
 * one that does not must fail the build rather than leak quietly.
 * <p>
 * The concealment-less overloads of {@link GrpcErrorMapper#toStatusRuntimeException} still exist - the tests use
 * them, and they are the honest spelling for a caller that has already decided - and they default {@code conceal}
 * to false. That is exactly what makes a missed call site dangerous: it COMPILES, it PASSES every behavioural test
 * (it answers the right status with the right trailers), and the only thing wrong with it is that it puts the
 * exception's own message on the wire in production. No behavioural test can see that, because the test server is
 * not in production mode.
 * <p>
 * So this is a source-level guard rather than a behavioural one, in the spirit of
 * {@code RestoreSettingsTest.restoreThreadBoundMatchesTheGlobalConfiguration}: a comment asking the next editor to
 * use {@code mapError(...)} is not enforcement, and this is. It fails the moment a production call site in this
 * module maps a failure without stating a concealment decision.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7472EveryGrpcFailureIsConcealableTest {

  private static final Path   MAIN_SOURCES = Path.of("src", "main", "java", "com", "arcadedb", "server", "grpc");
  /**
   * Both mapper entry points a production call site can answer a failure through. {@code toStatusException} was
   * added on main while this branch was open and needs the same guard: it delegates to
   * {@code toStatusRuntimeException}, so a call that omits the decision leaks exactly the same way.
   */
  private static final List<String> MAPPER_CALLS = List.of("GrpcErrorMapper.toStatusRuntimeException(",
      "GrpcErrorMapper.toStatusException(");
  /**
   * What a concealment-aware call site passes as its last argument.
   * <p>
   * Two spellings, both honest. {@code concealErrors()} is what a service's own {@code mapError(...)} passes, and
   * {@code conceal} is what a STATIC helper takes as a parameter because it has no instance to ask - {@code
   * graphBatchLoadError} is the current example, and its caller resolves the flag from {@code concealErrors()}
   * once, up front, because the error paths run on the stream's own threads.
   * <p>
   * What neither spelling permits is a call that simply omits the argument, which is the whole failure mode: it
   * compiles, it answers the right status with the right trailers, and it leaks only in production.
   */
  private static final List<String> DECISIONS = List.of("concealErrors()", "conceal)", "conceal,");

  @Test
  void noProductionCallSiteMapsAFailureWithoutAConcealmentDecision() throws IOException {
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        // The mapper itself is where the overloads are DEFINED and where one delegates to another; it is the one
        // file whose calls are not call sites in this sense.
        if (source.getFileName().toString().equals("GrpcErrorMapper.java"))
          continue;

        for (final String statement : statementsCalling(Files.readString(source, StandardCharsets.UTF_8)))
          if (DECISIONS.stream().noneMatch(statement::contains))
            offenders.add(source.getFileName() + ": " + statement.strip());
      }
    }

    assertThat(offenders)
        .as("every gRPC failure must be mapped through a call that states whether to conceal - use this service's "
            + "mapError(...) helper, which passes concealErrors(), or thread a 'conceal' parameter from it")
        .isEmpty();
  }

  /**
   * Guards the guard: the scan has to actually find something, or a rename of the mapper - or a wrong source root -
   * would turn this test green by finding nothing at all, which is the vacuous pass this class exists to avoid.
   */
  @Test
  void theScanFindsTheCallSitesItIsMeantToCheck() throws IOException {
    assertThat(MAIN_SOURCES).as("the module's own sources, relative to the module directory Surefire runs in")
        .isDirectory();

    int found = 0;
    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        if (source.getFileName().toString().equals("GrpcErrorMapper.java"))
          continue;
        found += statementsCalling(Files.readString(source, StandardCharsets.UTF_8)).size();
      }
    }

    assertThat(found).as("the two services each map through one helper, so there is at least one call to find")
        .isGreaterThanOrEqualTo(2);
  }

  /**
   * Every statement in {@code source} that calls the mapper, from the call to the {@code ;} that ends it - so a call
   * whose arguments are wrapped across lines is judged whole rather than on its first line.
   */
  private static List<String> statementsCalling(final String source) {
    final List<String> statements = new ArrayList<>();
    for (final String call : MAPPER_CALLS)
      for (int at = source.indexOf(call); at > -1; at = source.indexOf(call, at + 1)) {
        // Not a call: the class name appearing inside a comment or a Javadoc reference.
        final int lineStart = source.lastIndexOf('\n', at) + 1;
        final String before = source.substring(lineStart, at).strip();
        if (before.startsWith("//") || before.startsWith("*") || before.startsWith("/*"))
          continue;

        final int end = source.indexOf(';', at);
        statements.add(source.substring(at, end > -1 ? end : source.length()));
      }
    return statements;
  }
}
