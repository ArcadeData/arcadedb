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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7472: every gRPC failure this module answers with must state a concealment decision, and a new one that
 * does not must fail the build rather than leak quietly.
 * <p>
 * "Every failure" means through EITHER channel, which is the correction this class needed: a failure reaches a
 * client as a gRPC {@code Status}, or - for {@code insertStream} and {@code insertBidirectional} - as an
 * {@code InsertError} field inside a normal response message. The first three scans below only ever knew about the
 * first, and this class claimed to cover both while the protobuf channel leaked raw exception text on the two RPCs
 * that use it (PR #7755 review).
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
   * A description interpolating a THROWABLE's own message - {@code e.getMessage()}, {@code cause.getMessage()}.
   * Not {@code request.getX()}, which is the caller's own input echoed back.
   */
  private static final Pattern MESSAGE_OF_A_THROWABLE =
      Pattern.compile("\\b(e|ex|t|cause|error|throwable)\\.getMessage\\(\\)");

  /**
   * The shapes that mean "this service does not know what this failure is": the classification of last resort, and
   * the status it produces for one. That is where ENGINE text lands, and engine text is what must not reach a
   * client in production.
   * <p>
   * The guard is deliberately not broader. A per-outcome arm - {@code PERMISSION_DENIED} for a security refusal,
   * {@code NOT_FOUND} for a user that does not exist - also reads {@code e.getMessage()}, but that message is a
   * sentence ArcadeDB wrote ABOUT THE REQUEST, and it is what makes the refusal actionable; the HTTP body keeps
   * exactly those in production too, in its {@code error} field. No syntactic rule can tell the two apart, so this
   * asks the question it CAN answer precisely rather than a broader one it would answer wrongly.
   */
  private static final List<String> CATCH_ALL_SHAPES = List.of("GrpcErrorMapper.statusCodeFor(", "Status.INTERNAL");

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
   * The OTHER way a description reaches a client: a status built DIRECTLY, keeping a code this service chose rather
   * than one the mapper would classify. Those never touch {@code GrpcErrorMapper}, so the scan above cannot see
   * them - and three of them were putting {@code e.getMessage()} on the wire in production while every mapped
   * failure was concealed (PR #7755 review).
   * <p>
   * The rule is {@link #CATCH_ALL_SHAPES} narrow on purpose - see there for why a broader one would be wrong
   * rather than merely stricter.
   */
  @Test
  void noDirectStatusMappingPutsAThrowablesMessageOnTheWire() throws IOException {
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList())
        for (final String statement : statementsMatching(Files.readString(source, StandardCharsets.UTF_8),
            ".withDescription("))
          if (MESSAGE_OF_A_THROWABLE.matcher(statement).find()
              && CATCH_ALL_SHAPES.stream().anyMatch(statement::contains)
              && DECISIONS.stream().noneMatch(statement::contains))
            offenders.add(source.getFileName() + ": " + statement.strip().replaceAll("\\s+", " "));
    }

    assertThat(offenders)
        .as("a status built directly from a throwable's message must conceal it in production - route it through "
            + "concealable(...), which also writes the log entry the concealed text promises")
        .isEmpty();
  }

  /**
   * Concealing is only half of it: the concealed text tells the operator to check the server log, so whatever
   * conceals must also be what WRITES that entry. This asks that pairing directly - a method that uses
   * {@link GrpcErrorMapper#CONCEALED_DESCRIPTION} must also call {@code logConcealed}.
   * <p>
   * The gap this closes was invisible to the scans above and is why they are not enough on their own:
   * {@code ArcadeDbGrpcAdminService}'s catch-all DID call {@code concealErrors()}, so it read as a stated decision,
   * but it used the answer only to pick text. It concealed the failure from the client and logged nothing anywhere
   * - worse than not concealing, because then nobody had the detail (PR #7755 review).
   */
  @Test
  void everyMethodThatConcealsAlsoLogs() throws IOException {
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        final String text = Files.readString(source, StandardCharsets.UTF_8);
        for (int at = text.indexOf("CONCEALED_DESCRIPTION"); at > -1;
            at = text.indexOf("CONCEALED_DESCRIPTION", at + 1)) {
          // The declaration itself, and Javadoc mentions of it, are not uses.
          final int lineStart = text.lastIndexOf('\n', at) + 1;
          final String line = text.substring(lineStart, text.indexOf('\n', at) < 0 ? text.length() : text.indexOf('\n', at));
          if (line.strip().startsWith("*") || line.contains("static final String CONCEALED_DESCRIPTION"))
            continue;

          if (!enclosingMethod(text, at).contains("logConcealed("))
            offenders.add(source.getFileName() + ": " + line.strip());
        }
      }
    }

    assertThat(offenders)
        .as("a method that conceals must also write the log entry the concealed text promises - otherwise the "
            + "detail is not hidden from the client, it is destroyed")
        .isEmpty();
  }

  /**
   * The body of the method containing {@code at}, found by walking back to the nearest line that starts a member
   * declaration at class-member indentation. Good enough for this module's formatting, and the test that guards
   * the guard fails if it ever stops finding anything.
   */
  private static String enclosingMethod(final String text, final int at) {
    int start = 0;
    for (final java.util.regex.MatchResult m : Pattern.compile("(?m)^  (?:public|protected|private|static|final| )*[\\w<>,\\[\\]?. ]+\\([^)]*\\)[^;{]*\\{")
        .matcher(text).results().toList()) {
      if (m.start() > at)
        break;
      start = m.start();
    }
    final int end = text.indexOf("\n  }", at);
    return text.substring(start, end > -1 ? end : text.length());
  }

  /**
   * The OTHER response channel: {@code InsertError}'s free-form {@code message}, which is a protobuf field inside a
   * successful response rather than a {@code Status}, and so touches none of the machinery the scans above check.
   * <p>
   * Every free-form insert message goes through {@code insertErrorMessage(...)}, which conceals and logs. A raw
   * {@code .getMessage()} feeding an error row is what this refuses: that was a {@link DuplicatedKeyException}'s
   * index name and offending KEY VALUE reaching a production client verbatim while every status-borne failure was
   * concealed.
   */
  @Test
  void noInsertErrorCarriesARawExceptionMessage() throws IOException {
    final List<String> offenders = new ArrayList<>();

    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList())
        for (final String token : List.of("InsertError.newBuilder(", ".err("))
          for (final String statement : statementsMatching(Files.readString(source, StandardCharsets.UTF_8), token))
            if (MESSAGE_OF_A_THROWABLE.matcher(statement).find())
              offenders.add(source.getFileName() + ": " + statement.strip().replaceAll("\\s+", " "));
    }

    assertThat(offenders)
        .as("an InsertError's message must come from insertErrorMessage(...), which conceals and logs it - a raw "
            + "getMessage() here reaches a production client through a channel no Status concealment covers")
        .isEmpty();
  }

  /**
   * Guards the guards: EVERY scan above has to actually find the constructs it judges, or a rename, a reformat or a
   * wrong source root turns it green by finding nothing at all - which is the vacuous pass this class exists to
   * avoid, and the specific fragility a source-level test carries (PR #7755 review).
   * <p>
   * One assertion per scan, each naming what it expects to exist, so the failure says which scan went blind rather
   * than only that something did.
   */
  @Test
  void everyScanFindsTheConstructsItJudges() throws IOException {
    assertThat(MAIN_SOURCES).as("the module's own sources, relative to the module directory Surefire runs in")
        .isDirectory();

    final Map<String, Integer> found = new LinkedHashMap<>();
    try (final Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        final String text = Files.readString(source, StandardCharsets.UTF_8);
        final boolean mapper = source.getFileName().toString().equals("GrpcErrorMapper.java");

        if (!mapper)
          found.merge("mapper calls", statementsCalling(text).size(), Integer::sum);
        found.merge("directly-built statuses", statementsMatching(text, ".withDescription(").size(), Integer::sum);
        found.merge("InsertError rows", statementsMatching(text, "InsertError.newBuilder(").size(), Integer::sum);
        found.merge("concealed descriptions",
            mapper ? 0 : text.split("CONCEALED_DESCRIPTION", -1).length - 1, Integer::sum);
      }
    }

    found.forEach((what, count) -> assertThat(count)
        .as("the scan for %s found nothing, so it is asserting about nothing - has the construct been renamed?", what)
        .isPositive());
  }

  /**
   * Every statement in {@code source} that calls the mapper, from the call to the {@code ;} that ends it - so a call
   * whose arguments are wrapped across lines is judged whole rather than on its first line.
   */
  private static List<String> statementsCalling(final String source) {
    final List<String> statements = new ArrayList<>();
    for (final String call : MAPPER_CALLS)
      statements.addAll(statementsMatching(source, call));
    return statements;
  }

  /**
   * Every statement in {@code source} containing {@code token}, from the start of the line it begins on to the
   * {@code ;} that ends it - so a call whose arguments wrap across lines is judged whole, and a token appearing
   * MID-statement (as {@code .withDescription(} does) is judged with what precedes it too.
   */
  private static List<String> statementsMatching(final String source, final String token) {
    final List<String> statements = new ArrayList<>();
    for (int at = source.indexOf(token); at > -1; at = source.indexOf(token, at + 1)) {
      // Not a call: the token appearing inside a comment or a Javadoc reference.
      final int lineStart = source.lastIndexOf('\n', at) + 1;
      final String before = source.substring(lineStart, at).strip();
      if (before.startsWith("//") || before.startsWith("*") || before.startsWith("/*"))
        continue;

      final int end = source.indexOf(';', at);
      statements.add(source.substring(lineStart, end > -1 ? end : source.length()));
    }
    return statements;
  }
}
