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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
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
      "GrpcErrorMapper.toStatusException(",
      // concealableDescription takes the decision as its last argument and RETURNS THE THROWABLE'S MESSAGE when it
      // is false, so a direct call passing a literal false leaks exactly what #7472 is about - and it slipped
      // through both other scans, because SAFE_DESCRIPTIONS strips this call whole before looking for a raw
      // message (PR #7942 review). Listing it here makes it state a decision like any other mapper entry point:
      // concealErrors() and a threaded `conceal` parameter pass, a literal false does not.
      "GrpcErrorMapper.concealableDescription(");
  /**
   * ANY receiver's {@code .getMessage()}.
   * <p>
   * This used to be an allow-list of six variable spellings - {@code \\b(e|ex|t|cause|error|throwable)} - and that
   * is a rule an allow-list cannot express. The defect the two scans below were WRITTEN for lived in
   * {@code catch (DuplicatedKeyException dup)}, {@code catch (DuplicatedKeyException retryDup)} and
   * {@code catch (Exception retryEx)}; none of {@code dup}, {@code retryDup}, {@code retryEx} is in that list, so
   * the guard would have caught 2 of the 6 sites it exists for and walked past the four that leaked the index name
   * and the offending KEY VALUE. The next site is as likely as not to be written {@code catch (ValidationException
   * ve)} (issue #7911).
   * <p>
   * So the question is asked the other way round, and the whole rule is in the two SAFE_* lists below: a statement
   * is judged by whether its free text comes out of the ONE call that conceals and logs, with that call's own
   * arguments taken out of the picture first. "Spot anything that is not the safe call" is exact where "spot every
   * unsafe spelling" can only ever be a list someone has to remember to extend.
   * <p>
   * Every {@code .getMessage()} in this module today is a throwable's. If a protobuf message ever grows a
   * {@code message} FIELD, its getter goes in {@link #NOT_A_THROWABLE} rather than this pattern going back to
   * guessing from the receiver's name.
   */
  private static final Pattern ANY_GET_MESSAGE = Pattern.compile("\\.getMessage\\s*\\(\\s*\\)");

  /**
   * Receivers whose {@code getMessage()} is NOT a throwable's - the caller's own input echoed back, which is safe
   * and which {@link #ANY_GET_MESSAGE} would otherwise flag. Empty today: this module has no protobuf message with
   * a {@code message} field. It exists so that adding one is a one-line, reviewed exception instead of a reason to
   * loosen the pattern.
   */
  private static final List<String> NOT_A_THROWABLE = List.of();

  /**
   * The one call an {@code InsertError}'s free text may come from. {@code insertErrorMessage(...)} conceals in
   * production and writes the log entry the concealed text promises.
   */
  private static final String SAFE_INSERT_ERROR_MESSAGE = "insertErrorMessage(";

  /**
   * The calls a directly-built catch-all {@code Status}'s description may come from. {@code concealable(...)} is
   * the service-side helper; {@code GrpcErrorMapper.concealableDescription(...)} is what it delegates to, which a
   * static helper with no instance to ask calls itself.
   */
  private static final List<String> SAFE_DESCRIPTIONS = List.of("concealable(", "GrpcErrorMapper.concealableDescription(");

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

        offenders.addAll(mapperCallsWithoutADecision(source.getFileName().toString(),
            Files.readString(source, StandardCharsets.UTF_8)));
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
        offenders.addAll(directStatusesCarryingARawMessage(source.getFileName().toString(),
            Files.readString(source, StandardCharsets.UTF_8)));
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
      for (final Path source : sources.filter(p -> p.toString().endsWith(".java")).toList())
        offenders.addAll(concealmentsWithoutALogEntry(source.getFileName().toString(),
            Files.readString(source, StandardCharsets.UTF_8)));
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
    for (final MatchResult m : Pattern.compile("(?m)^  (?:public|protected|private|static|final| )*[\\w<>,\\[\\]?. ]+\\([^)]*\\)[^;{]*\\{")
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
        offenders.addAll(insertErrorsCarryingARawMessage(source.getFileName().toString(),
            Files.readString(source, StandardCharsets.UTF_8)));
    }

    assertThat(offenders)
        .as("an InsertError's message must come from insertErrorMessage(...), which conceals and logs it - a raw "
            + "getMessage() here reaches a production client through a channel no Status concealment covers")
        .isEmpty();
  }

  /**
   * Guards the guards, half one: every scan has to actually FIND the constructs it judges, or a rename, a reformat
   * or a wrong source root turns it green by finding nothing at all - which is the vacuous pass this class exists
   * to avoid, and the specific fragility a source-level test carries (PR #7755 review).
   * <p>
   * One assertion per scan, each naming what it expects to exist, so the failure says which scan went blind rather
   * than only that something did.
   * <p>
   * Finding statements is NOT enough on its own, which is what {@link #everyScanFlagsAKnownBadStatement} adds: the
   * InsertError scan found 13 statements - "not blind" by this measure - while its judgement recognised none of
   * the four spellings that actually leaked (issue #7911).
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
   * Guards the guards, half two: every scan's JUDGEMENT still fires.
   * <p>
   * One known-bad fixture per scan, each written in a spelling the previous version of this class did NOT
   * recognise, so a rule that stops recognising anything fails loudly instead of passing quietly on a clean tree.
   * The receiver names used here - dup, retryEx, retryDup - are the ones from the leak the guard was written for
   * and could not see.
   */
  @Test
  void everyScanFlagsAKnownBadStatement() {
    assertThat(mapperCallsWithoutADecision("Fixture.java",
        "    throw GrpcErrorMapper.toStatusRuntimeException(dup, \"insert\");"))
        .as("the mapper-call scan no longer flags a call that states no concealment decision")
        .hasSize(1);

    assertThat(directStatusesCarryingARawMessage("Fixture.java",
        "    return GrpcErrorMapper.statusCodeFor(retryEx).toStatus().withDescription(retryEx.getMessage()).asException();"))
        .as("the direct-status scan no longer flags a catch-all status built from a throwable's own message")
        .hasSize(1);

    assertThat(concealmentsWithoutALogEntry("Fixture.java",
        "  private String describe(final Throwable dup) {\n"
            + "    return GrpcErrorMapper.CONCEALED_DESCRIPTION;\n"
            + "  }\n"))
        .as("the conceal-must-log scan no longer flags a method that conceals without logging")
        .hasSize(1);

    assertThat(insertErrorsCarryingARawMessage("Fixture.java",
        "        c.err(c.received - 1, \"CONFLICT\", retryDup.getMessage(), \"\");"))
        .as("the InsertError scan no longer flags a raw exception message on the protobuf channel")
        .hasSize(1);

    assertThat(mapperCallsWithoutADecision("Fixture.java",
        "    return GrpcErrorMapper.concealableDescription(this, op, e, false);"))
        .as("a concealment decision hard-coded to false is a decision to LEAK, and the scan must say so")
        .hasSize(1);

    // ...and each of them accepts the SAFE spelling of the same statement, or the rule is simply "refuse
    // everything", which fails the build on correct code and gets deleted rather than obeyed.
    assertThat(mapperCallsWithoutADecision("Fixture.java",
        "    throw GrpcErrorMapper.toStatusRuntimeException(dup, \"insert\", concealErrors());")).isEmpty();
    assertThat(mapperCallsWithoutADecision("Fixture.java",
        "    return GrpcErrorMapper.concealableDescription(this, op, e, concealErrors());")).isEmpty();
    assertThat(directStatusesCarryingARawMessage("Fixture.java",
        "    return GrpcErrorMapper.statusCodeFor(retryEx).toStatus().withDescription(concealable(op, retryEx)).asException();"))
        .isEmpty();
    assertThat(insertErrorsCarryingARawMessage("Fixture.java",
        "        c.err(c.received - 1, \"CONFLICT\", insertErrorMessage(retryDup), \"\");")).isEmpty();

    // an unbalanced parenthesis INSIDE the safe call's own string argument must not swallow what follows it,
    // or the raw message after it disappears from the text the rule is applied to
    assertThat(insertErrorsCarryingARawMessage("Fixture.java",
        "        c.err(0, \"CONFLICT\", insertErrorMessage(dup) + \"a (partial\" + retryEx.getMessage(), \"\");"))
        .hasSize(1);

    // ...and a .getMessage() that is only MENTIONED in a string or a comment is not a call site
    assertThat(insertErrorsCarryingARawMessage("Fixture.java",
        "        c.err(0, \"CONFLICT\", insertErrorMessage(dup), \"use e.getMessage() instead\");")).isEmpty();

    // a semicolon inside a string argument does not end the statement, or everything past it - the raw message
    // included - would fall outside the text the rule is applied to
    assertThat(insertErrorsCarryingARawMessage("Fixture.java",
        "        c.err(0, \"a; b\", retryEx.getMessage(), \"\");")).hasSize(1);
  }

  /** A mapper call that does not state whether to conceal. */
  private static List<String> mapperCallsWithoutADecision(final String fileName, final String text) {
    final List<String> offenders = new ArrayList<>();
    for (final String statement : statementsCalling(text))
      if (DECISIONS.stream().noneMatch(statement::contains))
        offenders.add(fileName + ": " + statement.strip());
    return offenders;
  }

  /**
   * A catch-all {@code Status} built DIRECTLY whose description carries a throwable's own message from anywhere
   * other than {@link #SAFE_DESCRIPTIONS}. The safe calls' own arguments are removed before looking, so
   * {@code concealable("op", e)} is judged on what is left rather than on what it was handed.
   */
  private static List<String> directStatusesCarryingARawMessage(final String fileName, final String text) {
    final List<String> offenders = new ArrayList<>();
    for (final String statement : statementsMatching(text, ".withDescription("))
      if (CATCH_ALL_SHAPES.stream().anyMatch(statement::contains) && carriesARawMessage(statement, SAFE_DESCRIPTIONS))
        offenders.add(fileName + ": " + statement.strip().replaceAll("\\s+", " "));
    return offenders;
  }

  /** A method that uses {@code CONCEALED_DESCRIPTION} without calling {@code logConcealed}. */
  private static List<String> concealmentsWithoutALogEntry(final String fileName, final String text) {
    final List<String> offenders = new ArrayList<>();
    for (int at = text.indexOf("CONCEALED_DESCRIPTION"); at > -1; at = text.indexOf("CONCEALED_DESCRIPTION", at + 1)) {
      // The declaration itself, and Javadoc mentions of it, are not uses.
      final int lineStart = text.lastIndexOf('\n', at) + 1;
      final String line = text.substring(lineStart, text.indexOf('\n', at) < 0 ? text.length() : text.indexOf('\n', at));
      if (line.strip().startsWith("*") || line.contains("static final String CONCEALED_DESCRIPTION"))
        continue;

      if (!enclosingMethod(text, at).contains("logConcealed("))
        offenders.add(fileName + ": " + line.strip());
    }
    return offenders;
  }

  /**
   * An {@code InsertError} row whose free text comes from anywhere other than
   * {@link #SAFE_INSERT_ERROR_MESSAGE}.
   */
  private static List<String> insertErrorsCarryingARawMessage(final String fileName, final String text) {
    final List<String> offenders = new ArrayList<>();
    for (final String token : List.of("InsertError.newBuilder(", ".err("))
      for (final String statement : statementsMatching(text, token))
        if (carriesARawMessage(statement, List.of(SAFE_INSERT_ERROR_MESSAGE)))
          offenders.add(fileName + ": " + statement.strip().replaceAll("\\s+", " "));
    return offenders;
  }

  /**
   * Whether {@code statement} reads a throwable's own message anywhere OTHER than inside one of {@code safeCalls}.
   * <p>
   * That is the inversion issue #7911 asked for: the arguments of the one call that conceals and logs are taken
   * out first, and anything still reading {@code .getMessage()} in what remains is on the wire raw - whatever the
   * variable happens to be called.
   */
  private static boolean carriesARawMessage(final String statement, final List<String> safeCalls) {
    String remaining = statement;
    for (final String safeCall : safeCalls)
      remaining = withoutCallsTo(remaining, safeCall);

    final boolean[] isCode = codeMask(remaining);
    final Matcher matcher = ANY_GET_MESSAGE.matcher(remaining);
    while (matcher.find()) {
      if (!isCode[matcher.start()])
        continue;

      final String before = remaining.substring(0, matcher.start());
      if (NOT_A_THROWABLE.stream().noneMatch(before::endsWith))
        return true;
    }
    return false;
  }

  /**
   * {@code text} with every {@code call(...)} occurrence and its BALANCED argument list removed, so what the safe
   * call was handed is not mistaken for what the statement puts on the wire.
   */
  private static String withoutCallsTo(final String text, final String call) {
    final boolean[] isCode = codeMask(text);

    final StringBuilder result = new StringBuilder(text.length());
    int from = 0;
    for (int at = text.indexOf(call); at > -1; at = text.indexOf(call, from)) {
      if (!isCode[at]) {
        // the call NAME itself sits in a string or a comment, so it is not a call
        result.append(text, from, at + call.length());
        from = at + call.length();
        continue;
      }
      result.append(text, from, at);

      int depth = 0;
      int i = at + call.length() - 1;
      for (; i < text.length(); i++) {
        if (!isCode[i])
          continue;
        if (text.charAt(i) == '(')
          ++depth;
        else if (text.charAt(i) == ')' && --depth == 0)
          break;
      }
      from = i < text.length() ? i + 1 : text.length();
    }
    return result.append(text.substring(from)).toString();
  }

  /** The first {@code ;} at or after {@code from} that is real code, or {@code -1} if there is none. */
  private static int codeSemicolon(final String source, final int from) {
    final boolean[] isCode = codeMask(source);
    for (int at = source.indexOf(';', from); at > -1; at = source.indexOf(';', at + 1))
      if (isCode[at])
        return at;
    return -1;
  }

  /**
   * For each character, whether it is CODE rather than the inside of a string literal, a char literal or a comment.
   * <p>
   * {@link #withoutCallsTo} balances parentheses to find where a safe call's arguments end, and a {@code (} inside
   * {@code insertErrorMessage("a (partial" + e)} would otherwise make it swallow past the real end of the call -
   * taking a following raw {@code .getMessage()} out of the text with it and hiding a leak from
   * {@link #carriesARawMessage} (PR #7942 review). Counting only code parentheses removes that gap, and the same
   * mask stops a {@code .getMessage()} written inside a string or comment from being read as a call site.
   */
  private static boolean[] codeMask(final String text) {
    final boolean[] isCode = new boolean[text.length()];

    boolean inString = false;
    boolean inChar = false;
    boolean inTextBlock = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;

    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      isCode[i] = !inString && !inChar && !inTextBlock && !inLineComment && !inBlockComment;

      if (inLineComment) {
        if (c == '\n')
          inLineComment = false;
        continue;
      }
      if (inBlockComment) {
        if (c == '*' && i + 1 < text.length() && text.charAt(i + 1) == '/') {
          isCode[++i] = false;
          inBlockComment = false;
        }
        continue;
      }
      if (inTextBlock) {
        // an escape inside a text block consumes the next character, so the `\"""` the JLS prescribes for three
        // literal quotes carries only TWO unescaped ones and does not close the block (PR #7942 review)
        if (c == '\\' && i + 1 < text.length()) {
          isCode[++i] = false;
          continue;
        }
        // a text block ends only at its closing delimiter; the newline resync below must not touch it
        if (c == '"' && i + 2 < text.length() && text.charAt(i + 1) == '"' && text.charAt(i + 2) == '"') {
          isCode[++i] = false;
          isCode[++i] = false;
          inTextBlock = false;
        }
        continue;
      }
      if (!inString && !inChar && c == '"' && i + 2 < text.length() && text.charAt(i + 1) == '"'
          && text.charAt(i + 2) == '"') {
        isCode[++i] = false;
        isCode[++i] = false;
        inTextBlock = true;
        continue;
      }
      if ((inString || inChar) && c == '\\') {
        // an escape consumes the next character, so a literal \" does not end the literal
        if (i + 1 < text.length())
          isCode[++i] = false;
        continue;
      }
      if (c == '"' && !inChar)
        inString = !inString;
      else if (c == '\'' && !inString)
        inChar = !inChar;
      else if (c == '\n')
        // an unterminated literal cannot span a line; resynchronise rather than mis-read the rest of the text
        inString = inChar = false;
      else if (c == '/' && !inString && !inChar && i + 1 < text.length()) {
        if (text.charAt(i + 1) == '/') {
          inLineComment = true;
          isCode[i] = false;
        } else if (text.charAt(i + 1) == '*') {
          inBlockComment = true;
          isCode[i] = false;
        }
      }
    }
    return isCode;
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

      // The terminator has to be a CODE semicolon: one inside a string argument - an error message ending in
      // ";" - would cut the statement short, and anything past the cut, a raw .getMessage() included, would never
      // be judged. That is a false NEGATIVE, which is the failure this class exists to prevent (PR #7942 review).
      final int end = codeSemicolon(source, at);
      statements.add(source.substring(lineStart, end > -1 ? end : source.length()));
    }
    return statements;
  }
}
