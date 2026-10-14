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
package com.arcadedb.utility;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.utility.RegexOverAlphabet.Verdict;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9990: {@link RegexAutomaton} decides whether two patterns share a string by walking their automata in product,
 * which is only a proof if each automaton accepts exactly what {@link java.util.regex.Matcher#matches()} accepts. The
 * differential test below holds it to that over thousands of generated patterns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RegexAutomatonTest {
  private static final String DIGITS = "0123456789-";

  @Test
  void anImpossiblePatternIsRefutedWhateverTheLengthOfTheTexts() {
    // the census of the issue: 10^19 texts of a LONG, which no enumeration exhausts
    assertThat(intersects("[0-9]{20}", "-?[0-9]{1,19}", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(intersects("[0-9]{25}", "-?[0-9]{1,19}", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(intersects("[0-9]{19}", "-?[0-9]{1,19}", DIGITS)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(intersects("-[0-9]{19}", "-?[0-9]{1,19}", DIGITS)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(intersects("--[0-9]+", "-?[0-9]{1,19}", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
    // an unbounded shape and an unbounded pattern
    assertThat(intersects("[0-9]{300}", "-?[0-9]+", DIGITS)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(intersects(".*", "-?[0-9]{1,19}", DIGITS)).isEqualTo(Verdict.SATISFIABLE);
    // the empty string is never a text
    assertThat(intersects("", "-?[0-9]*", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(intersects("x*", "-?[0-9]*", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
  }

  @Test
  void anchorsOnlyHoldAtTheEnds() {
    assertThat(intersects("^[0-9]+$", "[0-9]+", DIGITS)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(intersects("[0-9]^[0-9]", "[0-9]+", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(intersects("[0-9]$[0-9]", "[0-9]+", DIGITS)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(intersects("\\A[0-9]\\z", "[0-9]+", DIGITS)).isEqualTo(Verdict.SATISFIABLE);
    // an anchor inside a repetition is left to the fallback
    assertThat(RegexAutomaton.compile("(^1|2)+", DIGITS)).isNull();
  }

  @Test
  void whatIsNotRegularIsLeftToTheCaller() {
    for (final String notCompiled : List.of("(1)\\1", "1(?=2)2", "(?<=1)2", "(?>12)", "1*+1", "1{2}{3}", "\\b1", "(?x)1 2",
        "1(?i)2", "\\k<n>", "[[]]1]", "1\\B2", "\\G1", "1\\R", "\\X", "(?s)1(?m)2", "(?-i)1", "(?i:1)"))
      assertThat(RegexAutomaton.compile(notCompiled, DIGITS)).as(notCompiled).isNull();
    // an alphabet holding a line terminator: '$' would also match before it
    assertThat(RegexAutomaton.compile("1$", "1\n")).isNull();
    assertThat(RegexAutomaton.compile("1", "1\n")).isNotNull();
    // the classes Pattern itself answers for, the leading flags included
    assertAgreesWithJava("\\h?\\p{Digit}+\\P{Alpha}", "1a -\u00a0", 4);
    assertAgreesWithJava("(?iu)[a-z\\u00e0]+", "aA\u00e0\u00c01", 3);
    assertAgreesWithJava("(?U)\\w+", "a1\u00e0-", 3);
    // flags reach quoted text, quantifiers apply to anchors, and \p and \N work inside intersected classes
    assertAgreesWithJava("(?i)\\Qa.\\E+", "aA.", 4);
    assertThat(RegexAutomaton.compile("^*1$+", "12")).isNull();
    assertAgreesWithJava("(?:)*1()+2?", "12", 3);
    assertAgreesWithJava("[\\p{Digit}&&[^0]]+[\\N{LATIN SMALL LETTER A}b]", "01ab", 4);
    // a supplementary character in the pattern matches no character of an alphabet of chars, as it matches none for Pattern
    assertAgreesWithJava("1\uD83D\uDE00?", "1a", 3);
    // and an alphabet holding surrogates is refused: Pattern reads a pair of them as one character, the automaton could not
    assertThat(RegexAutomaton.compile("1\uD83D\uDE00?", "1\uD83D\uDE00")).isNull();
  }

  @Test
  void quotedTextAndEscapesReadAsJavaReadsThem() {
    final String alphabet = "ab.]{}-\\";
    for (final String regex : List.of("\\Qa.b\\E+", "[\\Q]\\E]", "\\Q\\E", "a\\.b", "\\x{61}\\u0062", "\\0141", "[]a]+", "[^]a]",
        "\\]\\{\\}", "a}", "]", "[a-b&&[^b]]", "\\\\Qa", "(?i)A", "(?<name>a)b", "\\p{Lower}+", "\\Qa"))
      assertAgreesWithJava(regex, alphabet, 4);
  }

  @Test
  void agreesWithJavaOnGeneratedPatterns() {
    final Random random = new Random(9990);
    final String alphabet = "ab1-.";
    int compiled = 0;
    for (int i = 0; i < 2_500; i++) {
      final String regex = generate(random, 3, false);
      if (isValid(regex) && assertAgreesWithJava(regex, alphabet, 4, true))
        ++compiled;
    }
    // the generator mostly writes regular patterns: the test checks the compiler, not the fallback
    assertThat(compiled).isGreaterThan(1_200);
  }

  /**
   * An anchor inside a repetition is where {@link Pattern} stops being a complete backtracker: {@code (?:^b*){2}} does not
   * match {@code b}, which the first turn reading {@code b} and the second nothing, or the first nothing and the second
   * {@code b}, would both match, because a turn that matched nothing ends the loop. Such a pattern is left to the fallback,
   * which asks Pattern itself, and every pattern with anchors that IS compiled agrees with Pattern exactly.
   */
  @Test
  void agreesWithJavaWithAnchorsAndLeavesAnchorsInsideRepetitionsToTheFallback() {
    assertThat(Pattern.compile("(?:^b*){2}").matcher("b").matches()).isFalse();
    for (final String repeatedAnchor : List.of("(?:^b*){2}", "(?:a$)*", "(?:(?:^)?a)+", "(?:a|b\\z)?"))
      assertThat(RegexAutomaton.compile(repeatedAnchor, "ab")).as(repeatedAnchor).isNull();

    final Random random = new Random(9976);
    final String alphabet = "ab1-.";
    int compiled = 0;
    for (int i = 0; i < 2_500; i++) {
      final String regex = generate(random, 3, true);
      if (isValid(regex) && assertAgreesWithJava(regex, alphabet, 4, true))
        ++compiled;
    }
    // an anchor inside a repetition is left to the fallback, so fewer of these compile than without anchors
    assertThat(compiled).isGreaterThan(300);
  }

  @Test
  void theProductAgreesWithAnEnumerationOfShortStrings() {
    final Random random = new Random(9853);
    final String alphabet = "ab1";
    final List<String> strings = strings(alphabet, 5);
    for (int i = 0; i < 1_500; i++) {
      final boolean anchors = random.nextBoolean();
      final String left = generate(random, 2, anchors);
      final String right = generate(random, 2, anchors);
      final RegexAutomaton a = RegexAutomaton.compile(left, alphabet);
      final RegexAutomaton b = RegexAutomaton.compile(right, alphabet);
      if (a == null || b == null)
        continue;
      final Pattern pa = Pattern.compile(left);
      final Pattern pb = Pattern.compile(right);
      boolean common = false;
      for (final String s : strings)
        if (!s.isEmpty() && pa.matcher(s).matches() && pb.matcher(s).matches()) {
          common = true;
          break;
        }
      final Verdict verdict = RegexAutomaton.intersects(a, b);
      assertThat(verdict).as("'%s' and '%s'", left, right).isNotEqualTo(Verdict.UNDECIDED);
      // a common string up to 5 long proves the answer is SATISFIABLE
      if (common)
        assertThat(verdict).as("'%s' and '%s'", left, right).isEqualTo(Verdict.SATISFIABLE);
      // and a SATISFIABLE answer comes with a string both patterns match, a shortest one
      if (verdict == Verdict.SATISFIABLE) {
        final String witness = RegexAutomaton.search(a, b, Long.MAX_VALUE).common();
        assertThat(witness).isNotEmpty();
        assertThat(witness.length() <= 5).isEqualTo(common);
        // bounded: a long witness can make a nested quantifier backtrack catastrophically in Pattern
        try {
          final long deadline = TimeBoundRegex.newDeadline(500);
          assertThat(TimeBoundRegex.matchesUntil(pa, witness, deadline) && TimeBoundRegex.matchesUntil(pb, witness, deadline))
              .as("'%s' and '%s' on '%s'", left, right, witness).isTrue();
        } catch (final TimeoutException e) {
          // Pattern could not say in time: nothing to compare with
        }
      }
    }
  }

  @Test
  void aDeadlineThatHasPassedLeavesTheAnswerUndecided() {
    final RegexAutomaton a = RegexAutomaton.compile("[0-9]{1,3000}", DIGITS);
    final RegexAutomaton b = RegexAutomaton.compile("[0-9]{3001}", DIGITS);
    assertThat(RegexAutomaton.intersects(a, b, System.nanoTime())).isEqualTo(Verdict.UNDECIDED);
    assertThat(RegexAutomaton.intersects(RegexAutomaton.compile("[0-9]{1,3000}", DIGITS),
        RegexAutomaton.compile("[0-9]{3001}", DIGITS))).isEqualTo(Verdict.UNSATISFIABLE);
  }

  @Test
  void aRepetitionTooLargeToExpandIsLeftToTheCaller() {
    assertThat(RegexAutomaton.compile("[0-9]{1000000}", DIGITS)).isNull();
    assertThat(RegexAutomaton.compile("([0-9]{1000}){1000}", DIGITS)).isNull();
    assertThat(RegexAutomaton.compile("[0-9]{1000}", DIGITS)).isNotNull();
    // nesting deeper than the parser recurses on: left to the caller, never a StackOverflowError
    final String deep = "(".repeat(10_000) + "0" + ")".repeat(10_000);
    assertThat(RegexAutomaton.compile(deep, DIGITS)).isNull();
    assertThat(RegexAutomaton.compile("(".repeat(150) + "0" + ")".repeat(150), DIGITS)).isNotNull();
    // a quantifier on a quantifier, however written, is not compiled
    assertThat(RegexAutomaton.compile("0{1000}{1000}", DIGITS)).isNull();
    assertThat(RegexAutomaton.compile("(0{100}){100}{100}", DIGITS)).isNull();
    assertThat(RegexAutomaton.compile(".{0,100000}", DIGITS)).isNull();
    assertThat(RegexAutomaton.compile("(0{1000}){1000}", DIGITS)).isNull();
    // refused from its size alone, before a state is built: this one would be 10^12 copies
    assertThat(RegexAutomaton.compile("(((0{1,10000}){1,10000}){1,10000})", DIGITS)).isNull();
  }

  @Test
  void aLargeProductIsWalkedWithoutAllocatingEveryPair() {
    // about 6,000 x 6,000 states: past the dense bit set, so the walk marks pairs in a set that grows with what it visits
    final RegexAutomaton a = RegexAutomaton.compile("[0-9]{1,1500}", DIGITS);
    final RegexAutomaton b = RegexAutomaton.compile("[0-9]{1499}5", DIGITS);
    assertThat(RegexAutomaton.intersects(a, b)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(RegexAutomaton.intersects(RegexAutomaton.compile("[0-9]{1,1500}", DIGITS),
        RegexAutomaton.compile("[0-9]{1501}", DIGITS))).isEqualTo(Verdict.UNSATISFIABLE);
  }

  private static Verdict intersects(final String pattern, final String shape, final String alphabet) {
    final RegexAutomaton a = RegexAutomaton.compile(pattern, alphabet);
    final RegexAutomaton b = RegexAutomaton.compile(shape, alphabet);
    assertThat(a).as(pattern).isNotNull();
    assertThat(b).as(shape).isNotNull();
    return RegexAutomaton.intersects(a, b);
  }

  private static boolean assertAgreesWithJava(final String regex, final String alphabet, final int maxLength) {
    return assertAgreesWithJava(regex, alphabet, maxLength, true);
  }

  /**
   * @param exactly true to require the automaton to accept exactly what Java does, false only to accept everything Java does
   *
   * @return whether the pattern compiled, after checking it against Java over every short string
   */
  private static boolean assertAgreesWithJava(final String regex, final String alphabet, final int maxLength,
      final boolean exactly) {
    final RegexAutomaton automaton = RegexAutomaton.compile(regex, alphabet);
    if (automaton == null)
      return false;
    final Pattern pattern = Pattern.compile(regex);
    for (final String s : strings(alphabet, maxLength)) {
      final boolean java = pattern.matcher(s).matches();
      if (exactly || java)
        assertThat(automaton.matches(s)).as("'%s' on '%s'", regex, s).isEqualTo(java);
    }
    return true;
  }

  private static boolean isValid(final String regex) {
    try {
      Pattern.compile(regex);
      return true;
    } catch (final PatternSyntaxException e) {
      return false;
    }
  }

  private static List<String> strings(final String alphabet, final int maxLength) {
    final List<String> result = new ArrayList<>();
    result.add("");
    int from = 0;
    for (int length = 1; length <= maxLength; length++) {
      final int to = result.size();
      for (int i = from; i < to; i++)
        for (int c = 0; c < alphabet.length(); c++)
          result.add(result.get(i) + alphabet.charAt(c));
      from = to;
    }
    return result;
  }

  private static final String[] ATOMS   = { "a", "b", "1", "-", "\\.", ".", "[ab]", "[^a]", "[a-b1]", "\\d", "\\D", "\\w",
      "[\\d-]", "\\Qa.\\E", "(?:)", "[a&&[^b]]", "\\x{61}" };
  private static final String[] ANCHORS = { "^", "$", "\\A", "\\z", "\\Z" };
  private static final String[] QUANTIFIERS = { "", "", "", "*", "+", "?", "{2}", "{0,2}", "{1,}", "*?", "+?", "{1,3}?" };

  private static String generate(final Random random, final int depth, final boolean anchors) {
    final StringBuilder regex = new StringBuilder();
    final int items = 1 + random.nextInt(4);
    for (int i = 0; i < items; i++) {
      if (depth > 0 && random.nextInt(4) == 0) {
        regex.append(random.nextBoolean() ? "(" : "(?:").append(generate(random, depth - 1, anchors));
        if (random.nextInt(3) == 0)
          regex.append('|').append(generate(random, depth - 1, anchors));
        regex.append(')');
      } else if (anchors && random.nextInt(5) == 0)
        regex.append(ANCHORS[random.nextInt(ANCHORS.length)]);
      else
        regex.append(ATOMS[random.nextInt(ATOMS.length)]);
      regex.append(QUANTIFIERS[random.nextInt(QUANTIFIERS.length)]);
    }
    if (random.nextInt(5) == 0)
      regex.append('|').append(generate(random, Math.max(0, depth - 1), anchors));
    return regex.toString();
  }
}
