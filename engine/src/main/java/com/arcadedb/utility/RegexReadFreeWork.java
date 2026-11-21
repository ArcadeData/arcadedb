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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Measures how much a {@link Pattern} can backtrack WITHOUT READING A CHARACTER of its input (issue #12595), which is the one
 * kind of work {@link TimeBoundRegex} cannot interrupt: it stops a runaway match from {@link CharSequence#charAt(int)}, and a
 * node that tests no character (an anchor, an empty alternative, a quantifier that matches nothing, a negative look-ahead at
 * the end of the input) never calls it. {@code \A(?:\A|\A)(?:\A|\A)...\z} against {@code "ab"} takes 2^n steps and reads
 * nothing, so no deadline ever fires.
 * <p>
 * Nothing in {@code java.util.regex} can be told to stop (it polls neither interrupts nor a clock, and a thread cannot be
 * stopped from outside any more), so the answer has to come BEFORE the match, from the structure of the pattern. Every
 * element of the pattern is given the number of DIFFERENT WAYS it can succeed without reading anything:
 * <ul>
 *   <li>a character, a class, {@code .}, {@code \R}: 0 - it has to read to succeed;</li>
 *   <li>an anchor or a word boundary, an empty group, an inline flag, a negative look-around, a quantifier that allows no
 *   repetition: 1;</li>
 *   <li>a back reference: 1 when its group can match nothing, else 0;</li>
 *   <li>a positive look-around or an atomic group: at most 1, whatever its body has (it keeps its first match);</li>
 *   <li>an alternation: the SUM of its options; a sequence: the PRODUCT of its elements, and 0 as soon as one must read;</li>
 *   <li>{@code x{m,n}}: the ways of {@code x} plus 1 when it may be skipped, because {@code java.util.regex} leaves a loop after
 *   the first turn that consumed nothing (a counted {@code (?:\A|\A){40}} is NOT 2^40 paths, the JDK cuts it after one turn),
 *   and a body that must read can only be skipped.</li>
 * </ul>
 * Reading is counted as if every character test failed read-free (the end of the input, where {@code a?} costs no
 * {@code charAt()}), which makes every number an upper bound of what any input can cost. The product of a RUN of elements, the
 * ones between two that must read, is what a match multiplies out before it reads again; the largest run of the pattern is the
 * answer. An element that must read ends the run because it either reads (and the deadline gets a look) or fails right there.
 * <p>
 * The ways are not the whole cost, so every element also carries its WORK: the read-free node visits one attempt makes
 * through it. A sequence pays each element once per path that reaches it, an alternation pays every option, and a counted
 * loop whose body can match nothing pays its body once per iteration, because the JDK does not leave a deterministic loop
 * before its minimum (a nested {@code (?:(?:(?:\A){1000}){1000}){1000}x} is 10^9 visits, one way, and not one read). Two
 * measures of it are taken, each with its own reading of the pattern:
 * <ul>
 *   <li>{@link Measure#work} at the END of the input, where every test fails without a read: what ONE attempt can cost, and the
 *   only attempt a {@code matches()} makes;</li>
 *   <li>{@link Measure#scanWork} in the MIDDLE of the input, where a character test reads and an anchor fails without one: what
 *   each start position of a {@code find()} costs when nothing reads (a hundred {@code \Ax} alternatives fail a hundred times at
 *   every position and never call {@code charAt()}). A pattern whose paths read at every position needs no bound here, since
 *   the deadline hears from every one of them.</li>
 * </ul>
 * <p>
 * The structure is parsed here and nothing else: what a character matches is never asked, and the pattern is known to compile
 * before this class sees it, so it never has to diagnose a syntax error, only to keep up with the syntax {@link Pattern}
 * accepts (comments mode, {@code \Q..\E} quoting, inline flags, nested classes, named groups). A pattern it cannot follow is
 * reported as unboundable rather than assumed harmless.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RegexReadFreeWork {
  /** every count stops here, so a product of large ones cannot overflow */
  static final long SATURATED = 1L << 40;

  private final String               source;
  // true: the middle of the input, where a character test reads; false: its end, where every test fails without a read
  private final boolean              middle;
  private       int                  position;
  private       int                  flags;
  private       long                 maxWays   = 1;
  private       int                  groups;
  // the ways of each capturing group once it is closed (0 while open or unknown), indexed by group number
  private       long[]               groupWays = new long[8];
  private       Map<String, Integer> names;

  /**
   * The structure measured.
   *
   * @param maxWays  the most different ways a run of elements can succeed without a read
   * @param groups   how many capturing groups the pattern opens (the JDK's own count)
   * @param work     the read-free node visits one attempt can make at the end of the input
   * @param scanWork the read-free node visits one start position of a scan makes in the middle of the input
   */
  record Measure(long maxWays, int groups, long work, long scanWork) {
    /** what is not understood is never trusted: as much work as can be counted */
    private static final Measure UNBOUNDED = new Measure(SATURATED, 0, SATURATED, SATURATED);

    /** the worse of two readings of the same pattern */
    private Measure max(final Measure other) {
      return new Measure(Math.max(maxWays, other.maxWays), groups, Math.max(work, other.work), Math.max(scanWork, other.scanWork));
    }
  }

  /** What an element can do without reading: the ways it succeeds, and the node visits it makes doing so. */
  private record Cost(long ways, long work) {
  }

  /** The syntax of the pattern is not one this class follows. */
  static final class Unparseable extends RuntimeException {
    private Unparseable(final String message) {
      super(message, null, false, false);
    }
  }

  private RegexReadFreeWork(final String source, final int flags, final boolean middle) {
    this.source = source;
    this.flags = flags;
    this.middle = middle;
  }

  /**
   * @return what {@code regex} can do without reading, {@link Measure#UNBOUNDED} when the pattern cannot be followed, so that
   * what is not understood is never trusted
   */
  static Measure measureOrUnbounded(final String regex, final int flags) {
    try {
      return measure(regex, flags);
    } catch (final Unparseable | StackOverflowError e) {
      return Measure.UNBOUNDED;
    }
  }

  /**
   * @param flags the flags {@link Pattern#flags()} answers, which are the ones in force at the END of the pattern: an inline
   *              {@code (?x)} or {@code (?-x)} has already replaced the ones the caller passed, so a pattern that has one may have
   *              started in either state, and each reads the same text differently ({@code #} starts a comment in one and is a
   *              character in the other). Every state it can have started in is measured and the worst one is the answer; one
   *              that cannot parse the text is not the state the pattern has.
   *
   * @throws Unparseable when the syntax is not one this class follows in any of them
   */
  static Measure measure(final String regex, final int flags) {
    if ((flags & Pattern.LITERAL) != 0)
      return new Measure(1, 0, 0, 0);
    final String unquoted = unquote(regex);
    final boolean commentsEither = mentionsFlag(unquoted, 'x');
    final boolean unixLinesEither = mentionsFlag(unquoted, 'd');

    Measure worst = null;
    Unparseable failure = null;
    // the state the flags say comes first, then the ones an inline flag could have changed it from
    for (int flipComments = 0; flipComments < (commentsEither ? 2 : 1); ++flipComments)
      for (int flipUnixLines = 0; flipUnixLines < (unixLinesEither ? 2 : 1); ++flipUnixLines) {
        final int start = flags ^ (flipComments == 1 ? Pattern.COMMENTS : 0) ^ (flipUnixLines == 1 ? Pattern.UNIX_LINES : 0);
        try {
          final Measure measure = measureStartingWith(unquoted, start);
          worst = worst == null ? measure : worst.max(measure);
        } catch (final Unparseable e) {
          failure = e;
        }
      }
    if (worst == null)
      throw failure;
    return worst;
  }

  /** What {@code regex} can do without reading when it starts in exactly the state {@code flags} give, no other considered. */
  static Measure measureInState(final String regex, final int flags) {
    if ((flags & Pattern.LITERAL) != 0)
      return new Measure(1, 0, 0, 0);
    return measureStartingWith(unquote(regex), flags);
  }

  private static Measure measureStartingWith(final String unquoted, final int flags) {
    final RegexReadFreeWork end = new RegexReadFreeWork(unquoted, flags, false);
    final Cost endCost = end.parse();
    final Cost middleCost = new RegexReadFreeWork(unquoted, flags, true).parse();
    return new Measure(end.maxWays, end.groups, endCost.work(), middleCost.work());
  }

  /** Whether an inline flag group of the text may set or clear {@code letter}: some "(?" is followed by it before a ')' or a ':'. */
  private static boolean mentionsFlag(final String text, final char letter) {
    for (int i = text.indexOf("(?"); i >= 0; i = text.indexOf("(?", i + 2))
      for (int j = i + 2; j < text.length(); ++j) {
        final char c = text.charAt(j);
        if (c == letter)
          return true;
        if (c == ')' || c == ':')
          break;
      }
    return false;
  }

  private Cost parse() {
    final Cost cost = alternation();
    if (position < source.length())
      throw new Unparseable("unbalanced ')' at " + position);
    return cost;
  }

  /**
   * {@code \Q..\E} spans become the escaped characters they stand for, as {@link Pattern} itself does before it parses
   * anything: the quotation can hold a parenthesis, a bracket or a quantifier, and it reaches into character classes too.
   */
  private static String unquote(final String regex) {
    final int length = regex.length();
    int i = 0;
    while (i < length - 1) {
      if (regex.charAt(i) != '\\')
        ++i;
      else if (regex.charAt(i + 1) != 'Q')
        i += 2;
      else
        break;
    }
    if (i >= length - 1)
      return regex;

    final StringBuilder out = new StringBuilder(length + 16);
    out.append(regex, 0, i);
    boolean quoted = false;
    while (i < length) {
      final char c = regex.charAt(i++);
      if (c == '\\') {
        if (quoted) {
          if (i < length && regex.charAt(i) == 'E') {
            ++i;
            quoted = false;
          } else
            out.append("\\\\");
        } else if (i < length && regex.charAt(i) == 'Q') {
          ++i;
          quoted = true;
        } else {
          out.append(c);
          if (i < length)
            out.append(regex.charAt(i++));
        }
      } else if (quoted && c < 128 && !Character.isLetterOrDigit(c))
        out.append('\\').append(c);
      else
        out.append(c);
    }
    return out.toString();
  }

  private Cost alternation() {
    Cost first = sequence();
    if (current() != '|') {
      note(first.ways());
      return first;
    }
    long ways = first.ways();
    long work = first.work();
    while (current() == '|') {
      ++position;
      final Cost option = sequence();
      ways = add(ways, option.ways());
      work = add(work, option.work());
    }
    note(ways);
    return new Cost(ways, work);
  }

  private Cost sequence() {
    // the product of the elements since the last one that has to read: what a match multiplies out before it reads again
    long run = 1;
    // the product of all of them, 0 as soon as one has to read
    long whole = 1;
    // how many paths reach the next element: the product of the ways of the run it is in, and so what its work is multiplied by.
    // A read starts a new run. In the middle of the input what follows it is work the deadline gets a look before (the read polls
    // it), so a scan does not count it; at the end of the input the work after a read is real, whatever it costs there, and a
    // suffix that matches nothing a billion times must not be hidden behind the character in front of it
    long arriving = 1;
    long work = 0;
    boolean any = false;
    for (; ; ) {
      final int c = current();
      if (c < 0 || c == '|' || c == ')')
        break;
      any = true;
      final Cost element = quantified(c);
      work = add(work, multiply(arriving, element.work()));
      arriving = element.ways() == 0 ? (middle ? 0 : 1) : multiply(arriving, element.ways());
      if (element.ways() == 0) {
        run = 1;
        whole = 0;
      } else {
        run = multiply(run, element.ways());
        note(run);
        if (whole != 0)
          whole = multiply(whole, element.ways());
      }
    }
    // an empty option is a visit that succeeds
    return new Cost(whole, any ? work : 1);
  }

  private Cost quantified(final int first) {
    // a body repeated {0} times is parsed (its syntax and its groups count) but never entered, so what it peaks at is not the
    // pattern's
    final long maxWaysBefore = maxWays;
    final Cost body = atom(first);
    final int q = current();
    final long min;
    final long max;
    switch (q) {
    case '*' -> {
      ++position;
      min = 0;
      max = -1;
    }
    case '+' -> {
      ++position;
      min = 1;
      max = -1;
    }
    case '?' -> {
      ++position;
      min = 0;
      max = 1;
    }
    case '{' -> {
      ++position;
      min = number();
      long upper = min;
      int d = current();
      if (d == ',') {
        ++position;
        upper = current() == '}' ? -1 : number();
        d = current();
      }
      if (d != '}')
        throw new Unparseable("unclosed repetition at " + position);
      ++position;
      max = upper;
    }
    default -> {
      return body;
    }
    }

    boolean possessive = false;
    final int suffix = current();
    if (suffix == '?')
      ++position;
    else if (suffix == '+') {
      ++position;
      possessive = true;
    }
    long ways = quantify(body.ways(), min, max);
    // a possessive loop keeps the first thing it finds
    if (possessive)
      ways = Math.min(1, ways);
    final long work;
    if (max == 0) {
      // never entered: one visit, and the peaks inside the body do not count
      maxWays = maxWaysBefore;
      work = 1;
    } else if (body.ways() == 0)
      // a body that has to read is attempted once, whatever it is repeated
      work = body.work();
    else
      // a body that can match nothing is entered once per iteration: its minimum, since a deterministic loop does not stop
      // early on a turn that consumed nothing, and one more when the loop may go on
      work = multiply(Math.max(min, 1) + (max < 0 || max > min ? 1 : 0), body.work());
    note(ways);
    return new Cost(ways, work);
  }

  private static long quantify(final long ways, final long min, final long max) {
    // x{0}: matches nothing at all, one way
    if (max == 0)
      return 1;
    // a body that has to read can only be skipped
    if (ways == 0)
      return min == 0 ? 1 : 0;
    // the JDK leaves a loop after the first turn that consumed nothing, so a body that can match nothing is entered once at most
    return add(min == 0 ? 1 : 0, ways);
  }

  private long number() {
    long value = 0;
    boolean any = false;
    for (int c = current(); c >= '0' && c <= '9'; c = current()) {
      value = Math.min(1L << 31, value * 10 + (c - '0'));
      ++position;
      any = true;
    }
    if (!any)
      throw new Unparseable("a repetition without a count at " + position);
    return value;
  }

  /** A character, a class, {@code .}: it reads, so at the end of the input it fails there (one visit) and in the middle it reads. */
  private Cost reading() {
    return new Cost(0, middle ? 0 : 1);
  }

  /** An anchor: one visit, and in the middle of the input it fails there, without a read. */
  private Cost anchor() {
    return new Cost(middle ? 0 : 1, 1);
  }

  /** A word boundary looks at the characters around it: in the middle of the input it reads. */
  private Cost boundary() {
    return new Cost(middle ? 0 : 1, middle ? 0 : 1);
  }

  /** What matches nothing without a test of its own: an empty group or option, an inline flag, an empty repetition. */
  private static Cost empty() {
    return new Cost(1, 1);
  }

  private Cost atom(final int c) {
    switch (c) {
    case '(':
      return group();
    case '[':
      skipClass();
      return reading();
    case '\\':
      return escape();
    case '^', '$':
      ++position;
      return anchor();
    case '*', '+', '?':
      throw new Unparseable("dangling '" + (char) c + "' at " + position);
    case '{':
      // Pattern reads a '{' where an atom should be as an empty one, and the repetition that follows applies to it
      return empty();
    default:
      ++position;
      return reading();
    }
  }

  private Cost group() {
    // the cursor is on the '('
    ++position;
    final int savedFlags = flags;
    int capturing = 0;
    boolean positiveLook = false;
    boolean negativeLook = false;
    boolean atomic = false;

    if (current() == '?') {
      ++position;
      if (position >= source.length())
        throw new Unparseable("unclosed group");
      // the character after "(?" is read as it is, comments mode or not
      final char kind = source.charAt(position++);
      switch (kind) {
      case ':' -> {
      }
      case '=' -> positiveLook = true;
      case '!' -> negativeLook = true;
      case '>' -> atomic = true;
      case '<' -> {
        final int next = current();
        if (next == '=') {
          ++position;
          positiveLook = true;
        } else if (next == '!') {
          ++position;
          negativeLook = true;
        } else {
          final String name = readName();
          capturing = ++groups;
          if (names == null)
            names = new HashMap<>();
          names.put(name, capturing);
        }
      }
      default -> {
        // inline flags: "(?i)" holds for the rest of the enclosing group, "(?i:...)" for its own body
        --position;
        parseFlags();
        final int f = current();
        if (f == ')') {
          ++position;
          return empty();
        }
        if (f != ':')
          throw new Unparseable("unknown inline modifier at " + position);
        ++position;
      }
      }
    } else
      capturing = ++groups;

    final Cost body = alternation();
    if (current() != ')')
      throw new Unparseable("unclosed group at " + position);
    ++position;
    flags = savedFlags;

    if (capturing > 0) {
      if (capturing >= groupWays.length)
        groupWays = Arrays.copyOf(groupWays, Math.max(capturing + 1, groupWays.length * 2));
      groupWays[capturing] = body.ways();
    }
    // a look-around and an atomic group run their body once and keep its first match; a negative one succeeds where its body
    // fails, which can be without a read
    if (negativeLook)
      return new Cost(1, add(body.work(), 1));
    if (positiveLook || atomic)
      return new Cost(Math.min(1, body.ways()), add(body.work(), 1));
    return body;
  }

  /** The inline flags at the cursor; only the two that change how the pattern is read matter here. */
  private void parseFlags() {
    boolean on = true;
    for (; ; ) {
      final int c = current();
      switch (c) {
      case '-' -> on = false;
      case 'x' -> flags = on ? flags | Pattern.COMMENTS : flags & ~Pattern.COMMENTS;
      case 'd' -> flags = on ? flags | Pattern.UNIX_LINES : flags & ~Pattern.UNIX_LINES;
      case 'i', 'm', 's', 'u', 'U', 'c' -> {
      }
      default -> {
        return;
      }
      }
      ++position;
    }
  }

  private Cost escape() {
    // the cursor is on the '\\'; the character after it is taken as it is
    ++position;
    if (position >= source.length())
      throw new Unparseable("a trailing backslash");
    final char e = source.charAt(position++);
    switch (e) {
    case 'A', 'G', 'z', 'Z':
      return anchor();
    case 'B':
      return boundary();
    case 'b':
      // \b{g}
      if (position < source.length() && source.charAt(position) == '{')
        skipPast('}');
      return boundary();
    case 'k': {
      // the '<' and the name are read as Pattern reads them, past whatever comments mode ignores
      if (current() != '<')
        throw new Unparseable("a named back reference without a name at " + position);
      ++position;
      final Integer group = names == null ? null : names.get(readName());
      return backReference(group == null ? 0 : group);
    }
    case '1', '2', '3', '4', '5', '6', '7', '8', '9': {
      // another digit belongs to the reference only while the group it names exists
      int group = e - '0';
      for (int d = current(); d >= '0' && d <= '9'; d = current()) {
        final int longer = group * 10 + (d - '0');
        if (longer > groups)
          break;
        group = longer;
        ++position;
      }
      return backReference(group);
    }
    case 'x', 'p', 'P', 'N':
      if (position < source.length() && source.charAt(position) == '{')
        skipPast('}');
      return reading();
    case 'c':
      // the control character is the one after it, whatever it is
      ++position;
      return reading();
    default:
      // classes (\d \w \s \h \v), \R, \X, the escapes of one character and the escaped punctuation: each reads
      return reading();
    }
  }

  /**
   * A group name at the cursor and the '>' that ends it, read through the comments-aware cursor as Pattern reads them, so
   * that comments mode may put whitespace in the middle of one.
   */
  private String readName() {
    final StringBuilder name = new StringBuilder();
    for (int c = current(); c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'; c = current()) {
      name.append((char) c);
      ++position;
    }
    if (current() != '>')
      throw new Unparseable("a group name that does not end in '>' at " + position);
    ++position;
    return name.toString();
  }

  private Cost backReference(final int group) {
    // a reference to a group that is still open, or has not been seen, fails: it can not succeed without reading either
    return new Cost(group > 0 && group < groupWays.length ? Math.min(1, groupWays[group]) : 0, 1);
  }

  /** Skips a character class, nested ones included, with the cursor on its '['. */
  private void skipClass() {
    ++position;
    if (current() == '^')
      ++position;
    // a ']' right after the opening stands for itself
    boolean first = true;
    for (; ; ) {
      final int c = current();
      if (c < 0)
        throw new Unparseable("an unclosed character class");
      if (c == ']' && !first) {
        ++position;
        return;
      }
      first = false;
      if (c == '[')
        skipClass();
      else if (c == '\\') {
        ++position;
        if (position >= source.length())
          throw new Unparseable("a trailing backslash");
        final char e = source.charAt(position++);
        if ((e == 'x' || e == 'p' || e == 'P' || e == 'N') && position < source.length() && source.charAt(position) == '{')
          skipPast('}');
        else if (e == 'c')
          ++position;
      } else
        ++position;
    }
  }

  private void skipPast(final char close) {
    final int end = source.indexOf(close, position);
    if (end < 0)
      throw new Unparseable("a '" + close + "' is missing after " + position);
    position = end + 1;
  }

  /** The character at the cursor, past what comments mode ignores; -1 at the end. */
  private int current() {
    if ((flags & Pattern.COMMENTS) != 0)
      skipIgnored();
    return position < source.length() ? source.charAt(position) : -1;
  }

  private void skipIgnored() {
    while (position < source.length()) {
      final char c = source.charAt(position);
      if (c == ' ' || c == '\t' || c == '\n' || c == 0x0B || c == '\f' || c == '\r')
        ++position;
      else if (c == '#') {
        ++position;
        while (position < source.length() && !isLineEnd(source.charAt(position)))
          ++position;
      } else
        return;
    }
  }

  private boolean isLineEnd(final char c) {
    if ((flags & Pattern.UNIX_LINES) != 0)
      return c == '\n';
    return c == '\n' || c == '\r' || c == '\u0085' || c == ' ' || c == ' ';
  }

  private void note(final long ways) {
    if (ways > maxWays)
      maxWays = ways;
  }

  private static long add(final long a, final long b) {
    return Math.min(SATURATED, a + b);
  }

  private static long multiply(final long a, final long b) {
    if (a == 0 || b == 0)
      return 0;
    return a > SATURATED / b ? SATURATED : Math.min(SATURATED, a * b);
  }
}
