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

import org.junit.jupiter.api.Test;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7141: a {@code LogManager.log(...)} call whose format string carries more conversions than the call supplies arguments
 * does not fail - {@code LogManager} pads the missing slots with {@code null}, so the message prints {@code null} exactly where the
 * offending value belonged. Two such calls had been sitting in the tree unnoticed ({@code ArcadeDBServer.loadDefaultDatabases} lost
 * the malformed startup command, {@code ComponentFile.drop} a phantom third placeholder), each costing an operator a debugging round
 * trip. Compilers cannot catch it: the format is a plain {@code String} and the arguments are varargs.
 * <p>
 * The census ranges over the authority (#11288): every call of {@code LogManager.log} in the compiled classes of every module
 * ({@link CompiledClasses}), with the format constant and the arguments read from the call itself ({@link CallOperands}). The
 * source-text census it replaces inspected only a call spelled {@code LogManager.instance().log(}, so a call through a held
 * instance passed it, and it had to guess whether the fourth argument was a {@code Throwable}: the overload the compiler picked
 * says so. A message that is not a constant (built at runtime) is out of reach, as it was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LogMessageArityCensusTest {
  private static final String AUTHORITY = "com/arcadedb/log/LogManager";
  private static final String METHOD    = "log";
  /** The overloads are {@code (requester, level, message, [throwable], args...)}: the message is the third argument. */
  private static final int    MESSAGE   = 2;
  private static final String THROWABLE = "Ljava/lang/Throwable;";
  private static final String VARARGS   = "[Ljava/lang/Object;";

  /** A java.util.Formatter conversion. {@code %%} and {@code %n} are stripped before this is applied. */
  private static final Pattern CONVERSION = Pattern.compile("%(?:\\d+\\$)?[-#+ 0,(]*\\d*(?:\\.\\d+)?[a-zA-Z]");

  private static final CompiledClasses.CallMatcher LOG_CALLS = (owner, name, desc) -> AUTHORITY.equals(owner) && METHOD.equals(name);

  @Test
  void everyLogCallSuppliesAnArgumentForEveryFormatConversion() throws Exception {
    final List<CallOperands.Site> calls = CallOperands.of(CompiledClasses.ofEveryModule(), LOG_CALLS);
    assertThat(calls.stream().filter(c -> CallOperands.stringConstant(c.argument(MESSAGE)) != null).count())
        .as("the census must reach the LogManager.log calls with a constant format, hundreds of them").isGreaterThan(500);

    assertThat(shortCalls(calls)).as("log calls whose format string has more conversions than the call supplies arguments: "
        + "the missing ones print as null (issue #7141)").isEmpty();
  }

  /** The control: a call through a held instance, one after a Throwable, and none for a complete call or an escaped percent. */
  @Test
  void theCensusSeesAShortCallWhateverItsShape() throws Exception {
    final List<String> offenders = shortCalls(CallOperands.of(CompiledClasses.ofTheProbes(), LOG_CALLS));

    assertThat(offenders).hasSize(2);
    assertThat(offenders.get(0)).startsWith("census/ShapeProbes$LogArity#oneShortAfterAThrowable ");
    assertThat(offenders.get(1)).startsWith("census/ShapeProbes$LogArity#oneShortThroughALocal ");
  }

  private static List<String> shortCalls(final List<CallOperands.Site> calls) {
    final List<String> offenders = new ArrayList<>();
    for (final CallOperands.Site call : calls) {
      final String format = CallOperands.stringConstant(call.argument(MESSAGE));
      if (format == null)
        continue;
      final int conversions = conversionsOf(format);
      if (conversions == 0)
        continue;

      final Integer supplied = suppliedArguments(call);
      if (supplied != null && supplied < conversions)
        offenders.add(call.key() + " expects " + conversions + " argument(s), supplies " + supplied + ": " + format.lines().findFirst()
            .orElse(format));
    }
    offenders.sort(null);
    return offenders;
  }

  /** The format arguments of the overload the call names, or null when they are an array of unknown length. */
  private static Integer suppliedArguments(final CallOperands.Site call) {
    final Type[] parameters = Type.getArgumentTypes(call.call().desc);
    int supplied = 0;
    for (int i = MESSAGE + 1; i < parameters.length; i++) {
      final String parameter = parameters[i].getDescriptor();
      if (THROWABLE.equals(parameter))
        continue;
      if (VARARGS.equals(parameter)) {
        final Integer length = CallOperands.arrayLength(call, call.argument(i));
        if (length == null)
          return null;
        supplied += length;
      } else
        supplied++;
    }
    return supplied;
  }

  private static int conversionsOf(final String format) {
    final Matcher m = CONVERSION.matcher(format.replace("%%", "").replace("%n", ""));
    int count = 0;
    while (m.find())
      count++;
    return count;
  }
}
