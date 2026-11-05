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
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9807 and #9968: the server's low-disk, heap-pressure and safepoint warnings and its start event were built with
 * {@code String.format} and no {@code Locale}. The text is the body of a server event that {@code GET /api/v1/server} serves to
 * any REST client, so under a comma-decimal locale the figures carried a comma, and under a locale with its own numbering system
 * they were not even ASCII digits: one cluster answered with a different string from each node. The messages are pinned by
 * {@code Issue9807ServerMonitorLocaleTest}.
 * <p>
 * The census ranges over the authority (#11288): every call of {@code ServerEventLog.reportEvent}, or of an implementation of it,
 * in the compiled classes of every module ({@link CompiledClasses}), with its message followed back to what made it
 * ({@link CallOperands}): through a local, through each part of a string concatenation, and into the method of the code base that
 * returned it ({@code ServerMonitor.lowDiskMessage}). A {@code String.format} without a {@code Locale}, or a
 * {@code String.formatted}, which takes none, anywhere on that path is an offender. The source-text census it replaces read the
 * argument text of a {@code .reportEvent(} spelling and the initializer of a local named by it, so a message built in a helper,
 * which is where the four were, passed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ServerEventMessageLocaleCensusTest {
  private static final String EVENT_LOG      = "com/arcadedb/server/event/ServerEventLog";
  private static final String REPORT         = "reportEvent";
  private static final int    MESSAGE        = 3;
  private static final String STRING         = "java/lang/String";
  private static final String DEFAULT_FORMAT = "(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;";
  /** How many calls deep a message is followed into the methods that build it. */
  private static final int    DEPTH          = 4;

  @Test
  void noServerEventMessageIsFormattedWithTheDefaultLocale() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> followed = new TreeSet<>();
    final Set<String> offenders = offenders(classes, CompiledClasses.implementationsOf(EVENT_LOG, classes), followed);

    assertThat(followed).as("the census must follow a message into the method that builds it").contains(
        "server/monitor/ServerMonitor#lowDiskMessage", "server/ArcadeDBServer#startedMessage");
    assertThat(offenders).as("server events whose message is formatted with the JVM default locale: every node of a cluster "
        + "reports a different string, in its own digits - pass Locale.ROOT to String.format (issues #9807, #9968)").isEmpty();
  }

  /** The control: a message formatted in a helper, in a concatenation, and none for a message given a Locale. */
  @Test
  void theCensusFollowsAMessageWhereverItIsFormatted() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofTheProbes();
    final Set<String> eventLogs = CompiledClasses.implementationsOf(EVENT_LOG, CompiledClasses.ofEveryModule());

    assertThat(offenders(classes, eventLogs, new TreeSet<>())).containsExactly("census/ShapeProbes$EventMessage#builtInAHelper",
        "census/ShapeProbes$EventMessage#concatenated");
  }

  private static Set<String> offenders(final Map<String, byte[]> classes, final Set<String> eventLogs, final Set<String> followed) {
    final List<CallOperands.Site> reports = CallOperands.of(classes,
        (owner, name, desc) -> eventLogs.contains(owner) && REPORT.equals(name));
    assertThat(reports).as("the census must reach the reportEvent calls").isNotEmpty();

    final Set<String> offenders = new TreeSet<>();
    for (final CallOperands.Site report : reports)
      if (formatsWithTheDefaultLocale(classes, report.in(), report.argument(MESSAGE), DEPTH, followed))
        offenders.add(report.key());
    return offenders;
  }

  private static boolean formatsWithTheDefaultLocale(final Map<String, byte[]> classes, final CallOperands.Method method,
      final Set<AbstractInsnNode> origins, final int depth, final Set<String> followed) {
    for (final AbstractInsnNode origin : origins) {
      if (origin instanceof MethodInsnNode call) {
        if (STRING.equals(call.owner) && ("formatted".equals(call.name) || "format".equals(call.name) && DEFAULT_FORMAT.equals(
            call.desc)))
          return true;
        if (depth > 0) {
          final CallOperands.Method builder = CallOperands.declarationOf(classes, call);
          if (builder != null) {
            followed.add(builder.key());
            if (formatsWithTheDefaultLocale(classes, builder, builder.returned(), depth - 1, followed))
              return true;
          }
        }
      } else if (origin instanceof InvokeDynamicInsnNode concatenation && formatsWithTheDefaultLocale(classes, method,
          method.operandsOf(concatenation), depth, followed))
        return true;
    }
    return false;
  }
}
