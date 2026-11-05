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

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bans the no-argument {@code String.toUpperCase()} / {@code toLowerCase()} from EVERY module's production classes.
 * <p>
 * They fold with the JVM DEFAULT locale, and in the Turkish, Azeri and Lithuanian locales {@code 'i'} upper-cases to the dotted
 * {@code 'İ'} (U+0130) rather than to {@code 'I'}. Every internal use of these methods in this code base is keyword or identifier
 * normalisation - a SQL keyword, an enum constant name, a function name - where that is simply wrong, and the failure is invisible
 * to anyone not running such a locale. Issue #7900 found the family by running the suite under {@code -Duser.language=tr}, and the
 * damage on the worst arm was silent: {@code CREATE INDEX ... COLLATE ci} built a case-SENSITIVE index, the UNIQUE constraint
 * stopped firing, and the wrong collation was PERSISTED. A behavioural test can only cover the sites somebody thought to write one
 * for; this covers the next one.
 * <p>
 * The fix at any site this flags is one argument: {@code toUpperCase(Locale.ROOT)}. If a site genuinely wants the user's locale -
 * formatting text for a human to read - it says so with {@code Locale.getDefault()}, which is explicit and passes.
 * <p>
 * The census ranges over the authority (#11288): every call of the two no-argument methods, or method reference to them
 * ({@code String::toUpperCase}), in the compiled classes of every module ({@link CompiledClasses}). The source-text census it
 * replaces matched a regular expression in the {@code com/arcadedb} sources of each module, behind a seventy-line lexer that told
 * code from comments and literals, and a method reference passed it. In bytecode a comment or a literal is not a call, and a
 * method reference is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class NoDefaultLocaleCaseConversionCensusTest {
  private static final String       AUTHORITY  = "java/lang/String";
  private static final Set<String>  CONVERSION = Set.of("toUpperCase", "toLowerCase");
  private static final String       NO_LOCALE  = "()Ljava/lang/String;";

  @Test
  void noProductionClassFoldsCaseWithTheDefaultLocale() throws Exception {
    final Map<String, Set<String>> callSites = callSitesOf(CompiledClasses.ofEveryModule());

    assertThat(callSites).as("the no-argument toUpperCase()/toLowerCase() folds with the JVM default locale, which turns 'i' into "
        + "'İ' on a Turkish, Azeri or Lithuanian server and silently breaks keyword matching - pass Locale.ROOT (or "
        + "Locale.getDefault() if the text really is for a human to read)").isEmpty();
  }

  /** The control: the census still recognises the call, a method reference to it, and not the overload with a locale. */
  @Test
  void theCensusSeesEveryShapeOfTheConversion() throws Exception {
    final Map<String, Set<String>> sites = callSitesOf(CompiledClasses.ofTheProbes());

    assertThat(sites).containsKeys("census/ShapeProbes$CaseConversion#plain", "census/ShapeProbes$CaseConversion#methodReference");
    assertThat(sites).doesNotContainKeys("census/ShapeProbes$CaseConversion#withALocale",
        "census/ShapeProbes$CaseConversion#namedInALiteral");
  }

  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) {
    return CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> AUTHORITY.equals(owner) && CONVERSION.contains(name) && NO_LOCALE.equals(desc));
  }
}
