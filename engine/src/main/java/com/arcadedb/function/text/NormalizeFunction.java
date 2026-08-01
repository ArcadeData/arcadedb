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
package com.arcadedb.function.text;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.text.Normalizer;
import java.util.Locale;

/**
 * normalize(string, [normalForm]) - returns the given string normalized using the specified normal form.
 * Default normal form is NFC.
 * Supported forms: NFC, NFD, NFKC, NFKD.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NormalizeFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "normalize";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args[0] == null || CypherFunctionHelper.isExplicitNull(args, 1))
      return null;

    // STRING-only, as Cypher declares it and as isNormalized() already did: toString()-ing whatever arrived turned
    // normalize(123) into the string "123" rather than the type error Neo4j raises, so a wrong query looked like a
    // successful one - the failure mode #5476 and #5477 were about. See issue #5602.
    if (!(args[0] instanceof CharSequence))
      throw CypherFunctionHelper.typeMismatch("normalize", "a STRING", args[0]);

    return Normalizer.normalize(args[0].toString(), parseNormalForm(args.length > 1 ? args[1] : null, "normalize"));
  }

  /**
   * Resolves the optional normal-form argument shared by {@code normalize()} and {@code isNormalized()}, defaulting to
   * NFC as Cypher does. Kept here rather than duplicated so the two functions accept exactly the same set of form
   * names and reject an unknown one with the same message.
   *
   * <p>
   * A {@code null} here means the argument was omitted. A form written as an explicit {@code null} never reaches this
   * method: it propagates, per {@link CypherFunctionHelper#isExplicitNull} (issue #5629).
   *
   * @param form         the argument as written, or {@code null} when the call omitted it
   * @param functionName the caller's name, so the error names the function the client actually wrote
   */
  public static Normalizer.Form parseNormalForm(final Object form, final String functionName) {
    if (form == null)
      return Normalizer.Form.NFC;

    final String formName = form.toString().toUpperCase(Locale.ROOT);
    return switch (formName) {
      case "NFC" -> Normalizer.Form.NFC;
      case "NFD" -> Normalizer.Form.NFD;
      case "NFKC" -> Normalizer.Form.NFKC;
      case "NFKD" -> Normalizer.Form.NFKD;
      default -> throw new CommandSemanticException(functionName + "(): unsupported normalization form: " + formName
          + ". Supported forms: NFC, NFD, NFKC, NFKD");
    };
  }
}
