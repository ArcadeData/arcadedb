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

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.text.Normalizer;

/**
 * isNormalized(input, [normalForm]) - whether the given string is already in the requested Unicode normal form,
 * defaulting to NFC. The boolean counterpart of {@code normalize()}, and the pair Neo4j exposes; the accepted form
 * names (NFC, NFD, NFKC, NFKD) come from {@link NormalizeFunction#parseNormalForm} so the two cannot diverge.
 * <p>
 * The name had been registered as known to the Cypher parser since before any executor existed, so a call parsed and
 * then failed at execution with "Unknown function". Issue #5602.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IsNormalizedFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "isNormalized";
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
    // Cypher null semantics: a null input propagates rather than answering false, which would make "not normalized"
    // indistinguishable from "no value". The normal form propagates for the same reason (issue #5629).
    if (args[0] == null || CypherFunctionHelper.isExplicitNull(args, 1))
      return null;

    // STRING-only, like normalize(): asking whether a number is in NFC form is a type error, not a false.
    if (!(args[0] instanceof CharSequence))
      throw CypherFunctionHelper.typeMismatch("isNormalized", "a STRING", args[0]);

    return Normalizer.isNormalized(args[0].toString(),
        NormalizeFunction.parseNormalForm(args.length > 1 ? args[1] : null, "isNormalized"));
  }
}
