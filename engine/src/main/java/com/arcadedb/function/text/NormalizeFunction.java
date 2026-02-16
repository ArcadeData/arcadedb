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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.text.Normalizer;

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
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 1 || args.length > 2)
      throw new CommandExecutionException("normalize() requires 1 or 2 arguments");
    if (args[0] == null)
      return null;

    final String input = args[0].toString();
    final Normalizer.Form form;
    if (args.length > 1 && args[1] != null) {
      final String formName = args[1].toString().toUpperCase();
      form = switch (formName) {
        case "NFC" -> Normalizer.Form.NFC;
        case "NFD" -> Normalizer.Form.NFD;
        case "NFKC" -> Normalizer.Form.NFKC;
        case "NFKD" -> Normalizer.Form.NFKD;
        default -> throw new CommandExecutionException("normalize(): unsupported normalization form: " + formName
            + ". Supported forms: NFC, NFD, NFKC, NFKD");
      };
    } else {
      form = Normalizer.Form.NFC;
    }
    return Normalizer.normalize(input, form);
  }
}
