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
package com.arcadedb.query.opencypher.functions.text;

import com.arcadedb.query.sql.executor.CommandContext;

import java.text.Normalizer;
import java.util.regex.Pattern;

/**
 * text.slug(string, [delimiter]) - Create URL-friendly slug.
 *
 * @author ArcadeDB Team
 */
public class TextSlug extends AbstractTextFunction {
  private static final Pattern NON_ASCII = Pattern.compile("[^\\p{ASCII}]");
  private static final Pattern NON_ALPHANUMERIC = Pattern.compile("[^a-zA-Z0-9]");
  private static final Pattern MULTIPLE_DASHES = Pattern.compile("-+");

  @Override
  protected String getSimpleName() {
    return "slug";
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
  public String getDescription() {
    return "Create a URL-friendly slug from the string";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String delimiter = args.length > 1 ? asString(args[1]) : "-";
    final String delim = (delimiter != null) ? delimiter : "-";

    // Normalize Unicode characters
    String normalized = Normalizer.normalize(str, Normalizer.Form.NFD);
    normalized = NON_ASCII.matcher(normalized).replaceAll("");

    // Replace non-alphanumeric with delimiter
    String slug = NON_ALPHANUMERIC.matcher(normalized).replaceAll(delim);

    // Replace multiple delimiters with single
    if ("-".equals(delim)) {
      slug = MULTIPLE_DASHES.matcher(slug).replaceAll("-");
    } else {
      slug = slug.replaceAll(Pattern.quote(delim) + "+", delim);
    }

    // Trim delimiters from ends
    slug = slug.replaceAll("^" + Pattern.quote(delim) + "|" + Pattern.quote(delim) + "$", "");

    return slug.toLowerCase();
  }
}
