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
package com.arcadedb.server.http;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reduces an OpenAPI path template to the form a plugin actually registers it under. A template
 * with a path parameter (e.g. "/api/v1/cluster/peer/{peerId}") is registered as a PREFIX up to and
 * including the slash before the parameter ("/api/v1/cluster/peer/"), because {@code PathHandler}
 * has no concept of a named path parameter - method dispatch happens inside the handler, not in the
 * router. A template with no parameter is registered EXACTLY as written. Two templates can
 * normalize to the same prefix - e.g. both "/api/v1/ha/snapshot/{database}" and
 * "/api/v1/ha/snapshot/{database}/checksums" collapse to "/api/v1/ha/snapshot/" - because one
 * handler answers both, registered once. This means a fabricated spec path added under an
 * already-registered prefix is invisible to the per-plugin anti-drift check - the collapse trades
 * finer reverse-drift precision for matching what the router can actually distinguish.
 */
public final class RoutePathNormalizer {

  private RoutePathNormalizer() {
  }

  public static Set<String> normalize(final Set<String> templatePaths) {
    return templatePaths.stream().map(RoutePathNormalizer::normalize).collect(Collectors.toSet());
  }

  public static String normalize(final String templatePath) {
    final int paramStart = templatePath.indexOf('{');
    if (paramStart < 0)
      return templatePath;
    return templatePath.substring(0, templatePath.lastIndexOf('/', paramStart) + 1);
  }
}
