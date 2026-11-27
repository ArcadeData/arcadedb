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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the static content handler keeps in its process-wide caches: the paths it remembers as missing, and the paths of the
 * assets it holds. Both are bounded in count, so the size of one key has to be bounded too, and only a reference a rendered
 * page makes is worth remembering as missing: a path a client asks for says nothing about what a page will reference.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GetDynamicContentHandlerPathLimitsTest extends BaseGraphServerTest {
  // One marker per test: the caches are process-wide, so a test that looks for what another one left would fail on it
  private static final String SHORT_MARKER = "path-limits-short";
  private static final String LONG_MARKER  = "path-limits-long";
  private static final String UNIT_MARKER  = "path-limits-unit";

  // HTTP/1.1: the request line is the whole point, and the client's attempt to upgrade a connection would carry it elsewhere
  private final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  /** A path asked for by a client and found missing is answered 404 and not remembered, however short. */
  @Test
  void aMissingPathAskedForByAClientIsNotRemembered() throws Exception {
    for (int i = 0; i < 20; i++) {
      final String path = "/" + SHORT_MARKER + "-" + i + "-" + "a".repeat(300) + ".js";
      assertThat(get(path).statusCode()).isEqualTo(404);
    }

    assertThat(GetDynamicContentHandler.MISSING_STATIC_CONTENT).noneMatch(key -> key.contains(SHORT_MARKER));
  }

  /** A path longer than any asset is answered 404 and nothing of it is kept. */
  @Test
  void aPathLongerThanAnyAssetIsAnsweredNotFoundAndNotKept() throws Exception {
    final int length = GetDynamicContentHandler.STATIC_PATH_MAX_LENGTH * 400;
    for (int i = 0; i < 3; i++) {
      final String path = "/" + LONG_MARKER + "-" + i + "-" + "b".repeat(length) + ".js";
      assertThat(get(path).statusCode()).isEqualTo(404);
    }
    // A page path takes the other branch of the handler
    assertThat(get("/" + LONG_MARKER + "-page-" + "c".repeat(length) + ".html").statusCode()).isEqualTo(404);

    assertThat(GetDynamicContentHandler.MISSING_STATIC_CONTENT).noneMatch(key -> key.contains(LONG_MARKER));
    assertThat(GetDynamicContentHandler.STATIC_CONTENT_CACHE.keySet()).noneMatch(key -> key.contains(LONG_MARKER));
  }

  /** An asset is still served, and cached under its path. */
  @Test
  void anAssetIsStillServedAndCached() throws Exception {
    assertThat(get("/js/cache-probe.js").statusCode()).isEqualTo(200);
    assertThat(GetDynamicContentHandler.STATIC_CONTENT_CACHE).containsKey("static/js/cache-probe.js");
  }

  /** A miss is remembered only when the caller says it comes from a page, never from a request. */
  @Test
  void aMissIsRememberedOnlyWhenTheCallerAsksForIt() throws Exception {
    final String unasked = "static/js/" + UNIT_MARKER + "-unasked.js";
    assertThat(GetDynamicContentHandler.loadStaticResource(unasked)).isNull();
    assertThat(GetDynamicContentHandler.loadStaticResource(unasked, false)).isNull();
    assertThat(GetDynamicContentHandler.MISSING_STATIC_CONTENT).doesNotContain(unasked);

    final String asked = "static/js/" + UNIT_MARKER + "-asked.js";
    try {
      assertThat(GetDynamicContentHandler.loadStaticResource(asked, true)).isNull();
      assertThat(GetDynamicContentHandler.MISSING_STATIC_CONTENT).contains(asked);
      // Remembered: the answer is the same
      assertThat(GetDynamicContentHandler.loadStaticResource(asked, true)).isNull();
    } finally {
      // The set is process-wide and bounded: leave it as it was found
      GetDynamicContentHandler.MISSING_STATIC_CONTENT.remove(asked);
    }
  }

  /** Even a page's own miss is not remembered once its path is longer than any asset's. */
  @Test
  void aMissWithAnOverlongPathIsNeverRemembered() throws Exception {
    final String path = "static/js/" + UNIT_MARKER + "-" + "d".repeat(GetDynamicContentHandler.STATIC_PATH_MAX_LENGTH) + ".js";
    assertThat(GetDynamicContentHandler.loadStaticResource(path, true)).isNull();
    assertThat(GetDynamicContentHandler.MISSING_STATIC_CONTENT).doesNotContain(path);
  }

  /**
   * An asset that exists under a spelling longer than its own (a classpath directory resolves repeated separators) is served,
   * but its spelling does not become a key.
   */
  @Test
  void anExistingAssetUnderAPaddedPathIsServedButNotCached() throws Exception {
    final String padded = "static/" + "/".repeat(GetDynamicContentHandler.STATIC_PATH_MAX_LENGTH + 1) + "js/cache-probe.js";

    final GetDynamicContentHandler.StaticAsset asset = GetDynamicContentHandler.loadStaticResource(padded, true);

    // The test classpath is a directory, which resolves the padded spelling: the assertion below is not vacuous
    assertThat(asset).as("the padded spelling resolves on a directory classpath").isNotNull();
    assertThat(GetDynamicContentHandler.STATIC_CONTENT_CACHE).doesNotContainKey(padded);
    assertThat(asset.version()).isEqualTo(GetDynamicContentHandler.loadStaticResource("static/js/cache-probe.js").version());
  }

  /** A reference a rendered page makes to an asset that is not there is still remembered, so the page does not look it up again. */
  @Test
  void aMissingAssetReferencedByAPageIsStillRemembered() throws Exception {
    final String reference = "static/js/missing-asset.js";

    assertThat(get("/stamp-probe.html").statusCode()).isEqualTo(200);

    assertThat(GetDynamicContentHandler.MISSING_STATIC_CONTENT).contains(reference);
    assertThat(GetDynamicContentHandler.loadStaticResource(reference)).isNull();
  }

  private HttpResponse<String> get(final String path) throws Exception {
    return client.send(HttpRequest.newBuilder(new URI(getServerHttpUrl(path))).GET().build(), HttpResponse.BodyHandlers.ofString());
  }
}
