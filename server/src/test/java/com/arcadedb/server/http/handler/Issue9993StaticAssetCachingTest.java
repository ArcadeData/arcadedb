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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9993 and #9961: how Studio's static content is typed and cached.
 * <ul>
 * <li>#9993: the {@code .css} branch of the content-type chain left the file templated, so a stylesheet got neither the
 * byte cache nor any caching header and was run through the HTML template engine, and {@code .png} had no branch and was
 * served as {@code text/plain}. The chain is a table now, checked against the extensions Studio ships.</li>
 * <li>#9961: every asset was cached by browsers for a day under a URL no upgrade changes, so a returning browser paired a
 * fresh page with stale scripts. An asset is now versioned by a digest of its bytes: rendered pages address it with that
 * digest, only the URL naming the current bytes may be kept for good, and every other request revalidates against the
 * {@code ETag}.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9993StaticAssetCachingTest extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  // ---------------------------------------------------------------------------------------------------------------------
  // #9993: the content-type table
  // ---------------------------------------------------------------------------------------------------------------------

  /**
   * No extension Studio ships falls to the {@code text/plain} default by accident, and none but a page is templated. Reads
   * the Studio sources, and the built tree with its vendor bundles when it is there (the Studio module's own test checks
   * that one on every build).
   */
  @Test
  void everyShippedExtensionHasAnEntryAndOnlyPagesAreTemplated() throws IOException {
    final Map<String, Path> shipped = new TreeMap<>();
    for (final Path root : new Path[] { Path.of("..", "studio", "src", "main", "resources", "static"),
        Path.of("..", "studio", "target", "classes", "static") })
      if (Files.isDirectory(root))
        try (final Stream<Path> files = Files.walk(root)) {
          files.filter(Files::isRegularFile).forEach(file -> {
            final String name = file.getFileName().toString();
            final int dot = name.lastIndexOf('.');
            if (dot > 0)
              shipped.putIfAbsent(name.substring(dot + 1).toLowerCase(), file);
          });
        }
    assertThat(shipped).as("the Studio sources are read").containsKeys("html", "js", "css", "png", "svg");

    for (final Map.Entry<String, Path> extension : shipped.entrySet()) {
      final GetDynamicContentHandler.ContentTypeSpec spec = GetDynamicContentHandler.CONTENT_TYPES.get(extension.getKey());
      assertThat(spec).as("Content-Type of .%s, shipped as %s", extension.getKey(), extension.getValue()).isNotNull();
      assertThat(spec.templated()).as("only a page is templated: .%s", extension.getKey())
          .isEqualTo(extension.getKey().startsWith("htm"));
    }
  }

  @Test
  void aStylesheetIsAStaticAssetNotATemplate() throws Exception {
    final HttpResponse<String> response = get("/css/cache-probe.css", null);
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type")).contains("text/css");
    assertThat(response.headers().firstValue("ETag")).isPresent();
    // Served as written: the template engine would have replaced the directive with the server version
    assertThat(response.body()).contains("${var:swVersion}");
  }

  @Test
  void aPngIsServedAsAnImage() throws Exception {
    final HttpResponse<byte[]> response = client.send(HttpRequest.newBuilder(new URI(getServerHttpUrl("/images/cache-probe.png")))
        .GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type")).contains("image/png");
    assertThat(response.body()).isEqualTo(
        Files.readAllBytes(Path.of("src", "test", "resources", "static", "images", "cache-probe.png")));
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // #9961: versioned URLs, revalidation
  // ---------------------------------------------------------------------------------------------------------------------

  @Test
  void anAssetRequestedWithoutItsVersionIsRevalidated() throws Exception {
    final HttpResponse<String> response = get("/js/cache-probe.js", null);
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Cache-Control")).contains("no-cache");
    assertThat(response.headers().firstValue("ETag")).contains(etagOf("static/js/cache-probe.js"));
  }

  @Test
  void onlyTheUrlNamingTheCurrentBytesIsCachedForGood() throws Exception {
    final String version = GetDynamicContentHandler.loadStaticResource("static/js/cache-probe.js").version();

    final HttpResponse<String> current = get("/js/cache-probe.js?v=" + version, null);
    assertThat(current.statusCode()).isEqualTo(200);
    assertThat(current.headers().firstValue("Cache-Control")).contains("public, max-age=31536000, immutable");

    // A URL naming other bytes - a page of another build - must not be told to keep these for good
    final HttpResponse<String> other = get("/js/cache-probe.js?v=0123456789abcdef", null);
    assertThat(other.statusCode()).isEqualTo(200);
    assertThat(other.headers().firstValue("Cache-Control")).contains("no-cache");
  }

  @Test
  void aRevalidationOfUnchangedBytesIsAnsweredNotModified() throws Exception {
    final String etag = etagOf("static/css/cache-probe.css");

    final HttpResponse<String> notModified = get("/css/cache-probe.css", etag);
    assertThat(notModified.statusCode()).isEqualTo(304);
    assertThat(notModified.body()).isEmpty();
    // A 304 may only declare the length of the representation it stands for (RFC 9110): it declares none
    assertThat(notModified.headers().firstValue("Content-Length")).isEmpty();
    assertThat(notModified.headers().firstValue("ETag")).contains(etag);
    assertThat(notModified.headers().firstValue("Cache-Control")).contains("no-cache");

    // A list, and a weak comparison, as If-None-Match allows
    assertThat(get("/css/cache-probe.css", "\"other\", W/" + etag).statusCode()).isEqualTo(304);
    // Bytes that changed since: the whole asset
    final HttpResponse<String> changed = get("/css/cache-probe.css", "\"0123456789abcdef\"");
    assertThat(changed.statusCode()).isEqualTo(200);
    assertThat(changed.body()).contains("#123456");

    // The connection that answered 304 serves the next request
    for (int i = 0; i < 3; i++) {
      assertThat(get("/css/cache-probe.css", etag).statusCode()).isEqualTo(304);
      assertThat(get("/js/cache-probe.js", null).statusCode()).isEqualTo(200);
    }
  }

  /** A rendered page addresses each local asset it references by the version of its bytes, and leaves the rest alone. */
  @Test
  void aRenderedPageAddressesItsAssetsByVersion() throws Exception {
    final String js = GetDynamicContentHandler.loadStaticResource("static/js/cache-probe.js").version();
    final String css = GetDynamicContentHandler.loadStaticResource("static/css/cache-probe.css").version();
    final String png = GetDynamicContentHandler.loadStaticResource("static/images/cache-probe.png").version();

    final HttpResponse<String> response = get("/stamp-probe.html", null);
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Cache-Control")).as("a page is never reused without revalidating")
        .contains("no-cache");

    final String page = response.body();
    assertThat(page).contains("href=\"css/cache-probe.css?v=" + css + "\"")//
        .contains("href='images/cache-probe.png?v=" + png + "'")//
        .contains("<img src=\"images/cache-probe.png?v=" + png + "\"")//
        .contains("<script src=\"js/cache-probe.js?v=" + js + "\"")//
        .contains("<script src=\"/js/cache-probe.js?v=" + js + "\"");
    // Left as written: a query of its own, no asset behind it, outside the static tree, an outside origin, a fragment, a
    // page, an attribute the browser does not load
    assertThat(page).contains("src=\"js/cache-probe.js?v=pinned\"")//
        .contains("src=\"js/missing-asset.js\"")//
        .contains("src=\"../js/cache-probe.js\"")//
        .contains("src=\"//cdn.example.com/js/cache-probe.js\"")//
        .contains("href=\"https://fonts.googleapis.com/css2?family=Poppins&display=swap\"")//
        .contains("href=\"#top\"")//
        .contains("href=\"index.html\"")//
        .contains("data-href=\"js/cache-probe.js\"");
  }

  /** A relative reference resolves against the directory of its page, an absolute one against the static root. */
  @Test
  void aReferenceResolvesAgainstThePageDirectory() throws IOException {
    final String version = GetDynamicContentHandler.loadStaticResource("static/sub/sub-probe.js").version();
    assertThat(GetDynamicContentHandler.stampAssetVersions("<script src=\"sub-probe.js\"></script>", "sub/")).isEqualTo(
        "<script src=\"sub-probe.js?v=" + version + "\"></script>");
    assertThat(GetDynamicContentHandler.stampAssetVersions("<script src=\"sub-probe.js\"></script>", "")).as(
        "no such asset at the root").isEqualTo("<script src=\"sub-probe.js\"></script>");
    assertThat(GetDynamicContentHandler.stampAssetVersions("<script src=\"/sub/sub-probe.js\"></script>", "other/")).isEqualTo(
        "<script src=\"/sub/sub-probe.js?v=" + version + "\"></script>");
  }

  /** The version is a digest of the bytes, so two different assets never share one and one asset keeps it. */
  @Test
  void theVersionIsADigestOfTheBytes() throws IOException {
    final GetDynamicContentHandler.StaticAsset js = GetDynamicContentHandler.loadStaticResource("static/js/cache-probe.js");
    final GetDynamicContentHandler.StaticAsset css = GetDynamicContentHandler.loadStaticResource("static/css/cache-probe.css");
    assertThat(js.version()).isNotEqualTo(css.version());
    assertThat(new String(js.bytes(), StandardCharsets.UTF_8)).contains("cache-probe");
    assertThat(GetDynamicContentHandler.loadStaticResource("static/js/cache-probe.js").version()).isEqualTo(js.version());
  }

  private static String etagOf(final String resourcePath) throws IOException {
    return GetDynamicContentHandler.loadStaticResource(resourcePath).etag();
  }

  private HttpResponse<String> get(final String path, final String ifNoneMatch) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(getServerHttpUrl(path))).GET();
    if (ifNoneMatch != null)
      builder.header("If-None-Match", ifNoneMatch);
    return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }
}
