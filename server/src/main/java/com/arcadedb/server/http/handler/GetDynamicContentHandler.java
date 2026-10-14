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

import com.arcadedb.Constants;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StringUtils;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Deque;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Map.entry;

/**
 * Serves Studio: its pages, rendered by a small template engine, and its static assets.
 * <p>
 * <b>Content types (issue #9993).</b> The extension of the requested file selects its Content-Type and whether it is a
 * template, from {@link #CONTENT_TYPES}: only {@code .html}/{@code .htm} pages are. An extension missing from the table is
 * served as {@code text/plain}, so every extension Studio ships has an entry, which a test checks against the shipped tree.
 * <p>
 * <b>Browser caching (issue #9961).</b> A static asset is identified by a digest of its bytes, sent as its {@code ETag}.
 * Every page the template engine renders addresses the local assets it references with that digest
 * ({@code js/studio-table.js?v=<digest>}, see {@link #stampAssetVersions}), and only a request whose {@code v} names the
 * bytes being served is told to keep them for good ({@code immutable}): an upgrade that changes an asset changes its URL,
 * so a returning browser can never pair a fresh page with a stale script, while an unchanged asset - a vendor library
 * across most upgrades - stays cached. Any other request for an asset is told to revalidate ({@code no-cache}) and is
 * answered {@code 304 Not Modified} while its {@code If-None-Match} still names the bytes. Pages are never reused without
 * revalidating: they embed per-request content.
 */
public class GetDynamicContentHandler extends AbstractServerHttpHandler {

  /** How one extension is served: its Content-Type, and whether it is a page rendered by the template engine. */
  record ContentTypeSpec(String contentType, boolean templated) {
  }

  /**
   * The extensions served, lower case. A template is rendered on every request and interprets {@code ${...}} directives,
   * so only pages may be one: a stylesheet or a script run through it would have any {@code ${} sequence eaten.
   */
  static final Map<String, ContentTypeSpec> CONTENT_TYPES = Map.ofEntries(//
      entry("html", new ContentTypeSpec("text/html", true)),//
      entry("htm", new ContentTypeSpec("text/html", true)),//
      entry("js", new ContentTypeSpec("text/javascript", false)),//
      entry("mjs", new ContentTypeSpec("text/javascript", false)),//
      entry("json", new ContentTypeSpec("application/json", false)),//
      entry("map", new ContentTypeSpec("application/json", false)),//
      entry("css", new ContentTypeSpec("text/css", false)),//
      entry("txt", new ContentTypeSpec("text/plain", false)),//
      entry("svg", new ContentTypeSpec("image/svg+xml", false)),//
      entry("ico", new ContentTypeSpec("image/x-icon", false)),//
      entry("png", new ContentTypeSpec("image/png", false)),//
      entry("jpg", new ContentTypeSpec("image/jpeg", false)),//
      entry("jpeg", new ContentTypeSpec("image/jpeg", false)),//
      entry("gif", new ContentTypeSpec("image/gif", false)),//
      entry("webp", new ContentTypeSpec("image/webp", false)),//
      entry("avif", new ContentTypeSpec("image/avif", false)),//
      entry("woff", new ContentTypeSpec("font/woff", false)),//
      entry("woff2", new ContentTypeSpec("font/woff2", false)),//
      entry("ttf", new ContentTypeSpec("font/ttf", false)),//
      entry("otf", new ContentTypeSpec("font/otf", false)),//
      entry("wasm", new ContentTypeSpec("application/wasm", false)));

  /** What an extension missing from {@link #CONTENT_TYPES} is served as. */
  static final ContentTypeSpec UNKNOWN_CONTENT_TYPE = new ContentTypeSpec("text/plain", false);

  /** A static asset as served: its bytes, the digest that versions them, and that digest as an entity tag. */
  record StaticAsset(byte[] bytes, String version, String etag) {
  }

  /** The {@code Cache-Control} of an asset requested under the version of its bytes: that URL never names other bytes. */
  static final String CACHE_VERSIONED   = "public, max-age=31536000, immutable";
  /** The {@code Cache-Control} of everything else: stored, but revalidated before every use. */
  static final String CACHE_REVALIDATE  = "no-cache";
  /** The query parameter that carries an asset's version in the URLs {@link #stampAssetVersions} writes. */
  static final String VERSION_PARAMETER = "v";

  /**
   * A {@code src} or {@code href} attribute whose value is a plain path: no query, fragment or scheme, the shape of a
   * reference to a local asset. Group 1 is the opening quote, group 2 the value.
   */
  private static final Pattern ASSET_REFERENCE = Pattern.compile("(?<![\\w-])(?:src|href)\\s*=\\s*([\"'])([^\"'?#:\\s]+)\\1");

  // Static assets are immutable classpath resources for the process lifetime. Cache them, with their digest, so repeated
  // requests skip the getResourceAsStream + full read + toByteArray copy + hash on every hit. Bounded so a pathological set
  // of distinct URIs cannot grow it without limit. Templated pages are intentionally NOT cached: they embed per-request
  // dynamic content (now, uuid, role-gated protectBegin sections).
  private static final int                      STATIC_CACHE_MAX_ENTRIES = 512;
  private static final Map<String, StaticAsset> STATIC_CONTENT_CACHE     = new ConcurrentHashMap<>();
  // The resource paths found missing, for the same reason and with the same bound: a page that references an asset not on
  // the classpath would otherwise look it up again on every render
  private static final Set<String>              MISSING_STATIC_CONTENT   = ConcurrentHashMap.newKeySet();

  // Shown at the server root when the Studio module is not on the classpath (e.g. the "base"/"headless"
  // distributions). Self-contained (no external assets) so it renders even without Studio installed.
  private static final String                  STUDIO_NOT_BUNDLED_PAGE  = """
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>ArcadeDB Server</title>
        <style>
          body { font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background: #1b1f24;
                 color: #e6e6e6; margin: 0; display: flex; min-height: 100vh; align-items: center; justify-content: center; }
          .card { max-width: 640px; padding: 2.5rem; background: #23282f; border-radius: 12px;
                  box-shadow: 0 8px 30px rgba(0,0,0,0.35); }
          h1 { margin: 0 0 .5rem; font-size: 1.5rem; }
          p { line-height: 1.55; }
          code { background: #12151a; padding: .15rem .4rem; border-radius: 4px; font-size: .9em; }
          a { color: #4ea1ff; }
          .muted { color: #9aa4af; font-size: .9rem; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>ArcadeDB Server is running</h1>
          <p>The REST API is available at <code>/api/v1</code>, but the <strong>Studio</strong> web console is
             not bundled in this distribution.</p>
          <p>To use Studio, either:</p>
          <ul>
            <li>Download the full distribution <code>arcadedb-&lt;version&gt;.tar.gz</code> (without the
                <code>-base</code>/<code>-headless</code> suffix), or</li>
            <li>Rebuild a custom distribution including Studio:
                <code>./arcadedb-builder.sh --modules=console,studio</code>.</li>
          </ul>
          <p class="muted">See <a href="https://docs.arcadedb.com">docs.arcadedb.com</a> for details.</p>
        </div>
      </body>
      </html>
      """;

  public GetDynamicContentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload)
      throws Exception {
    String uri = exchange.getRequestURI();

    if (uri.contains(".."))
      // UNAUTHORIZED ACCESS TO RELATIVE PATH
      return new ExecutionResponse(404, "Not Found");

    if (uri.isEmpty() || "/".equals(uri))
      uri = "/index.html";

    if (!uri.contains("."))
      // NO EXTENSION + HTML PAGES
      uri += ".html";

    final ContentTypeSpec spec = contentTypeOf(uri);
    final String contentType = spec.contentType();
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, contentType);
    if ("text/html".equals(contentType))
      // A policy governs documents only: Studio's pages refuse inline script, so an HTML-injection bug cannot run any
      // (issue #9681). Scripts, styles and fonts do not need it.
      setContentSecurityPolicy(exchange, null);

    Metrics.counter("http.static-content").increment();

    final String resourcePath = "static" + uri;
    LogManager.instance().log(this, Level.FINE, "Loading file %s ", "/" + resourcePath);

    if (spec.templated()) {
      // Templated pages are re-rendered per request (they embed now/uuid/role-gated sections) and are not cached.
      final InputStream file = getClass().getClassLoader().getResourceAsStream(resourcePath);
      if (file == null) {
        final byte[] fallback = missingResourceFallback(resourcePath);
        if (fallback != null)
          return new ExecutionResponse(200, fallback);
        return new ExecutionResponse(404, "Not Found");
      }
      final Binary fileContent = FileUtils.readStreamAsBinary(file);
      file.close();
      final String page = templating(exchange, new String(fileContent.toByteArray(), DatabaseFactory.getDefaultCharset()),
          new HashMap<>());
      exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, CACHE_REVALIDATE);
      return new ExecutionResponse(200,
          stampAssetVersions(page, uri.substring(1, uri.lastIndexOf('/') + 1)).getBytes(DatabaseFactory.getDefaultCharset()));
    }

    // Immutable static asset: served from the in-process cache after the first read.
    final StaticAsset asset = loadStaticResource(resourcePath);
    if (asset == null)
      return new ExecutionResponse(404, "Not Found");

    exchange.getResponseHeaders().put(Headers.ETAG, asset.etag());
    final Deque<String> version = exchange.getQueryParameters().get(VERSION_PARAMETER);
    exchange.getResponseHeaders().put(Headers.CACHE_CONTROL,
        version != null && asset.version().equals(version.peekFirst()) ? CACHE_VERSIONED : CACHE_REVALIDATE);

    if (matchesEntityTag(exchange.getRequestHeaders().get(Headers.IF_NONE_MATCH), asset.etag()))
      return new ExecutionResponse(StatusCodes.NOT_MODIFIED, "");

    return new ExecutionResponse(200, asset.bytes());
  }

  /** How the file {@code uri} names is served, from its extension. */
  static ContentTypeSpec contentTypeOf(final String uri) {
    return CONTENT_TYPES.getOrDefault(extensionOf(uri), UNKNOWN_CONTENT_TYPE);
  }

  /**
   * Whether an {@code If-None-Match} request header names {@code etag}: one of its entity tags does, compared weakly as
   * the header requires, or it is {@code *}. Splitting on commas holds for the tags this handler issues, which are hex; an
   * entity tag may contain a comma in general, so this is not a general-purpose If-None-Match parser.
   */
  static boolean matchesEntityTag(final Iterable<String> ifNoneMatch, final String etag) {
    if (ifNoneMatch == null)
      return false;
    for (final String header : ifNoneMatch)
      for (final String tag : header.split(",")) {
        String candidate = tag.trim();
        if ("*".equals(candidate))
          return true;
        if (candidate.startsWith("W/"))
          candidate = candidate.substring(2);
        if (candidate.equals(etag))
          return true;
      }
    return false;
  }

  /**
   * Appends the version of the referenced asset to every {@code src} and {@code href} of a rendered page that names a
   * local static asset ({@code js/studio-table.js} becomes {@code js/studio-table.js?v=<digest>}), so the URL a browser
   * caches an asset under changes exactly when the asset does (issue #9961). Done on the rendered page rather than written
   * into each page by hand, so no page and no reference added later can miss it. A reference is left as written when it
   * carries a query, a fragment or a scheme (an outside origin), leaves the static tree, names a page (templated, never
   * cached) or an extension that is not served, or names no asset on the classpath.
   *
   * @param directory the directory of the page, relative to the static root, that relative references resolve against:
   *                  {@code ""} for a page at the root, otherwise ending with {@code /}
   */
  static String stampAssetVersions(final String page, final String directory) throws IOException {
    final Matcher matcher = ASSET_REFERENCE.matcher(page);
    StringBuilder stamped = null;
    int copied = 0;
    while (matcher.find()) {
      final String reference = matcher.group(2);
      final ContentTypeSpec spec = CONTENT_TYPES.get(extensionOf(reference));
      // A protocol-relative "//host/path" names another origin, like one with a scheme
      if (spec == null || spec.templated() || reference.contains("..") || reference.startsWith("//"))
        continue;

      final StaticAsset asset = loadStaticResource(
          "static/" + (reference.startsWith("/") ? reference.substring(1) : directory + reference));
      if (asset == null)
        continue;

      if (stamped == null)
        stamped = new StringBuilder(page.length() + 1024);
      final int valueEnd = matcher.end(2);
      stamped.append(page, copied, valueEnd).append('?').append(VERSION_PARAMETER).append('=').append(asset.version());
      copied = valueEnd;
    }
    if (stamped == null)
      return page;
    return stamped.append(page, copied, page.length()).toString();
  }

  /** The extension of the file a path names, lower case, or an empty string when it has none. */
  private static String extensionOf(final String path) {
    final int dot = path.lastIndexOf('.');
    return dot < 0 || dot < path.lastIndexOf('/') ? "" : path.substring(dot + 1).toLowerCase(Locale.ROOT);
  }

  /**
   * Loads an immutable classpath static asset, caching its raw bytes and their digest for the process lifetime so
   * repeated requests avoid re-reading the stream, re-copying the bytes and hashing them again. Returns {@code null} when
   * the resource does not exist. The cache is bounded; once full, further distinct resources are still served but not
   * cached. Package-private for direct unit testing.
   */
  static StaticAsset loadStaticResource(final String resourcePath) throws IOException {
    final StaticAsset cached = STATIC_CONTENT_CACHE.get(resourcePath);
    if (cached != null)
      return cached;
    if (MISSING_STATIC_CONTENT.contains(resourcePath))
      return null;

    final InputStream file = GetDynamicContentHandler.class.getClassLoader().getResourceAsStream(resourcePath);
    if (file == null) {
      if (MISSING_STATIC_CONTENT.size() < STATIC_CACHE_MAX_ENTRIES)
        MISSING_STATIC_CONTENT.add(resourcePath);
      return null;
    }

    final byte[] bytes;
    try {
      bytes = FileUtils.readStreamAsBinary(file).toByteArray();
    } finally {
      file.close();
    }

    final String version = digest(bytes);
    final StaticAsset asset = new StaticAsset(bytes, version, '"' + version + '"');
    if (STATIC_CONTENT_CACHE.size() < STATIC_CACHE_MAX_ENTRIES) {
      final StaticAsset existing = STATIC_CONTENT_CACHE.putIfAbsent(resourcePath, asset);
      return existing != null ? existing : asset;
    }
    return asset;
  }

  /** The version of an asset: the first 64 bits of the SHA-256 of its bytes, in hex. Computed once per cached asset. */
  private static String digest(final byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes), 0, 8);
    } catch (final NoSuchAlgorithmException e) {
      // Every Java platform is required to provide SHA-256
      throw new IllegalStateException(e);
    }
  }

  /**
   * Chooses the response body to serve when a requested Studio asset is not on the classpath. Studio is an
   * optional module: the "base"/"headless" distributions ship without it, so browsing to the server root would
   * otherwise return a bare "Not Found" that reads like a broken server. For the landing page request only
   * ({@code static/index.html}) this returns a small self-contained page explaining Studio is not bundled and
   * how to enable it; every other missing asset returns {@code null} so the caller emits a genuine 404.
   * Package-private for direct unit testing.
   */
  static byte[] missingResourceFallback(final String resourcePath) {
    if ("static/index.html".equals(resourcePath))
      return STUDIO_NOT_BUNDLED_PAGE.getBytes(DatabaseFactory.getDefaultCharset());
    return null;
  }

  protected String templating(final HttpServerExchange exchange, final String file, final Map<String, Object> variables)
      throws IOException {
    int beginTokenPos = file.indexOf("${");
    if (beginTokenPos < 0)
      return file;

    final StringBuilder buffer = new StringBuilder((int) (file.length() * 1.5));

    int pos = 0;
    while (pos < file.length() && beginTokenPos > -1) {
      buffer.append(file, pos, beginTokenPos);

      pos = beginTokenPos + "${".length();

      int endTokenPos = file.indexOf("}", pos);
      if (endTokenPos == -1) {
        LogManager.instance().log(this, Level.SEVERE, "Could not find end of token after position %d", beginTokenPos);
        return file;
      }

      final String command = file.substring(pos, endTokenPos);
      if (command.startsWith("include:")) {
        String include = file.substring(pos + "include:".length(), endTokenPos);

        final int sep = include.indexOf(' ');
        if (sep > -1) {
          final String params = include.substring(sep).trim();

          include = include.substring(0, sep);

          parseIncludeParameters(params, include, variables);
        }

        final InputStream fis = getClass().getClassLoader().getResourceAsStream("static/" + include);
        if (fis == null) {
          LogManager.instance().log(this, Level.WARNING, "Could not find file to include '%s' to include", include);
          pos = endTokenPos + "}".length();
          beginTokenPos = file.indexOf("${", pos);
          continue;
        }

        byte[] includeBytes = FileUtils.readStreamAsBinary(fis).toByteArray();

        // RECURSIVE
        includeBytes = templating(exchange, new String(includeBytes, DatabaseFactory.getDefaultCharset()), variables).getBytes(
            DatabaseFactory.getDefaultCharset());

        buffer.append(new String(includeBytes, DatabaseFactory.getDefaultCharset()));

      } else if (command.startsWith("var:")) {
        final int assignmentPos = command.indexOf("=");
        if (assignmentPos < 0) {
          // READ
          final String variableName = command.substring("var:".length());
          if ("swVersion".equalsIgnoreCase(variableName))
            buffer.append(Constants.getVersion());
          else if ("now".equalsIgnoreCase(variableName))
            buffer.append(System.currentTimeMillis());
          else if ("uuid".equalsIgnoreCase(variableName))
            buffer.append(UUID.randomUUID());
          else
            buffer.append(variables.get(variableName));
        } else {
          // WRITE
          final String variableName = command.substring("var:".length(), assignmentPos);
          final String variableValue = command.substring(assignmentPos + 1);
          variables.put(variableName, variableValue);
        }
      } else if (command.startsWith("protectBegin:")) {
        final String protect = file.substring(pos + "protectBegin:".length(), endTokenPos);

        pos = endTokenPos + "}".length();
        beginTokenPos = file.indexOf("${protectEnd", pos);
        endTokenPos = file.indexOf("}", beginTokenPos);

        final Set<String> currentUserRoles = exchange.getSecurityContext().getAuthenticatedAccount().getRoles();

        for (final String role : protect.split(","))
          if (currentUserRoles.contains(role)) {
            final String subContent = file.substring(pos, beginTokenPos);
            buffer.append(templating(exchange, subContent, variables));
            break;
          }

      } else
        // IGNORE IT
        buffer.append(file, beginTokenPos, endTokenPos + "}".length());

      pos = endTokenPos + "}".length();
      beginTokenPos = file.indexOf("${", pos);
    }

    buffer.append(file.substring(pos));

    return buffer.toString();
  }

  /**
   * Parses the space-separated {@code key=value} parameters of an {@code ${include:file.html k=v ...}} directive
   * into {@code variables}. Each pair is split on the FIRST '=' only, so a value containing a further '=' is kept
   * whole, and a parameter with no '=' at all is reported and skipped instead of throwing
   * {@link ArrayIndexOutOfBoundsException} out of the page render (issue #6423).
   */
  static void parseIncludeParameters(final String params, final String include, final Map<String, Object> variables) {
    // Split on a run of whitespace, not a single space: a run of consecutive spaces between parameters
    // (or a trailing one) would otherwise produce an empty-string element that logs a spurious "malformed
    // parameter ''" warning below.
    final String[] parameterPairs = params.trim().split("\\s+");
    for (final String pair : parameterPairs) {
      if (pair.isEmpty())
        continue;
      final String[] kv = StringUtils.splitKeyValue(pair);
      if (kv == null) {
        LogManager.instance().log(GetDynamicContentHandler.class, Level.WARNING,
            "Malformed include parameter '%s' in '%s', ignoring it", pair, include);
        continue;
      }
      variables.put(kv[0].trim(), kv[1].trim());
    }
  }
}
