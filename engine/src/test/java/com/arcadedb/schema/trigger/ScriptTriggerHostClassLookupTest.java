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
package com.arcadedb.schema.trigger;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GHSA-wx28-2265-f788: the trigger host-class-lookup allow-list was matched with
 * {@link String#matches(String)}, so every entry was compiled as a regular expression. The unescaped dots
 * in {@code "java.util.*"} made it match anything shaped {@code java<any char>util<anything>} - including
 * {@code java.util.zip.ZipFile} and {@code java.util.jar.JarFile}, which take a raw filesystem path in their
 * constructor and hand out an {@code InputStream}. That gave a JavaScript trigger an arbitrary host-file-read
 * primitive that neither {@code IOAccess.NONE} nor the reflection deny-list intercepts.
 * <p>
 * After the fix the allow-list is matched literally ({@code pkg.*} = that package only, {@code pkg.**} =
 * recursive, anything else = exact class name) and an unconditional deny-list rejects the I/O-capable class
 * families even when they sit inside an otherwise allowed package.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ScriptTriggerHostClassLookupTest extends TestHelper {

  private static final String SECRET = "TOP-SECRET-CREDENTIAL-42";

  @Test
  void triggerCannotReadHostFilesThroughZipFile() throws IOException {
    final Path archive = createArchiveWithSecret();
    try {
      database.command("sql", "CREATE DOCUMENT TYPE ZipTarget");
      database.command("sql", """
          CREATE TRIGGER zip_read BEFORE CREATE ON TYPE ZipTarget EXECUTE JAVASCRIPT '\
          var ZF = Java.type("java.util.zip.ZipFile");\
          var zf = new ZF("%s");\
          var stream = zf.getInputStream(zf.getEntry("secret.txt"));\
          var out = "";\
          var b = stream.read();\
          while (b >= 0) { out = out + String.fromCharCode(b); b = stream.read(); }\
          record.set("leaked", out);\
          true;'""".formatted(archive.toString().replace("\\", "\\\\")));

      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("ZipTarget").set("x", 1).save()))
          .isInstanceOf(RuntimeException.class)
          .hasMessageNotContaining(SECRET);

      assertThat(database.query("sql", "SELECT FROM ZipTarget").hasNext()).isFalse();
    } finally {
      Files.deleteIfExists(archive);
    }
  }

  @Test
  void triggerCannotLookupJarFile() {
    assertLookupDenied("JarTarget", "jar_read", "java.util.jar.JarFile");
  }

  @Test
  void triggerCannotLookupFileHandler() {
    assertLookupDenied("LogTarget", "log_write", "java.util.logging.FileHandler");
  }

  @Test
  void triggerCannotLookupFormatterThatTruncatesAFile() {
    // new Formatter(String fileName) creates/truncates an arbitrary path: java.util.Formatter sits directly in
    // java.util but is an I/O class, so the literal-prefix fix alone would still admit it.
    assertLookupDenied("FormatTarget", "format_write", "java.util.Formatter");
  }

  @Test
  void triggerCannotLookupConcurrentThreadPool() {
    assertLookupDenied("PoolTarget", "pool_spawn", "java.util.concurrent.ThreadPoolExecutor");
  }

  @Test
  void triggerCannotLookupServiceLoader() {
    assertLookupDenied("SpiTarget", "spi_load", "java.util.ServiceLoader");
  }

  @Test
  void triggerCannotLookupClassesShapedLikeTheOldRegex() {
    // "javaXutilY" used to satisfy the regex "java.util.*" whatever X and Y were. There is no such class in
    // the JDK, but the check must now reject the shape outright rather than depend on the name not existing.
    assertLookupDenied("RegexTarget", "regex_shape", "javaxutil.Anything");
  }

  @Test
  void triggerCanStillUseValueClasses() {
    database.command("sql", "CREATE DOCUMENT TYPE ValueTarget");
    database.command("sql", """
        CREATE TRIGGER value_ok BEFORE CREATE ON TYPE ValueTarget EXECUTE JAVASCRIPT '\
        var BigDecimal = Java.type("java.math.BigDecimal");\
        var LocalDate = Java.type("java.time.LocalDate");\
        var Formatter = Java.type("java.time.format.DateTimeFormatter");\
        var ArrayList = Java.type("java.util.ArrayList");\
        var Collectors = Java.type("java.util.stream.Collectors");\
        var list = new ArrayList();\
        list.add("a");\
        record.set("amount", new BigDecimal("10.5").multiply(new BigDecimal("2")).toString());\
        record.set("day", LocalDate.of(2026, 7, 28).format(Formatter.ISO_DATE));\
        record.set("size", list.size());\
        true;'""");

    database.transaction(() -> database.newDocument("ValueTarget").set("x", 1).save());

    final var result = database.query("sql", "SELECT FROM ValueTarget").next();
    assertThat(result.<String>getProperty("amount")).isEqualTo("21.0");
    assertThat(result.<String>getProperty("day")).isEqualTo("2026-07-28");
    assertThat(result.<Number>getProperty("size").intValue()).isEqualTo(1);
  }

  private void assertLookupDenied(final String typeName, final String triggerName, final String className) {
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.command("sql",
        "CREATE TRIGGER " + triggerName + " BEFORE CREATE ON TYPE " + typeName + " EXECUTE JAVASCRIPT 'var t = Java.type(\""
            + className + "\"); record.set(\"resolved\", true); true;'");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(typeName).set("x", 1).save()))
        .isInstanceOf(RuntimeException.class);

    assertThat(database.query("sql", "SELECT FROM " + typeName).hasNext()).isFalse();
  }

  private Path createArchiveWithSecret() throws IOException {
    final Path archive = Files.createTempFile("arcadedb-ghsa-wx28-", ".zip");
    try (final ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive))) {
      zip.putNextEntry(new ZipEntry("secret.txt"));
      zip.write(SECRET.getBytes(UTF_8));
      zip.closeEntry();
    }
    return archive;
  }
}
