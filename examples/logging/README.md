# Logging when embedding ArcadeDB

ArcadeDB routes its logs through the **SLF4J facade** when the SLF4J logger is selected:

```
-Darcadedb.log.impl=slf4j
```

The default logger is unchanged (`java.util.logging` with ArcadeDB's built-in text/ANSI and JSON
formats, selectable via `arcadedb.server.logFormat`), so standalone deployments behave exactly as
before. Selecting `slf4j` makes the engine log through `slf4j-api` only — the engine pins **no**
backend, so the embedding application picks the backend it already uses and needs **no dependency
exclusions**.

Add exactly **one** SLF4J binding and drop in the matching config file from this folder.

## Logback (`logback.xml`)

Add the binding:

```xml
<dependency>
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
</dependency>
```

`logback.xml` shows an ANSI-coloured console (equivalent to ArcadeDB's native `text` format) and a
structured JSON console using Logback's built-in `JsonEncoder` (Logback ≥ 1.5). Spring Boot pulls
in `logback-classic` by default, so embedding ArcadeDB in a Spring Boot app needs nothing extra.

## Log4j2 (`log4j2.xml`)

Add the binding and (for JSON) the template layout:

```xml
<dependency>
  <groupId>org.apache.logging.log4j</groupId>
  <artifactId>log4j-slf4j2-impl</artifactId>
</dependency>
<dependency>
  <groupId>org.apache.logging.log4j</groupId>
  <artifactId>log4j-core</artifactId>
</dependency>
<dependency>
  <groupId>org.apache.logging.log4j</groupId>
  <artifactId>log4j-layout-template-json</artifactId> <!-- only for the JSON layout -->
</dependency>
```

`log4j2.xml` shows the same two formats (ANSI console + ECS-style JSON via `JsonTemplateLayout`).

## Feature parity with the native (java.util.logging) logger

Everything ArcadeDB's built-in logger does is configured, under SLF4J, in the backend. Mapping:

| ArcadeDB native feature | Native setting | Logback | Log4j2 |
|---|---|---|---|
| Console format: text / JSON | `arcadedb.server.logFormat=text\|json` | `CONSOLE` appender (pattern) / `JSON` appender (`JsonEncoder`) | `Console` (PatternLayout) / `Json` (`JsonTemplateLayout`) |
| Log destination folder | `arcadedb.server.logsDirectory` (default `./log`) | `${LOG_DIR}` property → `-Darcadedb.logs.dir=/path` (default `./log`) | `${sys:arcadedb.logs.dir}` property → `-Darcadedb.logs.dir=/path` (default `./log`) |
| Include trace id/span on each line | `arcadedb.server.logIncludeTrace=true` | add `%X{arcadedb.traceId:-} %X{arcadedb.spanId:-}` to the pattern | add `%X{arcadedb.traceId} %X{arcadedb.spanId}` to the pattern |
| Per-package verbosity | `arcadedb-log.properties` levels | `<logger name="com.arcadedb" level="INFO"/>` | `<Logger name="com.arcadedb" level="INFO"/>` |
| Correlation fields in JSON | emitted by `JsonLogFormatter` | `JsonEncoder` includes the MDC automatically | `JsonTemplateLayout` includes the ThreadContext/MDC |
| Stack traces | printed by the formatter | printed by default (`%ex` / encoder) | printed by default (`%throwable` / template) |

The correlation field names differ slightly between the native JSON formatter and the MDC keys: the
native `JsonLogFormatter` emits `requestId`, `db`, `traceId`, `spanId`; over SLF4J the same values are
exposed in the MDC as `arcadedb.requestId`, `arcadedb.database`, `arcadedb.traceId`, `arcadedb.spanId`
(namespaced to avoid clobbering the host application's own MDC keys). Rename them in your JSON
layout/encoder if you need the exact native field names.

## Logger names and correlation (MDC)

- ArcadeDB loggers are named after the emitting class: `com.arcadedb.*`. Set levels per package.
- Per-request correlation is placed in the SLF4J **MDC** under:
  `arcadedb.requestId`, `arcadedb.database`, `arcadedb.traceId`, `arcadedb.spanId`.
  Reference them in a pattern with `%X{arcadedb.requestId}` (Logback and Log4j2 alike), or rely on
  the JSON encoders, which include the MDC automatically.

## Want plain `java.util.logging`?

You don't need the SLF4J logger for that: either keep the default logger, or select the SLF4J logger
and add the `org.slf4j:slf4j-jdk14` binding — SLF4J will forward to JUL.
