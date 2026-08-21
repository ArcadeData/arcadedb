# Spec-Driven Client Generation, Milestone 0 - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ArcadeDB OpenAPI spec and gRPC proto into a versioned, self-identifying, downloadable contract that a generated client can be built from.

**Architecture:** Milestone 0 of the rescoped Epic #4894. All work is in `ArcadeData/arcadedb`. Three small spec-fidelity fixes make the generated types correct and self-identifying, then one workflow publishes the contract as GitHub Release assets so the future `ArcadeData/arcadedb-clients` repository has something versioned to generate against. No client code is written here.

**Tech Stack:** Java 21, swagger-models (`io.swagger.v3.oas.models`), JUnit 5 + AssertJ, Maven, GitHub Actions, Docker.

**Spec:** `docs/superpowers/specs/2026-08-21-spec-driven-client-generation-4894-design.md`

## Global Constraints

- **Do not commit without the maintainer's go-ahead.** `CLAUDE.md` states "do not commit on git, I will do it after a review." Every task below ends with a commit step; confirm with the maintainer before the first one, then follow whatever cadence they set.
- **Never add Claude as an author** of any source code or commit.
- **No em dash characters (`—`)** in any file. Use a normal dash, a comma, or rephrase.
- **Java style:** use `final` on variables and parameters; single-statement `if` bodies need no braces; import classes rather than using fully-qualified names.
- **Test style:** `assertThat(x.isFoo()).isTrue();` (AssertJ). Every assertion carries an `.as(...)` explaining what breaks if it fails, matching the existing openapi contributor tests.
- **License headers:** every new `.java` file needs the Apache 2.0 header used by its siblings. **GitHub workflow files must NOT have one.**
- **Maven, subset builds need `-am`:** `./mvnw -o -pl server -am test`. Without `-am`, Maven resolves `arcadedb-engine` from `~/.m2` instead of the working tree, and a behaviour-only change then silently tests a stale artifact.
- **Never pass `-Dtest` to skip a class.** It replaces Surefire's include patterns and drags `*IT` classes into the `test` phase, where they fail by the hundred. Use `-DexcludedGroups=benchmark,vector` instead.
- **Integration tests are skipped unless `-DskipITs=false`.**
- **Server tests bind port 2480.** Run `lsof -nP -iTCP:2480 -sTCP:LISTEN` before believing a wall of red in the `server` module; a stray server makes failures read as `403 Too many failed authentication attempts`.
- **Count results from Maven's `Results:` summary**, not by aggregating `surefire-reports/*.txt`.

---

## File Map

| File | Responsibility | Task |
|---|---|---|
| GitHub issues #4894, #4897-#4903 | Epic restructuring; new M0 tracking issues | 1 |
| `server/src/main/java/com/arcadedb/server/http/handler/OpenApiSpecGenerator.java` | Stamp the real build version into `info.version` | 2 |
| `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java` | Assert the served spec identifies its server release | 2 |
| `server/src/main/java/com/arcadedb/server/http/handler/openapi/SpecBuilders.java` | New `headerParam` and `stringHeader` builders | 3 |
| `server/src/test/java/com/arcadedb/server/http/handler/openapi/SpecBuildersTest.java` | Cover the new builders | 3 |
| `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java` | Model `arcadedb-session-id` on the transaction-bearing operations | 3 |
| `server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java` | Assert the session header is declared where it belongs | 3 |
| `server/src/main/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpec.java` | Model PromQL `data.result` as a `oneOf` over its three shapes | 4 |
| `server/src/test/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpecTest.java` | Assert the three shapes are declared | 4 |
| `.github/workflows/publish-contract.yml` | Publish the OpenAPI spec and proto as release assets | 5 |

---

## Task 1: Restructure Epic #4894 and file the M0 tracking issues

**Context:** This task comes first because tasks 2 through 5 use `feat(#N)` and `fix(#N)` commit messages that reference the issues created here. It is project-management work with no code and no test cycle; its verification is that the issues read correctly.

**Files:** none. All changes are GitHub issues via `gh`.

**Interfaces:**
- Consumes: nothing.
- Produces: four issue numbers, referred to below as `$ISSUE_VERSION` (M0-1), `$ISSUE_SESSION` (M0-6), `$ISSUE_PROMQL` (M0-3), and `$ISSUE_CONTRACT` (M0-2). Record them at the top of your working notes; later tasks need them.

- [ ] **Step 1: Read the current Epic body so nothing worth keeping is lost**

```bash
gh issue view 4894 --repo ArcadeData/arcadedb --json body -q .body > /tmp/4894-old-body.md
wc -l /tmp/4894-old-body.md
```

Expected: the existing Epic body, roughly 90 lines. Keep the "Two specs, two personas" and "Non-goals" sections; they survive the rescope unchanged.

- [ ] **Step 2: Write the new Epic body**

Write `/tmp/4894-new-body.md` containing the M0/M1/M2/M3+ structure from the spec. It must state, at minimum: the two-repository topology and why (#4896 already gates spec-versus-routes, so a client repo risks staleness rather than incorrectness); that the contract is published as versioned release assets; the M0 checklist referencing the four issues created in step 4; that M1 is the TypeScript client for HTTP and gRPC in `ArcadeData/arcadedb-clients`; that M2 is the non-blocking smoke job; and that M3+ languages are deliberately undesigned until M1 and M2 are proven. Link the spec document.

- [ ] **Step 3: Apply the new body**

```bash
gh issue edit 4894 --repo ArcadeData/arcadedb --body-file /tmp/4894-new-body.md
```

Expected: `https://github.com/ArcadeData/arcadedb/issues/4894`.

- [ ] **Step 4: File the four M0 issues**

```bash
gh issue create --repo ArcadeData/arcadedb \
  --title "OpenAPI: info.version is hardcoded to 1.0.0, so the spec cannot identify its server release" \
  --label enhancement \
  --body "Part of #4894 (M0-1). \`OpenApiSpecGenerator.createApiInfo()\` calls \`info.setVersion(\"1.0.0\")\`, so a client generated from the spec has no way to record which server release produced it. Set it from \`Constants.getVersion()\`."

gh issue create --repo ArcadeData/arcadedb \
  --title "OpenAPI: the arcadedb-session-id header is undocumented, so generated clients cannot use transactions" \
  --label bug \
  --body "Part of #4894 (M0-6). \`PostBeginHandler\` returns \`arcadedb-session-id\` as a response header and \`AbstractServerHttpHandler\` reads it as a request header, but the spec models it nowhere: not on \`beginTransaction\`, \`commitTransaction\`, \`rollbackTransaction\`, \`executeQueryPost\`, or \`executeCommand\`. A client generated from the spec therefore cannot open, use, or close a transaction in a typed way. Note this is invisible to the #4896 anti-drift test, which compares routes rather than payload and header shapes."

gh issue create --repo ArcadeData/arcadedb \
  --title "OpenAPI: PromQL data.result is an opaque object array, so generated clients get untyped results" \
  --label enhancement \
  --body "Part of #4894 (M0-3). \`PrometheusApiSpec.createDataResponseSchema()\` declares \`data.result\` as an array of bare \`type: object\`, with the three real shapes (vector, matrix, scalar) described only in prose. Model them as a \`oneOf\` so a generated client can narrow on \`resultType\`."

gh issue create --repo ArcadeData/arcadedb \
  --title "Publish the OpenAPI spec and gRPC proto as versioned release assets" \
  --label enhancement \
  --body "Part of #4894 (M0-2). The OpenAPI spec exists only behind a running server and the proto only in the source tree, so the \`arcadedb-clients\` repository has no versioned contract to generate against. Publish both, with sha256 files, as GitHub Release assets on \`release: [published]\`, reusing the \`gh release upload\` pattern already in \`native-image.yml\`."
```

Expected: four issue URLs. Record the numbers.

- [ ] **Step 5: Close the superseded child issues**

For each of 4897, 4898, 4899, 4900, 4901, 4902, 4903, post a comment saying what replaced it, then close as not planned. #4897 and #4900 merge into the single M1 TypeScript deliverable; #4899 splits into the proto half of M0-2 plus `buf generate` in the clients repo; #4898, #4901, #4902 and #4903 defer to M3+.

```bash
for n in 4897 4898 4899 4900 4901 4902 4903; do
  gh issue comment "$n" --repo ArcadeData/arcadedb \
    --body "Superseded by the rescope of #4894. See the design at \`docs/superpowers/specs/2026-08-21-spec-driven-client-generation-4894-design.md\`. Closing in favour of the milestone structure tracked on the Epic."
  gh issue close "$n" --repo ArcadeData/arcadedb --reason "not planned"
done
```

Expected: seven comments and seven closures.

- [ ] **Step 6: Verify the Epic renders correctly**

```bash
gh issue view 4894 --repo ArcadeData/arcadedb
```

Expected: the M0 checklist links the four new issues, and the closed children show as struck through where referenced.

---

## Task 2: Stamp the real server version into the OpenAPI spec

**Context:** `info.version` is hardcoded to `"1.0.0"`. A generated client must be able to record which server release produced its types, and Task 5's workflow gates on this value matching the release tag.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/OpenApiSpecGenerator.java` (the `createApiInfo()` method, around line 110-128)
- Test: `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java`

**Interfaces:**
- Consumes: `$ISSUE_VERSION` from Task 1.
- Produces: the served spec's `info.version` equals `com.arcadedb.Constants.getVersion()`. Task 5's workflow depends on this.

**Why an IT rather than a unit test:** `OpenApiSpecGenerator`'s constructor takes an `HttpServer`, and `createApiInfo()` is private, so there is no seam for a unit test. `OpenApiSpecGenerationIT` already boots a server and fetches the spec over HTTP, which is also the exact path Task 5's workflow uses.

- [ ] **Step 1: Write the failing test**

Add to `OpenApiSpecGenerationIT`, and add `import com.arcadedb.Constants;` to the imports:

```java
  @Test
  void specDeclaresTheRunningServerVersion() throws Exception {
    final String specContent = getOpenApiSpec();
    final SwaggerParseResult result = new OpenAPIV3Parser().readContents(specContent, null, new ParseOptions());
    final Info info = result.getOpenAPI().getInfo();

    assertThat(info.getVersion())
        .as("a generated client must record which server release produced its types")
        .isEqualTo(Constants.getVersion());

    assertThat(info.getVersion())
        .as("the placeholder must be gone, not merely coincidentally equal")
        .isNotEqualTo("1.0.0");
  }
```

`Info`, `OpenAPIV3Parser`, `ParseOptions`, `SwaggerParseResult` and `getOpenApiSpec()` are already present in this file.

- [ ] **Step 2: Run the test to verify it fails**

```bash
lsof -nP -iTCP:2480 -sTCP:LISTEN
./mvnw -o -pl server -am verify -DskipITs=false -Dit.test=OpenApiSpecGenerationIT -DfailIfNoTests=false
```

Expected: FAIL on `specDeclaresTheRunningServerVersion`, reporting `expected: "26.9.1-SNAPSHOT" but was: "1.0.0"` (the exact version follows the current `pom.xml`). Confirm from Maven's `Results:` summary that the IT actually ran; a run reporting zero tests is a harness problem, not a pass.

- [ ] **Step 3: Write the minimal implementation**

In `OpenApiSpecGenerator.java`, add `import com.arcadedb.Constants;` and change the one line in `createApiInfo()`:

```java
    info.setVersion(Constants.getVersion());
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
./mvnw -o -pl server -am verify -DskipITs=false -Dit.test=OpenApiSpecGenerationIT -DfailIfNoTests=false
```

Expected: PASS, with the full `OpenApiSpecGenerationIT` suite green (15 tests before this change, 16 after).

- [ ] **Step 5: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/OpenApiSpecGenerator.java \
        server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java
git commit -m "feat(#$ISSUE_VERSION): stamp the running server version into the OpenAPI spec"
```

---

## Task 3: Model the `arcadedb-session-id` header

**Context:** `PostBeginHandler` returns the session id as a response header and `AbstractServerHttpHandler` reads it as a request header, but the spec models it nowhere. Without it a generated client cannot open, use, or close a transaction in a typed way. This is the single change that unblocks the `transaction(fn)` facade in M1.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/SpecBuilders.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/SpecBuildersTest.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java`

**Interfaces:**
- Consumes: `$ISSUE_SESSION` from Task 1.
- Produces: `SpecBuilders.headerParam(String name, String description, boolean required)` returning `io.swagger.v3.oas.models.parameters.Parameter`, and `SpecBuilders.stringHeader(String description)` returning `io.swagger.v3.oas.models.headers.Header`. Both are used only by `CoreApiSpec` today but are domain-free and available to any contributor.

- [ ] **Step 1: Determine whether the header is mandatory on commit and rollback**

Do not guess this. Handler shapes in this codebase routinely contradict what the route name implies; the #4895 effort produced 38 such corrections, every one found by reading the handler.

```bash
sed -n '1,120p' server/src/main/java/com/arcadedb/server/http/handler/PostCommitHandler.java
sed -n '1,120p' server/src/main/java/com/arcadedb/server/http/handler/PostRollbackHandler.java
sed -n '235,255p;435,450p' server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java
```

Decide: if a commit or rollback without the header is rejected, declare the parameter `required = true`; if it is tolerated, declare `required = false`. Record which you found and why in the commit message. On `executeQueryPost` and `executeCommand` the parameter is **always** `required = false`, because calls outside a transaction must remain legal.

- [ ] **Step 2: Write the failing builder test**

Add to `SpecBuildersTest`:

```java
  @Test
  void headerParamDeclaresAStringHeaderParameter() {
    final Parameter param = SpecBuilders.headerParam("x-example", "An example header", true);

    assertThat(param.getIn())
        .as("a header parameter that is not in:header binds as a query string by default")
        .isEqualTo("header");
    assertThat(param.getRequired()).isTrue();
    assertThat(param.getSchema().getType()).isEqualTo("string");
  }

  @Test
  void stringHeaderDescribesAResponseHeader() {
    final Header header = SpecBuilders.stringHeader("An example response header");

    assertThat(header.getDescription()).isEqualTo("An example response header");
    assertThat(header.getSchema().getType()).isEqualTo("string");
  }
```

Add `import io.swagger.v3.oas.models.headers.Header;` and `import io.swagger.v3.oas.models.parameters.Parameter;` to the test if not already present.

- [ ] **Step 3: Run to verify it fails**

```bash
./mvnw -o -pl server -am test -Dsurefire.includes='**/openapi/SpecBuildersTest.java'
```

Expected: compilation failure, `cannot find symbol: method headerParam`.

- [ ] **Step 4: Implement the builders**

In `SpecBuilders.java`, add `import io.swagger.v3.oas.models.headers.Header;` and these two methods next to `queryParam`:

```java
  public static Parameter headerParam(final String name, final String description, final boolean required) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("header");
    param.setRequired(required);
    param.setDescription(description);
    param.setSchema(new Schema<>().type("string"));
    return param;
  }

  public static Header stringHeader(final String description) {
    final Header header = new Header();
    header.setDescription(description);
    header.setSchema(new Schema<>().type("string"));
    return header;
  }
```

- [ ] **Step 5: Run to verify the builder tests pass**

```bash
./mvnw -o -pl server -am test -Dsurefire.includes='**/openapi/SpecBuildersTest.java'
```

Expected: PASS.

- [ ] **Step 6: Write the failing CoreApiSpec tests**

Add to `CoreApiSpecTest`. Note these assert the **literal** header string rather than importing `HttpSessionManager.ARCADEDB_SESSION_ID`: the wire name is the contract, and a test that reads the same constant as the production code would stay green through a rename that breaks every client.

```java
  @Test
  void beginDeclaresTheSessionIdResponseHeader() {
    final Operation post = openAPI.getPaths().get("/api/v1/begin/{database}").getPost();

    assertThat(post.getResponses().get("200").getHeaders())
        .as("a client that cannot read the session id cannot use the transaction it just opened")
        .containsKey("arcadedb-session-id");
  }

  @Test
  void commitAndRollbackDeclareTheSessionIdRequestHeader() {
    for (final String path : List.of("/api/v1/commit/{database}", "/api/v1/rollback/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      final Parameter header = post.getParameters().stream()
          .filter(p -> "arcadedb-session-id".equals(p.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no session header declared on " + path));

      assertThat(header.getIn())
          .as("the session id travels as a header on " + path)
          .isEqualTo("header");
    }
  }

  @Test
  void queryAndCommandAcceptAnOptionalSessionIdHeader() {
    for (final String path : List.of("/api/v1/query/{database}", "/api/v1/command/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      final Parameter header = post.getParameters().stream()
          .filter(p -> "arcadedb-session-id".equals(p.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no session header declared on " + path));

      assertThat(header.getRequired())
          .as("running outside a transaction must remain legal on " + path)
          .isFalse();
    }
  }
```

`List`, `Operation` and `Parameter` are already imported in this file.

- [ ] **Step 7: Run to verify they fail**

```bash
./mvnw -o -pl server -am test -Dsurefire.includes='**/openapi/CoreApiSpecTest.java'
```

Expected: three failures. `beginDeclaresTheSessionIdResponseHeader` fails with the headers map null or empty; the other two fail with `AssertionError: no session header declared on ...`.

- [ ] **Step 8: Implement the spec changes**

In `CoreApiSpec.java`, add these imports:

```java
import com.arcadedb.server.http.HttpSessionManager;
import io.swagger.v3.oas.models.responses.ApiResponses;
```

(`ApiResponses` may already be imported. `Parameter` is already imported.)

Add these constants to the class:

```java
  private static final String SESSION_HEADER = HttpSessionManager.ARCADEDB_SESSION_ID;

  private static final String SESSION_REQUEST_DESCRIPTION = """
      Session id returned by 'beginTransaction'. Present it on every call that must run inside that \
      transaction, and on the commit or rollback that ends it. Omit it to run outside a transaction.""";
```

Add the begin-specific response builder next to `createTransactionResponses()`:

```java
  /**
   * The transaction responses plus the session-id header that 'begin' alone returns. Built from a
   * fresh {@link #createTransactionResponses()} every call, so the header is never attached to the
   * shared instance that commit and rollback also use.
   */
  private ApiResponses createBeginResponses() {
    final ApiResponses responses = createTransactionResponses();
    responses.get("200").addHeaderObject(SESSION_HEADER, SpecBuilders.stringHeader("""
        Session id identifying the transaction just opened. Present it on the '%s' request header \
        of every subsequent call that belongs to this transaction.""".formatted(SESSION_HEADER)));
    return responses;
  }
```

In `createBeginPath()`, change the responses line to:

```java
    postOp.setResponses(createBeginResponses());
```

In `createCommitPath()` and `createRollbackPath()`, add after the `pathParam` line (using the `required` value you determined in Step 1):

```java
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, true));
```

In `createPostQueryPath()`, `createCommandPath()` and `createGetQueryPath()`, add after the `pathParam` line:

```java
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));
```

In `createGetQueryPath()` the operation variable is a GET, so use whatever local name that method already uses rather than `postOp`. The GET query path is included because it extends the same base handler and therefore honours the same session header; the spec document lists five operations, and this is a sixth added for consistency.

- [ ] **Step 9: Run to verify the tests pass**

```bash
./mvnw -o -pl server -am test -Dsurefire.includes='**/openapi/*Test.java'
```

Expected: the whole openapi contributor package green. Run the wider package, not just `CoreApiSpecTest`: `ApiSpecPathConstantsTest` and the IT assert operation inventories, and adding parameters must not disturb them.

- [ ] **Step 10: Run the integration test**

```bash
lsof -nP -iTCP:2480 -sTCP:LISTEN
./mvnw -o -pl server -am verify -DskipITs=false -Dit.test=OpenApiSpecGenerationIT -DfailIfNoTests=false
```

Expected: PASS. This confirms the assembled spec still parses cleanly and has no unresolved refs.

- [ ] **Step 11: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/SpecBuilders.java \
        server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/SpecBuildersTest.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java
git commit -m "fix(#$ISSUE_SESSION): document the arcadedb-session-id header on the transaction operations"
```

---

## Task 4: Model PromQL result shapes as a `oneOf`

**Context:** `data.result` is declared as an array of bare `type: object`, with the vector, matrix and scalar shapes described only in prose. A generated client gets untyped entries, which defeats the PromQL half of the M1 facade.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpec.java` (the `createDataResponseSchema()` method)
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpecTest.java`

**Interfaces:**
- Consumes: `$ISSUE_PROMQL` from Task 1.
- Produces: the `PromQLDataResponse` component's `data.result` property carries a three-branch `oneOf`.

- [ ] **Step 1: Verify the three shapes against the handler**

The existing prose describes vector entries as carrying `metric` and a single `value` pair, matrix entries as carrying `metric` and a list of `values` pairs, and a scalar as replacing the array with a single pair. Confirm this against the code before encoding it as structure:

```bash
grep -rn "resultType\|vector\|matrix\|scalar" server/src/main/java/com/arcadedb/server/http/handler/GetPromQLQueryHandler.java | head -30
```

If the handler disagrees with the prose, follow the handler and note the discrepancy in the commit message.

- [ ] **Step 2: Write the failing test**

Add to `PrometheusApiSpecTest`:

```java
  @Test
  void promQlResultDeclaresItsThreeShapes() {
    final Schema<?> response = openAPI.getComponents().getSchemas().get("PromQLDataResponse");
    final Schema<?> data = (Schema<?>) response.getProperties().get("data");
    final Schema<?> result = (Schema<?>) data.getProperties().get("result");

    assertThat(result.getOneOf())
        .as("vector, matrix and scalar are structurally different; a client must narrow on resultType")
        .hasSize(3);
  }

  @Test
  void promQlVectorEntriesCarryMetricAndValue() {
    final Schema<?> response = openAPI.getComponents().getSchemas().get("PromQLDataResponse");
    final Schema<?> data = (Schema<?>) response.getProperties().get("data");
    final Schema<?> result = (Schema<?>) data.getProperties().get("result");
    final Schema<?> vectorEntry = result.getOneOf().get(0).getItems();

    assertThat(vectorEntry.getProperties().keySet())
        .as("an instant sample is a labelled metric plus one [timestamp, value] pair")
        .containsExactlyInAnyOrder("metric", "value");
  }
```

Confirm the setup block in this test class matches `CoreApiSpecTest`'s (`openAPI.setPaths(new Paths()); openAPI.setComponents(new Components()); new PrometheusApiSpec().contribute(openAPI);`) and that the component really is registered as `PromQLDataResponse`; adjust the key if the contributor uses a different name.

- [ ] **Step 3: Run to verify it fails**

```bash
./mvnw -o -pl server -am test -Dsurefire.includes='**/openapi/PrometheusApiSpecTest.java'
```

Expected: FAIL, `getOneOf()` is null because `result` is currently a plain array.

- [ ] **Step 4: Implement the schema**

Replace `createDataResponseSchema()` in `PrometheusApiSpec.java` with:

```java
  private Schema<?> createDataResponseSchema() {
    final Schema<String> resultType = SpecBuilders.string(
        "Shape of 'result': a vector of instant samples, a matrix of range samples, or a scalar");
    resultType.setEnum(List.of("vector", "matrix", "scalar"));

    final Schema<Object> vectorEntry = SpecBuilders.object("One instant sample");
    vectorEntry.addProperty("metric", SpecBuilders.object("Label map, including the '__name__' label"));
    vectorEntry.addProperty("value", samplePair());

    final Schema<Object> matrixEntry = SpecBuilders.object("One range series");
    matrixEntry.addProperty("metric", SpecBuilders.object("Label map, including the '__name__' label"));
    matrixEntry.addProperty("values", SpecBuilders.arrayOf(samplePair(), "Samples ordered by timestamp"));

    final Schema<Object> result = new Schema<>();
    result.setDescription("""
        Evaluation result, shaped by 'resultType': an array of instant samples when 'vector', an \
        array of range series when 'matrix', and a single [timestamp, value] pair when 'scalar'.""");
    result.setOneOf(List.of(
        SpecBuilders.arrayOf(vectorEntry, "Entries when resultType is 'vector'"),
        SpecBuilders.arrayOf(matrixEntry, "Entries when resultType is 'matrix'"),
        samplePair()));

    final Schema<Object> data = SpecBuilders.object("Evaluation result");
    data.addProperty("resultType", resultType);
    data.addProperty("result", result);

    final Schema<Object> schema = SpecBuilders.object("Prometheus query response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", data);
    return schema;
  }

  /**
   * A fresh [timestamp, value] pair schema on every call. Returning a shared instance would place
   * one mutable schema object at several points in the document, the same defect class that put one
   * ApiResponse under two status keys during #4895.
   */
  private Schema<?> samplePair() {
    return SpecBuilders.arrayOf(
        SpecBuilders.object("Unix timestamp in seconds, then the sample value as a string"),
        "One [timestamp, value] pair");
  }
```

- [ ] **Step 5: Run to verify the tests pass**

```bash
./mvnw -o -pl server -am test -Dsurefire.includes='**/openapi/*Test.java'
```

Expected: the whole openapi contributor package green.

- [ ] **Step 6: Run the integration test to confirm refs still resolve**

```bash
lsof -nP -iTCP:2480 -sTCP:LISTEN
./mvnw -o -pl server -am verify -DskipITs=false -Dit.test=OpenApiSpecGenerationIT -DfailIfNoTests=false
```

Expected: PASS, including the existing zero-unresolved-refs assertion. A malformed `oneOf` surfaces here.

- [ ] **Step 7: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpecTest.java
git commit -m "feat(#$ISSUE_PROMQL): model the PromQL vector, matrix and scalar result shapes"
```

---

## Task 5: Publish the contract as release assets

**Context:** The OpenAPI spec exists only behind a running server and the proto only in the source tree. The `arcadedb-clients` repository needs a versioned, downloadable contract. This reuses the `release: [published]` plus `gh release upload` pattern already proven by `native-image.yml`.

**Files:**
- Create: `.github/workflows/publish-contract.yml`

**Interfaces:**
- Consumes: `$ISSUE_CONTRACT` from Task 1, and Task 2's guarantee that `info.version` equals the server version.
- Produces: four assets on every published release: `arcadedb-openapi-<tag>.json`, `arcadedb-server-<tag>.proto`, and a `.sha256` for each.

- [ ] **Step 1: Create the workflow**

No license header on workflow files.

```yaml
name: publish-contract

on:
  release:
    types: [published]
  workflow_dispatch:
    inputs:
      tag:
        description: "Release tag to publish contract artifacts for (e.g. 26.9.1)"
        required: true

permissions:
  contents: read

jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    env:
      TAG: ${{ github.event.release.tag_name || inputs.tag }}
      ROOT_PASSWORD: contractfetch
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          ref: ${{ github.event.release.tag_name || inputs.tag }}

      - name: Start the released server
        run: |
          docker run -d --name arcadedb-contract -p 2480:2480 \
            -e JAVA_OPTS="-Darcadedb.server.rootPassword=$ROOT_PASSWORD" \
            "arcadedata/arcadedb:$TAG"

      - name: Wait for readiness
        run: |
          for _ in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:2480/api/v1/ready || true)
            if [ "$code" = "204" ]; then
              echo "server ready"
              exit 0
            fi
            sleep 2
          done
          echo "server did not become ready within 120s"
          docker logs arcadedb-contract
          exit 1

      - name: Fetch the OpenAPI contract
        run: |
          curl -sS --fail -u "root:$ROOT_PASSWORD" \
            http://localhost:2480/api/v1/openapi.json | jq -S . > "arcadedb-openapi-$TAG.json"

      - name: Verify the contract identifies this release
        run: |
          declared=$(jq -r '.info.version' "arcadedb-openapi-$TAG.json")
          if [ "$declared" != "$TAG" ]; then
            echo "spec declares info.version=$declared but this release is $TAG"
            exit 1
          fi

      - name: Copy the proto
        run: cp grpc/src/main/proto/arcadedb-server.proto "arcadedb-server-$TAG.proto"

      - name: Checksum both artifacts
        run: |
          sha256sum "arcadedb-openapi-$TAG.json" > "arcadedb-openapi-$TAG.json.sha256"
          sha256sum "arcadedb-server-$TAG.proto" > "arcadedb-server-$TAG.proto.sha256"

      - name: Attach to the release
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          gh release upload "$TAG" \
            "arcadedb-openapi-$TAG.json" "arcadedb-openapi-$TAG.json.sha256" \
            "arcadedb-server-$TAG.proto" "arcadedb-server-$TAG.proto.sha256" \
            --clobber

      - name: Stop the server
        if: always()
        run: docker rm -f arcadedb-contract || true
```

`jq -S` sorts keys so the published contract is byte-stable across runs, which is what makes the clients repo's drift diff meaningful rather than noisy.

- [ ] **Step 2: Validate the workflow parses**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/publish-contract.yml')); print('valid')"
```

Expected: `valid`.

- [ ] **Step 3: Rehearse the fetch locally against a dev image**

The workflow cannot be exercised end to end before a release exists that contains Task 2's fix. Rehearse everything except `gh release upload`:

```bash
cd package && ../mvnw install -Pdocker -DskipTests && cd ..
docker run -d --name arcadedb-contract-local -p 2480:2480 \
  -e JAVA_OPTS="-Darcadedb.server.rootPassword=contractfetch" \
  arcadedata/arcadedb:latest
sleep 30
curl -sS --fail -u root:contractfetch http://localhost:2480/api/v1/openapi.json | jq -S . > /tmp/contract.json
jq -r '.info.version' /tmp/contract.json
jq '.paths | keys | length' /tmp/contract.json
docker rm -f arcadedb-contract-local
```

Expected: `info.version` prints the working tree's version rather than `1.0.0`, and the path count is 61. Note the image tag used here is `latest` built from your tree, not a released tag.

- [ ] **Step 4: Confirm the version gate actually gates**

Run the workflow via `workflow_dispatch` against the released tag `26.8.1`. It **must fail** at "Verify the contract identifies this release", because 26.8.1 predates Task 2 and still declares `1.0.0`. That failure is the proof the gate works; do not weaken it. The first successful run happens on the first release cut after this branch merges.

```bash
gh workflow run publish-contract.yml --repo ArcadeData/arcadedb -f tag=26.8.1
gh run list --repo ArcadeData/arcadedb --workflow=publish-contract.yml --limit 1
```

Expected: a failed run whose failing step is the version verification, with `spec declares info.version=1.0.0 but this release is 26.8.1`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/publish-contract.yml
git commit -m "feat(#$ISSUE_CONTRACT): publish the OpenAPI spec and proto as release assets"
```

---

## Done criteria for M0

- [ ] Epic #4894 describes the M0/M1/M2/M3+ structure; #4897 through #4903 are closed as superseded.
- [ ] `/api/v1/openapi.json` reports the running server's version.
- [ ] `beginTransaction` declares the `arcadedb-session-id` response header; `commitTransaction`, `rollbackTransaction`, `executeQuery`, `executeQueryPost` and `executeCommand` declare it as a request header.
- [ ] `PromQLDataResponse`'s `data.result` carries a three-branch `oneOf`.
- [ ] `publish-contract.yml` exists, parses, and its version gate has been shown to fail on a pre-fix tag.
- [ ] `./mvnw -o -pl server -am verify -DskipITs=false -Dit.test=OpenApiSpecGenerationIT` is green.

## Deliberately not in this plan

- **M0-4, the `ai/chat` SSE mismatch.** `mode=auto` returns `text/event-stream` while the spec documents JSON, and OpenAPI 3.0 cannot express a response content type selected by a request-body value. Fixing it needs a server behaviour change (split the operation, or move to Accept-header negotiation), which is a design decision of its own. It gates nothing in M1 and belongs in its own issue.
- **M1 and M2.** M1 is planned separately once M0 has landed and a real contract artifact exists, so that plan can be concrete about the generated types instead of guessing at them.
