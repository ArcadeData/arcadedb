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

// Bolt protocol conformance suite for issue #4888 (epic #4882, Group B).
// Implements every scenario in bolt/conformance/spec.yaml (#4883) against the
// official neo4j-driver. Each test name embeds its spec id ([AREA-NNN]) for
// traceability, per bolt/conformance/README.md.
//
// Since #5001 ArcadeDB advertises Bolt 5.0-5.4 (and still 4.4/4.0/3.0), so
// neo4j-driver negotiates 5.x (see PROTO-002). A future driver major dropping
// legacy negotiation would not affect this suite as long as 5.x stays offered.
const fs = require("fs");
const path = require("path");
const os = require("os");
const { execFileSync } = require("child_process");
const neo4j = require("neo4j-driver");
const { GenericContainer, Wait } = require("testcontainers");

const ROOT_PASSWORD = "playwithdata";
const IMAGE = process.env.ARCADEDB_DOCKER_IMAGE || "arcadedata/arcadedb:latest";
const BASE_JAVA_OPTS =
  `-Darcadedb.server.rootPassword=${ROOT_PASSWORD}` +
  " -Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}" +
  " -Darcadedb.server.plugins=BoltProtocolPlugin";
const TLS_STORE_PASSWORD = "changeit";
// neo4j:// routing (CONN-003) makes the driver connect to the address
// handleRoute advertises - the server's own bound container IP. That IP is
// host-routable on the native Docker bridge used by Linux CI, but not from
// the host on Docker Desktop (macOS/Windows), where it is unreachable and the
// driver times out. Run the scenario only where the routed address is
// reachable; bolt:// scenarios use the mapped host port and are unaffected.
const ROUTING_HOST_REACHABLE = process.platform === "linux";

const TYPE_MATRIX_FIXTURE = path.resolve(
  __dirname,
  "../../bolt/conformance/fixtures/type-matrix.cypher"
);

// Shared concurrency helper for TX-005 and ERR-004. Two sessions race to
// update the same node inside explicit transactions, one held open past the
// other's commit. Returns the list of errors raised. Timing-sensitive by
// nature: callers assert on the whole collected set, not a single index.
async function raceTwoWriters(driver, database, marker) {
  const setup = driver.session({ database });
  try {
    await setup.run("MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", {
      marker,
    });
  } finally {
    await setup.close();
  }

  const errors = [];
  async function racingWrite() {
    const s = driver.session({ database });
    const tx = s.beginTransaction();
    try {
      await tx.run(
        "MATCH (n:RaceProbe {marker: $marker}) SET n.value = n.value + 1 RETURN n",
        { marker }
      );
      await new Promise((r) => setTimeout(r, 500));
      await tx.commit();
    } catch (e) {
      errors.push(e);
      try {
        await tx.rollback();
      } catch (_) {
        /* transaction already broken */
      }
    } finally {
      await s.close();
    }
  }

  await Promise.all([racingWrite(), racingWrite()]);
  // Diagnostic (printed unconditionally so it shows even on an unexpected
  // pass): an isolated flip to a genuine TransientError is a real, actionable
  // server-behavior change worth concrete evidence for.
  errors.forEach((e, i) =>
    console.log(
      `raceTwoWriters[${marker}] errors[${i}]: code=${e.code} msg=${e.message}`
    )
  );
  return errors;
}

// Generate a throwaway self-signed keystore/truststore with the JDK keytool.
// ArcadeDB ships no default TLS store, so REQUIRED/OPTIONAL modes need one or
// startup crashes. Returns the temp dir holding keystore.p12 + truststore.jks,
// or null if keytool is unavailable (local runs without a JDK skip TLS).
function generateTlsCerts() {
  let keytool;
  try {
    keytool = execFileSync("which", ["keytool"]).toString().trim();
  } catch (_) {
    return null;
  }
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bolt-tls-"));
  const keystore = path.join(dir, "keystore.p12");
  const truststore = path.join(dir, "truststore.jks");
  const cert = path.join(dir, "bolt.cer");
  try {
    execFileSync(keytool, [
      "-genkeypair", "-alias", "bolt", "-keyalg", "RSA", "-keysize", "2048",
      "-validity", "3650", "-keystore", keystore, "-storetype", "PKCS12",
      "-storepass", TLS_STORE_PASSWORD, "-keypass", TLS_STORE_PASSWORD,
      "-dname", "CN=localhost, OU=ArcadeDB, O=ArcadeDB, L=Test, ST=Test, C=US",
    ]);
    execFileSync(keytool, [
      "-exportcert", "-alias", "bolt", "-keystore", keystore,
      "-storetype", "PKCS12", "-storepass", TLS_STORE_PASSWORD, "-file", cert,
    ]);
    execFileSync(keytool, [
      "-importcert", "-alias", "bolt", "-keystore", truststore,
      "-storetype", "JKS", "-storepass", TLS_STORE_PASSWORD, "-file", cert,
      "-noprompt",
    ]);
    return dir;
  } catch (_) {
    // keytool present but failed (permissions, policy, JDK quirk): clean up and
    // skip the TLS scenarios rather than crashing the whole suite.
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch (__) {
      /* ignore cleanup failure */
    }
    return null;
  }
}

const TLS_IMAGE_TAG = "arcadedb-bolt-tls-test:latest";

// Build a derived image with the certs COPYied in (bind-mounts are unreliable
// on some CI runners - the Python fixture documents empty mounted dirs). Uses
// testcontainers' Dockerfile build so the COPY resolves its own context.
// Returns the image tag; callers instantiate a fresh GenericContainer per test
// rather than reusing (and mutating) one shared builder instance.
async function buildTlsImage(certDir) {
  fs.writeFileSync(
    path.join(certDir, "Dockerfile"),
    `FROM ${IMAGE}\n` +
      "COPY --chown=arcadedb:arcadedb keystore.p12 truststore.jks /home/arcadedb/tls_certs/\n"
  );
  await GenericContainer.fromDockerfile(certDir).build(TLS_IMAGE_TAG);
  return TLS_IMAGE_TAG;
}

function tlsJavaOpts(mode) {
  return (
    BASE_JAVA_OPTS +
    ` -Darcadedb.bolt.ssl=${mode}` +
    " -Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12" +
    ` -Darcadedb.ssl.keyStorePassword=${TLS_STORE_PASSWORD}` +
    " -Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks" +
    ` -Darcadedb.ssl.trustStorePassword=${TLS_STORE_PASSWORD}`
  );
}

// keytool availability decided at load time so the TLS scenarios report as a
// real skip (not a misleading pass) on hosts without a JDK. CI runners have
// one; local runs without keytool skip CONN-002/CONN-005 honestly.
const KEYTOOL_AVAILABLE = (() => {
  try {
    execFileSync("which", ["keytool"]);
    return true;
  } catch (_) {
    return false;
  }
})();
const tlsIt = KEYTOOL_AVAILABLE ? it : it.skip;

// True if the collected racing errors contain no non-retryable code: a
// write-write conflict must surface as Neo.TransientError.* (retryable), never
// as a ClientError/DatabaseError (mirrors the Python suite's negative check).
function noNonRetryableErrors(errors) {
  return errors.every(
    (e) => !/^Neo\.(ClientError|DatabaseError)\./.test(String(e.code))
  );
}

describe("Bolt conformance (issue #4888)", () => {
  jest.setTimeout(180000);

  let container;
  let driver;
  let host;
  let httpPort;
  let boltPort;

  function boltUri(scheme = "bolt") {
    return `${scheme}://${host}:${boltPort}`;
  }

  async function httpCommand(apiPath, body) {
    const res = await fetch(`http://${host}:${httpPort}/api/v1/${apiPath}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization:
          "Basic " + Buffer.from(`root:${ROOT_PASSWORD}`).toString("base64"),
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    return res.json();
  }

  function session(database = "beer", extra = {}) {
    return driver.session({ database, ...extra });
  }

  beforeAll(async () => {
    if (!fs.existsSync(TYPE_MATRIX_FIXTURE))
      throw new Error(`type-matrix fixture not found: ${TYPE_MATRIX_FIXTURE}`);

    container = await new GenericContainer(IMAGE)
      .withExposedPorts(2480, 7687)
      .withEnvironment({ JAVA_OPTS: BASE_JAVA_OPTS })
      .withStartupTimeout(120000)
      .withWaitStrategy(
        Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching(
          (sc) => sc === 204
        )
      )
      .start();

    host = container.getHost();
    httpPort = container.getMappedPort(2480);
    boltPort = container.getMappedPort(7687);

    // Seed over HTTP only, never over Bolt.
    await httpCommand("server", { command: "create database boltscratch" });
    await httpCommand("command/beer", {
      language: "cypher",
      command: fs.readFileSync(TYPE_MATRIX_FIXTURE, "utf8"),
    });

    driver = neo4j.driver(boltUri(), neo4j.auth.basic("root", ROOT_PASSWORD));
  });

  afterAll(async () => {
    if (driver) await driver.close();
    if (container) await container.stop();
  });

  // --- connection --------------------------------------------------------
  describe("connection", () => {
    it("[CONN-001] connect via bolt:// scheme", async () => {
      await driver.verifyConnectivity();
    });

    (ROUTING_HOST_REACHABLE ? it : it.skip)("[CONN-003] neo4j:// routing discovery, single-node", async () => {
      const d = neo4j.driver(
        boltUri("neo4j"),
        neo4j.auth.basic("root", ROOT_PASSWORD)
      );
      try {
        await d.verifyConnectivity();
        const s = d.session({ database: "beer" });
        try {
          const r = await s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
          expect(r.records[0].get("name")).not.toBeNull();
        } finally {
          await s.close();
        }
      } finally {
        await d.close();
      }
    });

    it.skip("[CONN-004] neo4j:// routing reflects HA cluster topology", () => {
      // Requires a 3-node HA cluster; the single-node harness cannot
      // meaningfully exercise this without new multi-node orchestration
      // infrastructure. See CONN-004 / #4890.
    });
  });

  // --- connection: TLS ---------------------------------------------------
  describe("connection-tls", () => {
    let tlsImageTag; // tag of the testcontainers-built derived image
    let certDir;
    let tlsRequired;
    let tlsOptional;

    beforeAll(async () => {
      if (!KEYTOOL_AVAILABLE) return; // CONN-002/005 are it.skip in this case
      certDir = generateTlsCerts();
      if (!certDir) return;
      tlsImageTag = await buildTlsImage(certDir);
    });

    afterAll(async () => {
      if (tlsRequired) await tlsRequired.stop();
      if (tlsOptional) await tlsOptional.stop();
      // Remove the throwaway derived image so it does not accumulate on
      // developer machines (parity with the Python suite's teardown).
      if (tlsImageTag) {
        try {
          execFileSync("docker", ["rmi", "-f", tlsImageTag]);
        } catch (_) {
          /* image may be in use or already gone; ignore */
        }
      }
      if (certDir) {
        try {
          fs.rmSync(certDir, { recursive: true, force: true });
        } catch (_) {
          /* ignore cleanup failure */
        }
      }
    });

    tlsIt("[CONN-002] connect via bolt+ssc:// with TLS required", async () => {
      if (!certDir) return console.warn("keytool cert generation failed - skipping CONN-002");
      tlsRequired = await new GenericContainer(tlsImageTag)
        .withExposedPorts(2480, 7687)
        .withEnvironment({ JAVA_OPTS: tlsJavaOpts("REQUIRED") })
        .withStartupTimeout(120000)
        .withWaitStrategy(
          Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching(
            (sc) => sc === 204
          )
        )
        .start();
      const uri = `bolt+ssc://${tlsRequired.getHost()}:${tlsRequired.getMappedPort(
        7687
      )}`;
      const d = neo4j.driver(uri, neo4j.auth.basic("root", ROOT_PASSWORD));
      try {
        await d.verifyConnectivity();
        const s = d.session({ database: "beer" });
        try {
          const r = await s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
          expect(r.records[0].get("name")).not.toBeNull();
        } finally {
          await s.close();
        }
      } finally {
        await d.close();
      }
    });

    tlsIt("[CONN-005] TLS OPTIONAL falls back to plaintext bolt://", async () => {
      if (!certDir) return console.warn("keytool cert generation failed - skipping CONN-005");
      tlsOptional = await new GenericContainer(tlsImageTag)
        .withExposedPorts(2480, 7687)
        .withEnvironment({ JAVA_OPTS: tlsJavaOpts("OPTIONAL") })
        .withStartupTimeout(120000)
        .withWaitStrategy(
          Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching(
            (sc) => sc === 204
          )
        )
        .start();
      const uri = `bolt://${tlsOptional.getHost()}:${tlsOptional.getMappedPort(
        7687
      )}`;
      const d = neo4j.driver(uri, neo4j.auth.basic("root", ROOT_PASSWORD));
      try {
        await d.verifyConnectivity();
      } finally {
        await d.close();
      }
    });
  });

  // --- auth --------------------------------------------------------------
  describe("auth", () => {
    it("[AUTH-001] basic auth succeeds with valid credentials", async () => {
      await driver.verifyConnectivity();
      const s = session();
      try {
        const r = await s.run("RETURN 1 AS value");
        expect(r.records[0].get("value").toNumber()).toBe(1);
      } finally {
        await s.close();
      }
    });

    it("[AUTH-002] basic auth fails with invalid credentials", async () => {
      const d = neo4j.driver(
        boltUri(),
        neo4j.auth.basic("root", "wrong-password")
      );
      try {
        await expect(d.verifyConnectivity()).rejects.toMatchObject({
          code: "Neo.ClientError.Security.Unauthorized",
        });
      } finally {
        await d.close();
      }
    });

    it("[AUTH-003] auth scheme 'none' is rejected (intentional)", async () => {
      // The JS driver has no AuthTokens.none(); omitting the auth token is the
      // 'none' equivalent. ArcadeDB deliberately rejects it (see AUTH-003).
      const d = neo4j.driver(boltUri());
      try {
        await expect(d.verifyConnectivity()).rejects.toThrow();
      } finally {
        await d.close();
      }
    });
  });

  // --- transactions ------------------------------------------------------
  describe("transactions", () => {
    it("[TX-001] autocommit query executes and returns results", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        expect(r.records).toHaveLength(5);
      } finally {
        await s.close();
      }
    });

    it("[TX-002] explicit BEGIN/RUN/COMMIT persists changes", async () => {
      const s = session();
      try {
        const tx = s.beginTransaction();
        await tx.run("CREATE (:TxCommitProbe {marker: 'tx-002'})");
        await tx.commit();
        const r = await s.run(
          "MATCH (n:TxCommitProbe {marker: 'tx-002'}) RETURN count(n) AS c"
        );
        expect(r.records[0].get("c").toNumber()).toBe(1);
      } finally {
        await s.close();
      }
    });

    it("[TX-003] explicit BEGIN/RUN/ROLLBACK discards changes", async () => {
      const s = session();
      try {
        const tx = s.beginTransaction();
        await tx.run("CREATE (:TxRollbackProbe {marker: 'tx-003'})");
        await tx.rollback();
        const r = await s.run(
          "MATCH (n:TxRollbackProbe {marker: 'tx-003'}) RETURN count(n) AS c"
        );
        expect(r.records[0].get("c").toNumber()).toBe(0);
      } finally {
        await s.close();
      }
    });

    it("[TX-004] managed executeWrite commits on success", async () => {
      const s = session();
      try {
        await s.executeWrite((tx) =>
          tx.run("CREATE (:Beer {name: $n})", { n: "TX-004-Beer" })
        );
        const r = await s.run(
          "MATCH (b:Beer {name: $n}) RETURN count(b) AS c",
          { n: "TX-004-Beer" }
        );
        expect(r.records[0].get("c").toNumber()).toBe(1);
      } finally {
        await s.close();
      }
    });

    it("[TX-005] managed transaction retries on Neo.TransientError.*", async () => {
      const errors = await raceTwoWriters(driver, "beer", "tx-005");
      expect(errors.length).toBeGreaterThan(0);
      expect(
        errors.some((e) => String(e.code).startsWith("Neo.TransientError"))
      ).toBe(true);
      // The conflict must be retryable: no racing error is a non-retryable
      // ClientError/DatabaseError (this is the part that proves retryability).
      expect(noNonRetryableErrors(errors)).toBe(true);
    });
  });

  // --- causal-consistency ------------------------------------------------
  describe("causal-consistency", () => {
    it("[CAUSAL-001] bookmark enforces read-after-write across sessions", async () => {
      const a = session();
      let bookmarks;
      try {
        await a.run("CREATE (:CausalProbe {marker: 'causal-001'})");
        bookmarks = a.lastBookmarks();
      } finally {
        await a.close();
      }
      const b = driver.session({ database: "beer", bookmarks });
      try {
        const r = await b.run(
          "MATCH (n:CausalProbe {marker: 'causal-001'}) RETURN count(n) AS c"
        );
        expect(r.records[0].get("c").toNumber()).toBe(1);
      } finally {
        await b.close();
      }
    });
  });

  // --- multi-database ----------------------------------------------------
  describe("multi-database", () => {
    it("[MDB-001] session selects a specific named database", async () => {
      const s = session("beer");
      try {
        const r = await s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
        expect(r.records[0].get("name")).not.toBeNull();
      } finally {
        await s.close();
      }
    });

    it("[MDB-002] sessions across databases are isolated", async () => {
      const scratch = session("boltscratch");
      const tx = scratch.beginTransaction();
      try {
        await tx.run("CREATE (:ScratchProbe {marker: 'mdb-002'})");
        const beer = session("beer");
        try {
          const r = await beer.run(
            "MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c"
          );
          expect(r.records[0].get("c").toNumber()).toBe(0);
        } finally {
          await beer.close();
        }
        await tx.commit();
      } finally {
        await scratch.close();
      }
      const verify = session("boltscratch");
      try {
        const r = await verify.run(
          "MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c"
        );
        expect(r.records[0].get("c").toNumber()).toBe(1);
      } finally {
        await verify.close();
      }
    });
  });

  // --- result-handling ---------------------------------------------------
  describe("result-handling", () => {
    it("[RESULT-001] streaming PULL returns records incrementally", async () => {
      const s = session();
      try {
        const result = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 10");
        let seen = 0;
        for await (const record of result) {
          expect(record.get("name")).not.toBeNull();
          seen += 1;
        }
        expect(seen).toBe(10);
      } finally {
        await s.close();
      }
    });

    it("[RESULT-002] PULL n streams exactly n, further PULL continues", async () => {
      const s = session("beer", { fetchSize: 2 });
      try {
        const result = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        const iter = result[Symbol.asyncIterator]();
        const first = [await iter.next(), await iter.next()];
        expect(first.every((x) => !x.done)).toBe(true);
        let rest = 0;
        while (true) {
          const n = await iter.next();
          if (n.done) break;
          rest += 1;
        }
        expect(rest).toBe(3);
      } finally {
        await s.close();
      }
    });

    it("[RESULT-003] DISCARD abandons remaining rows", async () => {
      const s = session("beer", { fetchSize: 1 });
      try {
        // Pull exactly 1 row of a 5-row result, then DISCARD the remainder.
        // `for await ... break` closes the async iterator cleanly (its return()
        // discards the pending rows); result.summary() then returns the
        // ResultSummary WITHOUT materializing the remaining records into a
        // user-facing array (unlike `await result`).
        const result = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        for await (const _record of result) break; // eslint-disable-line no-unused-vars
        const summary = await result.summary();
        expect(summary).toBeDefined();
        expect(summary.query).toBeDefined();
      } finally {
        await s.close();
      }
    });

    it("[RESULT-004] ResultSummary counters reflect writes", async () => {
      const s = session();
      try {
        const result = await s.run(
          "CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
          { n: "RESULT-004-Beer", b: "RESULT-004-Brewery" }
        );
        const c = result.summary.counters.updates();
        expect(c.nodesCreated).toBe(2);
        expect(c.relationshipsCreated).toBe(1);
        expect(c.propertiesSet).toBeGreaterThanOrEqual(2);
      } finally {
        await s.close();
      }
    });
  });

  // --- type-roundtrip ----------------------------------------------------
  describe("type-roundtrip", () => {
    it("[TYPE-001] node round-trips as a native Bolt structure", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (b:Beer) RETURN b LIMIT 1");
        const node = r.records[0].get("b");
        expect(node).toBeInstanceOf(neo4j.types.Node);
        expect(node.labels).toContain("Beer");
        expect(node.properties.name).toBeDefined();
      } finally {
        await s.close();
      }
    });

    it("[TYPE-002] relationship round-trips as a native Bolt structure", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH ()-[rel]->() RETURN rel LIMIT 1");
        const rel = r.records[0].get("rel");
        expect(rel).toBeInstanceOf(neo4j.types.Relationship);
        expect(rel.type).toBeDefined();
      } finally {
        await s.close();
      }
    });

    it("[TYPE-003] path round-trips as a native Bolt structure", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1");
        const p = r.records[0].get("p");
        expect(p).toBeInstanceOf(neo4j.types.Path);
        expect(p.segments.length).toBeGreaterThanOrEqual(1);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-004] ByteArray round-trips as a bound parameter", async () => {
      const s = session();
      try {
        const payload = Int8Array.from([1, 2, 3, 4]);
        const r = await s.run("RETURN $b AS echo", { b: payload });
        const echo = r.records[0].get("echo");
        expect(Array.from(echo)).toEqual([1, 2, 3, 4]);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-005] nested lists and maps round-trip structurally", async () => {
      const s = session();
      try {
        const r = await s.run(
          "MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m"
        );
        expect(r.records[0].get("l")).toEqual([
          neo4j.int(1),
          neo4j.int(2),
          [neo4j.int(3), neo4j.int(4)],
        ]);
        expect(r.records[0].get("m")).toEqual({
          a: neo4j.int(1),
          b: { c: neo4j.int(2) },
        });
      } finally {
        await s.close();
      }
    });

    it("[TYPE-006] null values round-trip", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (t:TypeMatrix) RETURN t.nullProp AS n");
        expect(r.records[0].get("n")).toBeNull();
        const e = await s.run("RETURN $p AS echo", { p: null });
        expect(e.records[0].get("echo")).toBeNull();
      } finally {
        await s.close();
      }
    });

    it("[TYPE-007] LocalDate round-trips as a native Bolt Date", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (t:TypeMatrix) RETURN t.localDateProp AS d");
        const d = r.records[0].get("d");
        expect(d).toBeInstanceOf(neo4j.types.Date);
        const e = await s.run("RETURN $d AS echo", { d });
        expect(e.records[0].get("echo")).toEqual(d);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-008] LocalTime round-trips as a native Bolt LocalTime", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2");
        const t2 = r.records[0].get("t2");
        expect(t2).toBeInstanceOf(neo4j.types.LocalTime);
        const e = await s.run("RETURN $t AS echo", { t: t2 });
        expect(e.records[0].get("echo")).toEqual(t2);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-009] LocalDateTime round-trips as a native Bolt LocalDateTime", async () => {
      const s = session();
      try {
        const r = await s.run(
          "MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt"
        );
        const dt = r.records[0].get("dt");
        expect(dt).toBeInstanceOf(neo4j.types.LocalDateTime);
        const e = await s.run("RETURN $dt AS echo", { dt });
        expect(e.records[0].get("echo")).toEqual(dt);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-010] offset DateTime round-trips as a native Bolt DateTime", async () => {
      const s = session();
      try {
        const r = await s.run(
          "MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt"
        );
        const dt = r.records[0].get("dt");
        expect(dt).toBeInstanceOf(neo4j.types.DateTime);
        // fixture offset is +02:00 -> 7200 seconds (driver returns a neo4j Integer)
        const offset = dt.timeZoneOffsetSeconds;
        expect(neo4j.isInt(offset) ? offset.toNumber() : offset).toBe(7200);
        const e = await s.run("RETURN $dt AS echo", { dt });
        expect(e.records[0].get("echo")).toEqual(dt);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-011] Duration round-trips as a native Bolt Duration", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (t:TypeMatrix) RETURN t.durationProp AS d");
        const d = r.records[0].get("d");
        expect(d).toBeInstanceOf(neo4j.types.Duration);
        const e = await s.run("RETURN $d AS echo", { d });
        expect(e.records[0].get("echo")).toEqual(d);
      } finally {
        await s.close();
      }
    });

    it("[TYPE-012] Point round-trips as a native Bolt Point", async () => {
      const s = session();
      try {
        const r = await s.run("MATCH (t:TypeMatrix) RETURN t.pointProp AS p");
        const p = r.records[0].get("p");
        expect(p).toBeInstanceOf(neo4j.types.Point);
        expect(p.x).toBeCloseTo(12.34);
        expect(p.y).toBeCloseTo(56.78);
        const e = await s.run("RETURN $p AS echo", { p });
        expect(e.records[0].get("echo").x).toBeCloseTo(12.34);
      } finally {
        await s.close();
      }
    });
  });

  // --- errors ------------------------------------------------------------
  describe("errors", () => {
    it("[ERR-001] syntax error returns Neo.ClientError.Statement.SyntaxError", async () => {
      const s = session();
      try {
        await expect(s.run("MATCH (n RETURN n")).rejects.toMatchObject({
          code: "Neo.ClientError.Statement.SyntaxError",
        });
      } finally {
        await s.close();
      }
    });

    it("[ERR-002] semantic error returns SemanticError code", async () => {
      const s = session();
      try {
        await expect(
          s.run("MATCH (n:Beer) RETURN undefinedVariable")
        ).rejects.toMatchObject({
          code: "Neo.ClientError.Statement.SemanticError",
        });
      } finally {
        await s.close();
      }
    });

    // NOT APPLICABLE: exercising a RUN before HELLO/LOGON needs a raw Bolt
    // socket; the managed neo4j-driver always authenticates first, so this
    // cannot be driven through the official driver. Skipped as in the
    // Python/Go/C# suites. See ERR-003 / #4890.
    it.skip("[ERR-003] unauthenticated request returns Forbidden (raw socket)", () => {});

    it("[ERR-004] transient conditions surface Neo.TransientError.*", async () => {
      const errors = await raceTwoWriters(driver, "beer", "err-004");
      expect(errors.length).toBeGreaterThan(0);
      expect(
        errors.some((e) => String(e.code).startsWith("Neo.TransientError"))
      ).toBe(true);
      // A transient conflict must not be misclassified as a non-retryable
      // ClientError/DatabaseError.
      expect(noNonRetryableErrors(errors)).toBe(true);
    });
  });

  // --- protocol ----------------------------------------------------------
  describe("protocol", () => {
    it("[PROTO-001] version negotiation succeeds (4.4/4.0/3.0)", async () => {
      // A successful query proves the handshake negotiated a supported
      // version; the pinned neo4j-driver offers a range including 4.4.
      const s = session();
      try {
        const r = await s.run("RETURN 1 AS v");
        expect(r.records[0].get("v").toNumber()).toBe(1);
      } finally {
        await s.close();
      }
    });

    // Since #5001 the server advertises Bolt 5.0-5.4, so a 5.x-capable driver
    // negotiates a 5.x protocol version instead of silently downgrading to 4.4.
    it("[PROTO-002] Bolt 5.x negotiation is supported", async () => {
      const info = await driver.getServerInfo({ database: "beer" });
      expect(Number(info.protocolVersion)).toBeGreaterThanOrEqual(5);
    });

    it("[PROTO-003] RESET returns the connection to a clean state", async () => {
      const s = session("beer", { fetchSize: 2 });
      try {
        // Start a partial stream, then break out early: the async iterator's
        // return() discards the remaining rows and returns the connection to a
        // clean state (RESET/DISCARD semantics).
        const first = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        for await (const _record of first) break; // eslint-disable-line no-unused-vars
        // The session accepts a fresh RUN immediately afterward.
        const r = await s.run("RETURN 1 AS v");
        expect(r.records[0].get("v").toNumber()).toBe(1);
      } finally {
        await s.close();
      }
    });
  });
});
