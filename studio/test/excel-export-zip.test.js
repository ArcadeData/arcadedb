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

// The ZIP writer behind the result table's "Excel" button (js/studio-zip.js), which replaced JSZip: JSZip pulled five
// unmaintained packages (setimmediate, lie, immediate, readable-stream's helpers) into the Studio jar. The archive is
// read back here with an independent reader built on node:zlib, so a wrong offset, size or CRC fails the test rather
// than producing a file only the writer itself understands.
//
//     node --test studio/test/excel-export-zip.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const zlib = require("node:zlib");

const SOURCE = fs.readFileSync(path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-zip.js"), "utf8");

function load(globals) {
  const ctx = vm.createContext(Object.assign({ TextEncoder, Blob, Response, Uint8Array, DataView }, globals));
  vm.runInContext(SOURCE, ctx);
  return ctx;
}

const withCompression = () => load({ CompressionStream });

// Independent reader: walks the central directory from the end-of-central-directory record, then checks each local
// header against it and inflates or copies the data
function readZip(bytes) {
  const buf = Buffer.from(bytes);
  const eocd = buf.length - 22;
  assert.equal(buf.readUInt32LE(eocd), 0x06054b50, "end of central directory record");
  const count = buf.readUInt16LE(eocd + 10);
  const cdSize = buf.readUInt32LE(eocd + 12);
  let p = buf.readUInt32LE(eocd + 16);
  assert.equal(p + cdSize, eocd, "central directory ends where the EOCD record starts");

  const entries = [];
  for (let i = 0; i < count; i++) {
    assert.equal(buf.readUInt32LE(p), 0x02014b50, "central directory header");
    const flags = buf.readUInt16LE(p + 8);
    const method = buf.readUInt16LE(p + 10);
    const crc = buf.readUInt32LE(p + 16);
    const csize = buf.readUInt32LE(p + 20);
    const usize = buf.readUInt32LE(p + 24);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const commentLen = buf.readUInt16LE(p + 32);
    const local = buf.readUInt32LE(p + 42);
    const name = buf.toString("utf8", p + 46, p + 46 + nameLen);
    p += 46 + nameLen + extraLen + commentLen;

    assert.equal(buf.readUInt32LE(local), 0x04034b50, "local header of " + name);
    assert.equal(buf.readUInt16LE(local + 8), method, "local and central method agree for " + name);
    assert.equal(buf.readUInt32LE(local + 14), crc, "local and central CRC agree for " + name);
    assert.equal(buf.readUInt32LE(local + 18), csize, "local and central sizes agree for " + name);
    const localNameLen = buf.readUInt16LE(local + 26);
    const dataStart = local + 30 + localNameLen + buf.readUInt16LE(local + 28);
    assert.equal(buf.toString("utf8", local + 30, local + 30 + localNameLen), name);

    const raw = buf.subarray(dataStart, dataStart + csize);
    const data = method === 8 ? zlib.inflateRawSync(raw) : Buffer.from(raw);
    assert.equal(data.length, usize, "uncompressed size of " + name);
    assert.equal(zlib.crc32(data), crc, "CRC-32 of " + name);
    entries.push({ name, flags, method, text: data.toString("utf8") });
  }
  return entries;
}

async function bytesOf(blob) {
  return new Uint8Array(await blob.arrayBuffer());
}

test("CRC-32 matches the standard check value", () => {
  const ctx = load({});
  const crc = vm.runInContext("studioZipCrc32", ctx);
  assert.equal(crc(new TextEncoder().encode("123456789")), 0xcbf43926);
  assert.equal(crc(new Uint8Array(0)), 0);
});

test("the files DataTables adds through folder() and file() come back with their paths and contents", async () => {
  const ctx = withCompression();
  const zip = vm.runInContext("new StudioZip()", ctx);
  const sheet = "<worksheet>" + "<row><c t='inlineStr'><is><t>ArcadeDB</t></is></c></row>".repeat(200) + "</worksheet>";
  zip.file("[Content_Types].xml", "<Types/>");
  zip.folder("_rels").file(".rels", "<Relationships/>");
  zip.folder("xl").folder("worksheets").file("sheet1.xml", sheet);

  const blob = await zip.generateAsync({
    compression: "DEFLATE",
    type: "blob",
    mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  assert.equal(blob.type, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

  const entries = readZip(await bytesOf(blob));
  assert.deepEqual(
    entries.map((e) => e.name),
    ["[Content_Types].xml", "_rels/.rels", "xl/worksheets/sheet1.xml"],
  );
  assert.equal(entries[0].text, "<Types/>");
  assert.equal(entries[1].text, "<Relationships/>");
  assert.equal(entries[2].text, sheet);
  // the repetitive sheet is deflated, and every entry declares UTF-8 names
  assert.equal(entries[2].method, 8);
  for (const e of entries) assert.equal(e.flags & 0x0800, 0x0800);
});

test("non-ASCII cell text and file names survive as UTF-8", async () => {
  const ctx = withCompression();
  const zip = vm.runInContext("new StudioZip()", ctx);
  zip.folder("dàti").file("città.xml", "<t>Perché ✓ 数据 🚀</t>");

  const entries = readZip(await bytesOf(await zip.generateAsync({ compression: "DEFLATE", type: "blob" })));
  assert.equal(entries.length, 1);
  assert.equal(entries[0].name, "dàti/città.xml");
  assert.equal(entries[0].text, "<t>Perché ✓ 数据 🚀</t>");
});

test("without CompressionStream the files are stored uncompressed and still readable", async () => {
  const ctx = load({});
  const zip = vm.runInContext("new StudioZip()", ctx);
  const sheet = "<row/>".repeat(500);
  zip.folder("xl").file("sheet.xml", sheet);

  const entries = readZip(await bytesOf(await zip.generateAsync({ compression: "DEFLATE", type: "blob" })));
  assert.equal(entries[0].method, 0);
  assert.equal(entries[0].text, sheet);
});

test("an entry that would not shrink is stored rather than deflated", async () => {
  const ctx = withCompression();
  const zip = vm.runInContext("new StudioZip()", ctx);
  zip.file("a.txt", "x");

  const entries = readZip(await bytesOf(await zip.generateAsync({ compression: "DEFLATE", type: "blob" })));
  assert.equal(entries[0].method, 0);
  assert.equal(entries[0].text, "x");
});

test("an empty archive is a valid ZIP", async () => {
  const ctx = withCompression();
  const zip = vm.runInContext("new StudioZip()", ctx);
  assert.deepEqual(readZip(await bytesOf(await zip.generateAsync({ type: "blob" }))), []);
});

test("the writer registers itself as the ZIP library of the DataTables Buttons extension", () => {
  let registered = null;
  const ctx = load({ DataTable: { Buttons: { jszip: (lib) => (registered = lib) } } });
  assert.equal(registered, vm.runInContext("StudioZip", ctx));
});

test("loading without DataTables does not throw", () => {
  assert.doesNotThrow(() => load({}));
});

test("index.html loads the writer after the Buttons extension and no longer ships JSZip", () => {
  const html = fs.readFileSync(path.join(__dirname, "..", "src", "main", "resources", "static", "index.html"), "utf8");
  assert.ok(!/src="[^"]*jszip/i.test(html), "index.html still loads JSZip");
  const buttons = html.indexOf('src="dist/js/dataTables.buttons.min.js"');
  const writer = html.indexOf('src="js/studio-zip.js"');
  assert.ok(buttons > 0 && writer > buttons, "studio-zip.js must load after dataTables.buttons.min.js");

  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8"));
  assert.equal(pkg.dependencies.jszip, undefined, "package.json still depends on jszip");
});
