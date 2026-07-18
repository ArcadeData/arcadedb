#!/usr/bin/env python3
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
#
"""Sends a single MongoDB wire-protocol OP_MSG {hello: 1} command and checks a reply arrives.

Used by exercise.sh to exercise the Mongo wire-protocol plugin without depending on pymongo or
any BSON library: this hand-rolls the minimal BSON document and OP_MSG framing needed for a
`hello` handshake using only the Python standard library (socket + struct).
"""
import socket
import struct
import sys


def bson_cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def bson_int32_element(name: str, value: int) -> bytes:
    return b"\x10" + bson_cstring(name) + struct.pack("<i", value)


def bson_string_element(name: str, value: str) -> bytes:
    encoded = value.encode("utf-8") + b"\x00"
    return b"\x02" + bson_cstring(name) + struct.pack("<i", len(encoded)) + encoded


def bson_document(elements: bytes) -> bytes:
    total_length = 4 + len(elements) + 1
    return struct.pack("<i", total_length) + elements + b"\x00"


def build_hello_op_msg() -> bytes:
    doc = bson_document(bson_int32_element("hello", 1) + bson_string_element("$db", "admin"))
    flag_bits = struct.pack("<i", 0)
    section = b"\x00" + doc  # section kind 0 = a single BSON document
    body = flag_bits + section
    request_id = 1
    response_to = 0
    op_msg_code = 2013
    header = struct.pack("<iiii", 16 + len(body), request_id, response_to, op_msg_code)
    return header + body


def main() -> int:
    host, port = sys.argv[1], int(sys.argv[2])
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(build_hello_op_msg())
        sock.settimeout(5)
        reply = sock.recv(4096)
    return 0 if len(reply) >= 16 else 1


if __name__ == "__main__":
    sys.exit(main())
