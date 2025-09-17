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
package com.arcadedb.engine;

import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.vector.JVectorIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author carlos-rodrigues@8x8.com
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class FileManagerTest {

    public static final Set<String> FILE_EXT = Set.of(Dictionary.DICT_EXT,
            LocalBucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
            LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, JVectorIndex.FILE_EXT);

    @Test
    void construtor_failure_noPermissionsDirectory(@TempDir Path dir) throws IOException {
        // arrange

        Set<PosixFilePermission> noPerms = EnumSet.noneOf(PosixFilePermission.class);
        Files.setPosixFilePermissions(dir, noPerms);

        // act and assert
        assertThrows(IllegalArgumentException.class, () -> {
            new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);
        });

        // reset permissions to allow cleanup
        Set<PosixFilePermission> restorePerms = PosixFilePermissions.fromString("rwx------");
        Files.setPosixFilePermissions(dir, restorePerms);
    }

    @Test
    void construtor_failure_parentDirectoryWithNoPermissions(@TempDir Path dir) throws IOException {
        // arrange
        Set<PosixFilePermission> noPerms = EnumSet.noneOf(PosixFilePermission.class);
        Files.setPosixFilePermissions(dir, noPerms);

        // act and assert
        assertThrows(IllegalArgumentException.class, () -> {
            new FileManager(dir.toFile().getAbsolutePath() + "/child", ComponentFile.MODE.READ_WRITE, FILE_EXT);
        });

        // cleanup
        Set<PosixFilePermission> restorePerms = PosixFilePermissions.fromString("rwx------");
        Files.setPosixFilePermissions(dir, restorePerms);
    }

    @Test
    void construtor_success_emptyDirectory(@TempDir Path dir) throws IOException {
        // arrange
        // act
        FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);

        // assert
        assertTrue(fileManager.getFiles().isEmpty());
    }

    @Test
    void construtor_success_noDirectory() throws IOException {
        // arrange
        Path dir = Path.of(System.getProperty("java.io.tmpdir"), "nonExistentDir");

        // act
        FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);

        // assert
        assertTrue(fileManager.getFiles().isEmpty());
        // cleanup
        Files.deleteIfExists(dir);
    }
}
