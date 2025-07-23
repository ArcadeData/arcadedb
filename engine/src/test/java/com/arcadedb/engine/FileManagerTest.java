package com.arcadedb.engine;

import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.vector.HnswVectorIndex;
import org.junit.jupiter.api.Test;

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
public class  FileManagerTest {


    @Test
    void construtor_failure_noPermissionsDirectory() throws IOException {
        // arrange
        Path dir = Files.createTempDirectory("noPermsDir");
        Set<PosixFilePermission> noPerms = EnumSet.noneOf(PosixFilePermission.class);
        Files.setPosixFilePermissions(dir, noPerms);

        // act and assert
        assertThrows(IllegalArgumentException.class, () -> {
            new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, Set.of(Dictionary.DICT_EXT,
                    LocalBucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
                    LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, HnswVectorIndex.FILE_EXT));
        });

        // cleanup
        Set<PosixFilePermission> restorePerms = PosixFilePermissions.fromString("rwx------");
        Files.setPosixFilePermissions(dir, restorePerms);

        Files.deleteIfExists(dir);
    }

    @Test
    void construtor_failure_parentDirectoryWithNoPermissions() throws IOException {
        // arrange
        Path dir = Files.createTempDirectory("parentNoPermsDir");
        Set<PosixFilePermission> noPerms = EnumSet.noneOf(PosixFilePermission.class);
        Files.setPosixFilePermissions(dir, noPerms);

        // act and assert
        assertThrows(IllegalArgumentException.class, () -> {
            new FileManager(dir.toFile().getAbsolutePath() + "/child", ComponentFile.MODE.READ_WRITE, Set.of(Dictionary.DICT_EXT,
                    LocalBucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
                    LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, HnswVectorIndex.FILE_EXT));
        });

        // cleanup
        Set<PosixFilePermission> restorePerms = PosixFilePermissions.fromString("rwx------");
        Files.setPosixFilePermissions(dir, restorePerms);

        Files.deleteIfExists(dir);
    }

    @Test
    void construtor_success_emptyDirectory() throws IOException {
        // arrange
        Path dir = Files.createTempDirectory("emptyDir");

        // act
        FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, Set.of(Dictionary.DICT_EXT,
                LocalBucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT));

        // assert
        assertTrue(fileManager.getFiles().isEmpty());
        // cleanup
        Files.deleteIfExists(dir);
    }

    @Test
    void construtor_success_noDirectory() throws IOException {
        // arrange
        Path dir = Path.of("/tmp","nonExistentDir");

        // act
        FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, Set.of(Dictionary.DICT_EXT,
                LocalBucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT));

        // assert
        assertTrue(fileManager.getFiles().isEmpty());
        // cleanup
        Files.deleteIfExists(dir);
    }
}
