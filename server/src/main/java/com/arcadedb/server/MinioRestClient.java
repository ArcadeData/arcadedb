package com.arcadedb.server;

import java.util.concurrent.TimeUnit;

import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.http.Method;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MinioRestClient extends DataFabricRestClient {

    private static final String minioUrl = System.getenv("INTERNAL_MINIO_URL");
    private static final String bucketName = System.getenv("BACKUP_BUCKET_NAME");
    private static final String rootArcadePath = System.getenv("BACKUP_PATH");

    private static MinioClient getMinioClient() {
        try {
            return MinioClient.builder()
                    .endpoint(minioUrl)
                    .credentials(System.getenv("INTERNAL_MINIO_USERNAME"), System.getenv("INTERNAL_MINIO_PASSWORD"))
                    .build();
        } catch (Exception e) {
            log.error("Error building minio client", e.getMessage());
            log.debug("Exception", e);
        }

        return null;
    }

    /**
     * Creates a presigned url to upload a given file name for a expirating period
     * of time.
     */
    public static String getPreSignedUrl(String backupName, int expiryHours) {

        try {
            String url = getMinioClient().getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.PUT)
                            .bucket(bucketName)
                            .object(backupName)
                            .expiry(expiryHours, TimeUnit.HOURS)
                            .build());
            return url;
        } catch (Exception e) {
            log.error("Error creating presigned url", e.getMessage());
            log.debug("Exception", e);
        }
        return null;
    }

    public static void uploadBackup(String databaseName, String backupName) {
        try {
            getMinioClient().uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(String.format("%s/%s/%s", rootArcadePath, databaseName, backupName))
                            .filename(String.format("backups/%s/%s", databaseName, backupName))
                            .build());
        } catch (Exception e) {
            log.error("Error uploading backup ", e.getMessage());
            log.debug("Exception", e);
        }
    }
}
