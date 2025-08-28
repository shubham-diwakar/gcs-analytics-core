package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
class GcsFileSystemImplIntegrationTest {

    @Test
    public void getFileInfo_noCredentialProvided_urlPointsToPublicObject_success() throws IOException {
        String gcsObject = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet";
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().build())
                .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("bigquery/us-states/us-states.parquet");
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo("cloud-samples-data");
    }

    @Test
    public void getFileInfo_noCredentialProvided_urlPointsToPrivateObject_usesApplicationDefaultCredentials()
            throws IOException {
        String object = "gs://gcs-connector-private-test-bucket-do-not-delete/tpch_customer_1.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(options);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(object));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("tpch_customer_1.parquet");
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName())
                .isEqualTo("gcs-connector-private-test-bucket-do-not-delete");
    }

    @Test
    public void getFileInfo_anonymousCredentialProvided_urlPointsToPublicObject_success() throws IOException {
        String gcsObject = "gs://cloud-samples-data/bigquery/us-states/us-states.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(NoCredentials.getInstance(), options);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("bigquery/us-states/us-states.parquet");
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo("cloud-samples-data");
    }

    @Test
    public void getFileInfo_anonymousCredentialProvided_urlPointsToPrivateObject_throws() throws IOException {
        String object = "gs://gcs-connector-private-test-bucket-do-not-delete/tpch_customer_1.parquet";
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();
        GcsFileSystemImpl gcsFileSystem = new GcsFileSystemImpl(NoCredentials.getInstance(), options);

        IOException exception =
                assertThrows(IOException.class, () -> gcsFileSystem.getFileInfo(URI.create(object)));

        assertThat(exception).hasMessageThat().contains("Unable to access blob");
    }
}
