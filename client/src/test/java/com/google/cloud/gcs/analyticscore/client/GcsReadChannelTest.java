/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;

class GcsReadChannelTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static GcsReadOptions TEST_GCS_READ_OPTIONS =
      GcsReadOptions.builder().setProjectId(TEST_PROJECT_ID).build();

  private final Storage storage = LocalStorageHelper.getOptions().getService();

  @Test
  void constructor_nullStorage_throwsNullPointerException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> new GcsReadChannel(null, itemInfo, TEST_GCS_READ_OPTIONS));

    assertThat(e).hasMessageThat().isEqualTo("Storage instance cannot be null");
  }

  @Test
  void constructor_itemInfoDoesNotPointToObject_throws() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("test-bucket").build();
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setContentGeneration(0L).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS));

    assertThat(e).hasMessageThat().isEqualTo("Expected Gcs Object but got " + itemInfo.getItemId());
  }

  @Test
  void constructor_nullItemInfo_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> new GcsReadChannel(storage, null, TEST_GCS_READ_OPTIONS));

    assertThat(e).hasMessageThat().isEqualTo("Item info cannot be null");
  }

  @Test
  void read_inChunks_fillsBuffersAndAdvancesPosition() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    ByteBuffer buffer1 = ByteBuffer.allocate(5);
    ByteBuffer buffer2 = ByteBuffer.allocate(6);

    int bytesRead1 = gcsReadChannel.read(buffer1);
    int bytesRead2 = gcsReadChannel.read(buffer2);

    assertThat(bytesRead1).isEqualTo(5);
    assertThat(new String(buffer1.array(), StandardCharsets.UTF_8)).isEqualTo("hello");
    assertThat(bytesRead2).isEqualTo(6);
    assertThat(new String(buffer2.array(), StandardCharsets.UTF_8)).isEqualTo(" world");
    assertThat(gcsReadChannel.position()).isEqualTo(11);
  }

  @Test
  void read_fullObject_fillEntireObjectIntoBuffer() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo(objectData);
    assertThat(gcsReadChannel.position()).isEqualTo(objectData.length());
  }

  @Test
  void read_withSeek_advancesPositionAndReadsIntoBuffer() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    gcsReadChannel.position(6);
    ByteBuffer buffer = ByteBuffer.allocate(5);

    int bytesRead = gcsReadChannel.read(buffer);

    assertThat(bytesRead).isEqualTo(5);
    assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("world");
    assertThat(gcsReadChannel.position()).isEqualTo(11L);
  }

  @Test
  void position_negative_throwsEOFException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);

    EOFException e = assertThrows(EOFException.class, () -> gcsReadChannel.position(-1L));

    assertThat(e)
        .hasMessageThat()
        .contains("Invalid seek offset: position value (-1) must be >= 0");
  }

  @Test
  void position_equalToSize_throwsEOFException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    long size = objectData.length();

    EOFException e = assertThrows(EOFException.class, () -> gcsReadChannel.position(size));

    assertThat(e)
        .hasMessageThat()
        .contains(
            String.format(
                "Invalid seek offset: position value (%d) must be " + "between 0 and 11 for ",
                itemInfo.getSize()));
  }

  @Test
  void position_greaterThanSize_throwsEOFException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    long size = objectData.length();

    EOFException e = assertThrows(EOFException.class, () -> gcsReadChannel.position(size + 1));

    assertThat(e)
        .hasMessageThat()
        .contains(
            String.format(
                "Invalid seek offset: position value (%d) must be " + "between 0 and %d",
                size + 1, size));
  }

  @Test
  void write_throwsUnsupportedOperationException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    ByteBuffer src = ByteBuffer.allocate(10);

    assertThrows(UnsupportedOperationException.class, () -> gcsReadChannel.write(src));
  }

  @Test
  void truncate_throwsUnsupportedOperationException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);

    assertThrows(UnsupportedOperationException.class, () -> gcsReadChannel.truncate(5L));
  }

  @Test
  void isOpen_forUnClosedChannel_returnsTrue() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);

    assertThat(gcsReadChannel.isOpen()).isTrue();
  }

  @Test
  void isOpen_forClosedChannel_returnsFalse() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);
    gcsReadChannel.close();

    assertThat(gcsReadChannel.isOpen()).isFalse();
  }

  @Test
  void size_returnsBlobContentLength() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);

    assertThat(gcsReadChannel.size()).isEqualTo(objectData.length());
  }

  @Test
  void readVectored_throwsNotImplementedException() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
    String objectData = "hello world";
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    createBlobInStorage(
        BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    GcsReadChannel gcsReadChannel = new GcsReadChannel(storage, itemInfo, TEST_GCS_READ_OPTIONS);

    assertThrows(
        NotImplementedException.class,
        () -> gcsReadChannel.readVectored(new ArrayList<>(), ByteBuffer::allocate));
  }

  private void createBlobInStorage(BlobId blobId, String blobContent) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, blobContent.getBytes(StandardCharsets.UTF_8));
  }
}
