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
package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GcsInputStreamTest {

  private final URI testUri = URI.create("gs://test-bucket/test-object");
  @Mock private VectoredSeekableByteChannel mockChannel;
  @Mock private GcsFileSystem mockFileSystem;
  private GcsInputStream gcsInputStream;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    gcsInputStream = new GcsInputStream(mockChannel, testUri);
  }

  @Test
  void create_usesFileSystemOptions_openChannelAndCallsConstructor() throws IOException {
    GcsFileSystemOptions mockFileSystemOptions = mock(GcsFileSystemOptions.class);
    GcsClientOptions mockClientOptions = mock(GcsClientOptions.class);
    GcsReadOptions readOptions = GcsReadOptions.builder().build();
    when(mockFileSystem.getFileSystemOptions()).thenReturn(mockFileSystemOptions);
    when(mockFileSystemOptions.getGcsClientOptions()).thenReturn(mockClientOptions);
    when(mockClientOptions.getReadOptions()).thenReturn(Optional.of(readOptions));
    when(mockFileSystem.open(testUri, readOptions)).thenReturn(mockChannel);

    GcsInputStream stream = GcsInputStream.create(mockFileSystem, testUri);

    verify(mockFileSystem).open(testUri, readOptions);
    assertThat(stream.getPos()).isEqualTo(0);
  }

  @Test
  void getPos_onNewStream_returnsInitialPosition() {
    assertThat(gcsInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void seek_updatesPositionAndUnderlyingChannel() throws IOException {
    gcsInputStream.seek(123L);

    assertThat(gcsInputStream.getPos()).isEqualTo(123L);
    verify(mockChannel).position(123L);
  }

  @Test
  void seek_withNegativePosition_throwsIllegalArgumentException() {
    var exception = assertThrows(IllegalArgumentException.class, () -> gcsInputStream.seek(-1L));

    assertThat(exception).hasMessageThat().contains("position can't be negative: -1");
  }

  @Test
  void seek_afterClose_throwsIOException() throws IOException {
    gcsInputStream.close();

    var exception = assertThrows(IOException.class, () -> gcsInputStream.seek(10));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot seek: already closed");
  }

  @Test
  void read_singleByte_readsFromChannelAndUpdatesPosition() throws IOException {
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put((byte) 77);
              return 1;
            });

    int result = gcsInputStream.read();

    assertThat(result).isEqualTo(77);
    assertThat(gcsInputStream.getPos()).isEqualTo(1);
  }

  @Test
  void read_singleByteAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);

    int result = gcsInputStream.read();

    assertThat(result).isEqualTo(-1);
    assertThat(gcsInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_singleByteAfterClose_throwsIOException() throws IOException {
    gcsInputStream.close();

    var exception = assertThrows(IOException.class, () -> gcsInputStream.read());

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void read_byteArray_readsFromChannelAndUpdatesPosition() throws IOException {
    byte[] data = "test-data".getBytes();
    when(mockChannel.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              inv.<ByteBuffer>getArgument(0).put(data);
              return data.length;
            });
    byte[] buffer = new byte[20];

    int bytesRead = gcsInputStream.read(buffer, 0, buffer.length);

    assertThat(bytesRead).isEqualTo(data.length);
    assertThat(gcsInputStream.getPos()).isEqualTo(data.length);
  }

  @Test
  void read_byteArrayAtEOF_returnsMinusOneAndDoesNotUpdatePosition() throws IOException {
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);
    byte[] buffer = new byte[20];

    int bytesRead = gcsInputStream.read(buffer, 0, buffer.length);

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(gcsInputStream.getPos()).isEqualTo(0);
  }

  @Test
  void read_byteArrayWithNegativeLength_returnsIndexOutOfBound() throws IOException {
    byte[] buffer = new byte[20];
    assertThrows(
        IndexOutOfBoundsException.class, () -> gcsInputStream.read(buffer, 0, -1 * buffer.length));
  }

  @Test
  void read_byteArrayWithNegativeOffset_returnsIndexOutOfBound() throws IOException {
    byte[] buffer = new byte[20];
    assertThrows(
        IndexOutOfBoundsException.class, () -> gcsInputStream.read(buffer, -1, buffer.length));
  }

  @Test
  void read_postEndOfBuffer_returnsIndexOutOfBound() throws IOException {
    byte[] buffer = new byte[20];
    assertThrows(
        IndexOutOfBoundsException.class, () -> gcsInputStream.read(buffer, 15, buffer.length / 2));
  }

    @Test
    void read_zeroLength_returnsZeroBytes() throws IOException {
        byte[] buffer = new byte[20];

        int bytesRead = gcsInputStream.read(buffer, 0,0);

        assertThat(bytesRead).isEqualTo(0);
        assertThat(gcsInputStream.getPos()).isEqualTo(0);
    }

  @Test
  void read_afterClose_throwsIOException() throws IOException {
    gcsInputStream.close();
    byte[] buffer = new byte[20];

    var exception =
        assertThrows(IOException.class, () -> gcsInputStream.read(buffer, 0, buffer.length));

    assertThat(exception).hasMessageThat().isEqualTo(testUri + ": Cannot read: already closed");
  }

  @Test
  void close_closesUnderlyingChannel() throws IOException {
    gcsInputStream.close();
    verify(mockChannel).close();
  }

  @Test
  void close_isIdempotent() throws IOException {
    gcsInputStream.close();
    gcsInputStream.close();
    verify(mockChannel, times(1)).close();
  }

  @Test
  void readFully_throwsUnsupported() {
    assertThrows(
        UnsupportedOperationException.class, () -> gcsInputStream.readFully(0, new byte[1], 0, 1));
  }

  @Test
  void readTail_throwsUnsupported() {
    assertThrows(
        UnsupportedOperationException.class, () -> gcsInputStream.readTail(new byte[1], 0, 1));
  }

  @Test
  void readVectored_throwsUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> gcsInputStream.readVectored(Collections.emptyList(), size -> null));
  }
}
