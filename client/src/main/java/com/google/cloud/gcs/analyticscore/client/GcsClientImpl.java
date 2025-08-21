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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
    private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
    private static final List<Storage.BlobField> BLOB_METADATA_FIELDS = ImmutableList.of(Storage.BlobField.GENERATION
            , Storage.BlobField.SIZE);

    @VisibleForTesting
    Storage storage;
    private final GcsClientOptions clientOptions;

    GcsClientImpl(Credentials credentials, GcsClientOptions clientOptions) {
        this.clientOptions = clientOptions;
        this.storage = createStorage(credentials);
    }


    GcsClientImpl(GcsClientOptions clientOptions) {
        this.clientOptions = clientOptions;
        this.storage = createStorage();
    }

    @Override
    public VectoredSeekableByteChannel openReadChannel(GcsItemId itemId, GcsReadOptions readOptions) throws IOException {
        checkNotNull(itemId);
        checkArgument(itemId.isGcsObject(), "Expected GCS object to be provided. But got: " + itemId);
        GcsItemInfo itemInfo = getGcsItemInfo(itemId);

        return new GcsReadChannel(storage, itemInfo, readOptions);
    }

    @Override
    public GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException {
        checkNotNull(itemId, "Item ID must not be null.");
        if (itemId.isGcsObject()) {

            return getGcsObjectInfo(itemId);
        }
        throw new UnsupportedOperationException(String.format("Expected gcs object but got %s", itemId));
    }

    @Override
    public void close() {
        try {
            storage.close();
        } catch (Exception e) {
            LOG.debug("Exception while closing storage instance", e);
        }
    }

    @VisibleForTesting
    Storage createStorage() {
        return createStorage(NoCredentials.getInstance());
    }

    private Storage createStorage(Credentials credentials) {
        checkArgument(clientOptions.getProjectId().isPresent(), "Project Id cannot be null");
        checkNotNull(credentials, "Credentials should not be null");
        List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        clientOptions.getUserAgent().ifPresent(userAgent -> builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-agent", userAgent))));
        clientOptions.getProjectId().ifPresent(builder::setProjectId);
        clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
        clientOptions.getServiceHost().ifPresent(builder::setHost);
        builder.setCredentials(credentials);

        return builder.build().getService();
    }

    private GcsItemInfo getGcsObjectInfo(GcsItemId itemId) throws IOException {
        checkArgument(itemId.isGcsObject(), String.format("Expected gcs object got %s", itemId));
        Blob blob = getBlob(itemId.getBucketName(), itemId.getObjectName().get());
        if (blob == null) {
            throw new IOException("Object not found:" + itemId);
        }

        return GcsItemInfo.builder().setItemId(itemId).setSize(blob.getSize()).setContentGeneration(blob.getGeneration()).build();
    }

    private Blob getBlob(String bucketName, String objectName) throws IOException {
        checkNotNull(bucketName);
        checkNotNull(objectName);
        BlobId blobId = BlobId.of(bucketName, objectName);
        try {
            return storage.get(blobId,
                    Storage.BlobGetOption.fields(BLOB_METADATA_FIELDS.toArray(new Storage.BlobField[0])));
        } catch (StorageException storageException) {
            throw new IOException("Unable to access blob :" + blobId, storageException);
        }
    }
}
