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

import com.google.auto.value.AutoValue;

import java.util.Optional;

/**
 * Configuration options for Gcs vectored read.
 */
@AutoValue
public abstract class GcsVectoredReadOptions {

    // The shortest distance allowed between chunks for them to be merged
    abstract int getMinMergeDistance();

    // The max allowed size of the combined chunk.
    abstract int getMaxMergeSize();

    static Builder builder() {
        return new AutoValue_GcsVectoredReadOptions
                .Builder()
                .setMinMergeDistance(4 * 1024) // 4 KB
                .setMaxMergeSize(8 * 1024 * 1024); // 8 MB
    }

    /**
     * Builder for {@link GcsVectoredReadOptions}.
     */
    @AutoValue.Builder
    abstract static class Builder {

        abstract Builder setMinMergeDistance(int minMergeDistance);

        abstract Builder setMaxMergeSize(int maxMergeSize);

        abstract GcsVectoredReadOptions build();
    }
}