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

import java.util.Map;

class GcsResourceInfo {
    private final GcsResourceId resourceId;

    // Size of an object (number of bytes).
    // Size is -1 for items that do not exist.
    private final long size;
    // Generation ID of the object when the metadata is read.
    private final long contentGeneration;
    // Encoding of object example: gzip.
    private final String contentEncoding;

    public GcsResourceInfo(GcsResourceId resourceId, long size, long contentGeneration, String contentEncoding) {
        this.resourceId = resourceId;
        this.size = size;
        this.contentGeneration = contentGeneration;
        this.contentEncoding = contentEncoding;
    }
}
