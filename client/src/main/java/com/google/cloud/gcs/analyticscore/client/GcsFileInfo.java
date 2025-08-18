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
import java.net.URI;
import java.util.Map;

/**
 * Contains information about a GCS file.
 *
 * <p>Note: This class wraps GcsItemInfo, adds file system specific information
 * and hides bucket/object specific information.
 */
@AutoValue
abstract class GcsFileInfo {

    abstract GcsItemInfo getItemInfo();

    /**
    * Gets the path of this file or directory.
    */
    abstract URI getUri();

    /**
     * Retrieve file attributes for this file.
     * @return A map of file attributes
     */
    abstract Map<String, byte[]> getAttributes();

    static Builder builder() {
        return new AutoValue_GcsFileInfo.Builder();
    }

    /**
     * Builder for {@link GcsFileInfo}.
     */
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setItemInfo(GcsItemInfo itemInfo);
      abstract Builder setUri(URI uri);
      abstract Builder setAttributes(Map<String, byte[]> attributes);
      abstract GcsFileInfo build();
    }
}