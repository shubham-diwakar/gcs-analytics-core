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

/** Configuration options for the GCS client. */
@AutoValue
public abstract class GcsClientOptions {

  abstract Optional<String> getProjectId();

  abstract Optional<String> getClientLibToken();

  abstract Optional<String> getServiceHost();

  abstract Optional<String> getUserAgent();

  static Builder builder() {
    return new AutoValue_GcsClientOptions.Builder();
  }

  /** Builder for {@link GcsClientOptions}. */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setProjectId(String projectId);

    abstract Builder setClientLibToken(String clientLibToken);

    abstract Builder setServiceHost(String serviceHost);

    abstract Builder setUserAgent(String userAgent);

    abstract GcsClientOptions build();
  }
}
