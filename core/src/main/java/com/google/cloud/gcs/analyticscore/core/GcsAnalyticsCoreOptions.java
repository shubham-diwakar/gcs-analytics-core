package com.google.cloud.gcs.analyticscore.core;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class GcsAnalyticsCoreOptions {
  private String prefix;
  private ImmutableMap<String, String> analyticsCoreOptions;

  public GcsAnalyticsCoreOptions(String prefix, Map<String, String> analyticsCoreOptions) {
    this.prefix = prefix;
    this.analyticsCoreOptions = ImmutableMap.copyOf(analyticsCoreOptions);
  }

  public GcsFileSystemOptions getGcsFileSystemOptions() {
    return GcsFileSystemOptions.createFromOptions(analyticsCoreOptions, prefix);
  }
}
