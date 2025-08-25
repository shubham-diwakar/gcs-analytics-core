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
import com.google.common.collect.ImmutableList;

/** Represents a byte range from a GCS object. */
@AutoValue
public abstract class GcsObjectCombinedRange {

  // The individual ranges that make up the combined range.
  public abstract ImmutableList<GcsObjectRange> getUnderlyingRanges();

  // The starting offset of the combined range.
  public abstract long getOffset();

  // The length of the combined range.
  public abstract int getLength();

  public static Builder builder() {
    return new AutoValue_GcsObjectCombinedRange.Builder();
  }

  /** Builder for {@link GcsObjectCombinedRange}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUnderlyingRanges(ImmutableList<GcsObjectRange> underlyingRanges);

    public abstract Builder setOffset(long offset);

    public abstract Builder setLength(int length);

    public abstract GcsObjectCombinedRange build();
  }

  /**
   * Returns a new {@link GcsObjectCombinedRange} representing the union of this combined range and
   * the given {@link GcsObjectRange}. The underlying ranges are combined, and the offset and length
   * are updated to encompass both ranges.
   *
   * <p>Note: This method does not perform safety checks, It simply expands the bounds to include
   * the new range.
   *
   * @param range The {@link GcsObjectRange} to union with this combined range.
   * @return A new {@link GcsObjectCombinedRange} representing the union.
   */
  public GcsObjectCombinedRange union(GcsObjectRange range) {
    ImmutableList.Builder<GcsObjectRange> newRanges = ImmutableList.builder();
    newRanges.addAll(getUnderlyingRanges());
    newRanges.add(range);

    long newOffset = Math.min(getOffset(), range.getOffset());
    long thisEnd = getOffset() + getLength();
    long rangeEnd = range.getOffset() + range.getLength();
    long newEnd = Math.max(thisEnd, rangeEnd);
    int newLength = (int) (newEnd - newOffset);

    return toBuilder()
        .setUnderlyingRanges(newRanges.build())
        .setOffset(newOffset)
        .setLength(newLength)
        .build();
  }

  public abstract Builder toBuilder();
}
