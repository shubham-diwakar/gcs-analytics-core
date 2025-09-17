# Configuration

This document outlines the key configuration properties for the GCS Analytics Core library.

## Footer Prefetching

For file formats that store important metadata in a footer at the end of the file (like Parquet and Apache ORC), reading this footer can involve a separate, high-latency read operation. The footer prefetching feature optimizes this by reading the end of the file on-demand when a read occurs in that region.

### `fs.gs.footer.prefetch.size`

This flag controls the size (in bytes) of the footer to prefetch.

-   **Description**: Determines how many bytes from the end of a file are considered the "footer" and will be cached in memory on the first read within that range. Subsequent reads within this cached range will be served from memory, avoiding network calls.
-   **Default Value**: `2097152` (2 MB)
-   **Usage**:
    -   To **disable** footer prefetching, set this value to `0`.


**Example:**
To disable prefetching:
```properties
fs.gs.footer.prefetch.size=0
```
