# Configuration

This document outlines the key configuration properties for the GCS Analytics Core library.

## Footer Prefetching

For file formats that store important metadata in a footer at the end of the file (like Parquet and Apache ORC), reading this footer can involve a separate, high-latency read operation. The footer prefetching feature optimizes this by reading the end of the file on-demand when a read occurs in that region.

### `fs.gs.footer.prefetch.enabled`

This flag controls whether footer prefetching is enabled.

-   **Description**: When enabled, a portion of the end of a file (the "footer") is cached in memory on the first read within that range. Subsequent reads within this cached range are served from memory, avoiding network calls.
-   **Default Value**: `true`
-   **Usage**:
    -   To **disable** footer prefetching, set this value to `false`.

**Example:**
To disable prefetching:
```properties
fs.gs.footer.prefetch.enabled=false
```

### `fs.gs.footer.prefetch.size.small-file`

-   **Description**: Controls the size (in bytes) of the footer to prefetch for files up to 1 GB in size.
-   **Default Value**: `102400` (100 KB)

### `fs.gs.footer.prefetch.size.large-file`

-   **Description**: Controls the size (in bytes) of the footer to prefetch for files larger than 1 GB.
-   **Default Value**: `1048576` (1 MB)

**Example:**
To set a 2MB prefetch size for large files and 200KB for small files:
```properties
fs.gs.footer.prefetch.size.large-file=2097152
fs.gs.footer.prefetch.size.small-file=204800
```
