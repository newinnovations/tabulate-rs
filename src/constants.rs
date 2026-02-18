/// Sentinel that can be inserted into the data to request a separating line.
///
/// Matches the value used by the original python-tabulate implementation.
pub const SEPARATING_LINE: &str = "\u{1}";

/// Minimum spacing applied around headers to match python-tabulate defaults.
pub const MIN_PADDING: usize = 2;

/// Table formats that expand multiline cells into additional rendered lines.
///
/// Mirrors the formats listed in python-tabulate 0.9.0's `multiline_formats`.
pub const MULTILINE_FORMATS: &[&str] = &[
    "plain",
    "simple",
    "grid",
    "simple_grid",
    "rounded_grid",
    "heavy_grid",
    "mixed_grid",
    "double_grid",
    "fancy_grid",
    "pipe",
    "orgtbl",
    "jira",
    "presto",
    "pretty",
    "psql",
    "rst",
    "simple_outline",
    "rounded_outline",
    "heavy_outline",
    "mixed_outline",
    "double_outline",
    "fancy_outline",
];
