use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use serde::Serialize;
use serde_json::{Map, Value};
use textwrap::{Options as WrapOptions, WordSplitter, wrap};

use crate::{
    alignment::{Alignment, DecimalLayout, align_cell},
    constants::{MIN_PADDING, MULTILINE_FORMATS, SEPARATING_LINE},
    format::{LineFormat, RowFormat, TableFormat, table_format},
    options::{
        FormatSpec, HeaderAlignment, Headers, MissingValues, RowAlignment, ShowIndex,
        TableFormatChoice, TabulateOptions,
    },
    width::visible_width,
};

/// Errors emitted while attempting to render a table.
#[derive(thiserror::Error, Debug)]
pub enum TabulateError {
    /// The requested table format is unknown.
    #[error("unknown table format: {0}")]
    UnknownFormat(String),
    /// Attempted to tabulate data that does not contain any rows.
    #[error("no rows to tabulate")]
    EmptyData,
    /// Provided explicit headers for a list of dict-like rows.
    #[error(
        "headers for a list of dicts must be a mapping or keyword such as 'keys' or 'firstrow'"
    )]
    InvalidHeadersForObjects,
    /// Attempted to use a headers variant that is not yet supported.
    #[error("header mode '{0}' is not yet supported in the Rust port")]
    UnsupportedHeaders(String),
    /// Provided index values length does not match the number of data rows.
    #[error("index length {found} does not match number of rows {expected}")]
    IndexLengthMismatch {
        /// Number of data rows expected to match the index values length.
        expected: usize,
        /// Number of index values provided by the caller.
        found: usize,
    },
    /// Converting user data into a tabular representation failed.
    #[error("failed to serialise row: {0}")]
    Serialization(String),
}

/// Render `tabular_data` according to the provided `options`.
pub fn tabulate<Data, Row>(
    tabular_data: Data,
    options: TabulateOptions,
) -> Result<String, TabulateError>
where
    Data: IntoIterator<Item = Row>,
    Row: Serialize,
{
    Tabulator::new(options).tabulate(tabular_data)
}

/// Stateful renderer used to build textual tables.
pub struct Tabulator {
    options: TabulateOptions,
}

impl Tabulator {
    /// Create a new renderer.
    pub fn new(options: TabulateOptions) -> Self {
        Self { options }
    }

    /// Render the given iterator of rows into a table string.
    pub fn tabulate<Data, Row>(&self, tabular_data: Data) -> Result<String, TabulateError>
    where
        Data: IntoIterator<Item = Row>,
        Row: Serialize,
    {
        let (format, format_name_opt) = self.resolve_format()?;
        let mut options = self.options.clone();
        let format_name_lower = format_name_opt.map(|name| name.to_ascii_lowercase());
        let format_name_key = format_name_lower.as_deref();

        if matches!(format_name_key, Some("pretty")) {
            options.disable_numparse = true;
            if options.num_align.is_none() {
                options.num_align = Some(Alignment::Center);
            }
            if options.str_align.is_none() {
                options.str_align = Some(Alignment::Center);
            }
        }
        if matches!(format_name_key, Some("colon_grid")) && options.col_global_align.is_none() {
            options.col_global_align = Some(Alignment::Left);
        }

        let mut normalized = normalize_tabular_data(tabular_data, &options)?;

        let default_index = normalized.default_index.clone();
        let default_index_header = normalized.default_index_header.clone();
        apply_show_index(
            &mut normalized.rows,
            &mut normalized.headers,
            &options.show_index,
            options.disable_numparse,
            default_index,
            default_index_header,
        )?;

        apply_wrapping(&mut normalized.rows, &mut normalized.headers, &options);

        let render_plan = RenderPlan::build(
            &normalized.rows,
            &normalized.headers,
            format,
            &options,
            options.table_format_name(),
        );
        Ok(render_plan.render())
    }

    fn resolve_format(&self) -> Result<(&TableFormat, Option<&str>), TabulateError> {
        match &self.options.table_format {
            TableFormatChoice::Name(name) => table_format(name)
                .map(|format| (format as &TableFormat, Some(name.as_str())))
                .ok_or_else(|| TabulateError::UnknownFormat(name.clone())),
            TableFormatChoice::Custom(format) => Ok((&**format, None)),
        }
    }
}

#[derive(Clone, Debug)]
struct CellData {
    text: String,
    value: ParsedValue,
}

impl CellData {
    fn new(text: String, value: ParsedValue) -> Self {
        Self { text, value }
    }

    fn empty() -> Self {
        Self::new(String::new(), ParsedValue::String)
    }
}

#[derive(Clone, Debug)]
enum ParsedValue {
    Int(i128),
    Float(f64),
    Bool(bool),
    String,
}

fn parse_value(text: &str, disable_numparse: bool) -> ParsedValue {
    if disable_numparse {
        return ParsedValue::String;
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return ParsedValue::String;
    }

    let normalized = normalize_numeric_candidate(trimmed);
    if let Ok(value) = normalized.parse::<i128>() {
        return ParsedValue::Int(value);
    }

    if let Ok(value) = normalized.parse::<f64>() {
        return ParsedValue::Float(value);
    }

    match trimmed {
        "true" | "True" => ParsedValue::Bool(true),
        "false" | "False" => ParsedValue::Bool(false),
        _ => ParsedValue::String,
    }
}

#[derive(Clone, Debug)]
enum RowKind {
    Data(Vec<CellData>),
    Separator,
}

struct NormalizedTable {
    headers: Vec<String>,
    rows: Vec<RowKind>,
    default_index: Option<Vec<String>>,
    default_index_header: Option<String>,
}

#[derive(Clone, Debug)]
enum RowValue {
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
    Separator,
}

fn process_item(value: Value, output: &mut Vec<Value>) {
    output.push(value);
}

fn expand_columnar_object(row_values: &mut Vec<RowValue>) -> Option<Vec<String>> {
    if row_values.len() != 1 {
        return None;
    }

    let original_row = row_values.remove(0);
    let entries = match original_row {
        RowValue::Object(entries) => entries,
        other => {
            row_values.insert(0, other);
            return None;
        }
    };

    let mut columns = Vec::with_capacity(entries.len());
    let mut keys = Vec::with_capacity(entries.len());
    let mut max_len = 0usize;

    for (key, value) in entries.iter() {
        match value {
            Value::Array(items) => {
                max_len = max(max_len, items.len());
                columns.push(items.clone());
                keys.push(key.clone());
            }
            _ => {
                row_values.insert(0, RowValue::Object(entries));
                return None;
            }
        }
    }

    if max_len == 0 {
        row_values.clear();
        return Some(keys);
    }

    for column in columns.iter_mut() {
        while column.len() < max_len {
            column.push(Value::Null);
        }
    }

    let mut expanded = Vec::with_capacity(max_len);
    for idx in 0..max_len {
        let mut row = Vec::with_capacity(columns.len());
        for column in &columns {
            row.push(column[idx].clone());
        }
        expanded.push(RowValue::Array(row));
    }

    *row_values = expanded;
    Some(keys)
}

struct DataFrameData {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    index: Option<Vec<Value>>,
    index_label: Option<String>,
}

struct NumpyRecArrayData {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

fn parse_dataframe(value: Value) -> Option<DataFrameData> {
    let mut map = match value {
        Value::Object(map) => map,
        _ => return None,
    };

    match map.remove("__tabulate_dataframe__") {
        Some(Value::Bool(true)) => {}
        _ => return None,
    }

    let columns = match map.remove("columns")? {
        Value::Array(values) => values.into_iter().map(value_to_plain_string).collect(),
        _ => return None,
    };

    let rows = match map.remove("data")? {
        Value::Array(rows) => {
            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                match row {
                    Value::Array(values) => result.push(values),
                    _ => return None,
                }
            }
            result
        }
        _ => return None,
    };

    let index = match map.remove("index") {
        Some(Value::Array(values)) => Some(values),
        Some(Value::Null) | None => None,
        _ => return None,
    };

    let mut index_label = match map.remove("index_label") {
        Some(Value::String(label)) => Some(label),
        Some(Value::Null) => None,
        Some(Value::Array(labels)) => {
            let combined = labels
                .into_iter()
                .map(value_to_plain_string)
                .collect::<Vec<_>>()
                .join(" ");
            if combined.is_empty() {
                None
            } else {
                Some(combined)
            }
        }
        Some(_) => return None,
        None => None,
    };

    if index_label.is_none() {
        if let Some(Value::String(label)) = map.remove("index_name") {
            index_label = Some(label);
        } else if let Some(Value::Array(labels)) = map.remove("index_names") {
            let combined = labels
                .into_iter()
                .map(value_to_plain_string)
                .collect::<Vec<_>>()
                .join(" ");
            if !combined.is_empty() {
                index_label = Some(combined);
            }
        }
    }

    Some(DataFrameData {
        columns,
        rows,
        index,
        index_label,
    })
}

fn parse_numpy_recarray(value: &Value) -> Option<NumpyRecArrayData> {
    let map = match value {
        Value::Object(map) => map,
        _ => return None,
    };
    match map.get("__tabulate_numpy_recarray__") {
        Some(Value::Bool(true)) => {}
        _ => return None,
    }
    let dtype = match map.get("dtype") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|entry| match entry {
                Value::Array(parts) if !parts.is_empty() => parts
                    .first()
                    .and_then(|name| name.as_str())
                    .map(str::to_string),
                _ => None,
            })
            .collect::<Vec<_>>(),
        _ => return None,
    };
    if dtype.is_empty() {
        return None;
    }
    let rows = match map.get("rows") {
        Some(Value::Array(items)) => items
            .iter()
            .map(|row| match row {
                Value::Array(values) => values.clone(),
                _ => Vec::new(),
            })
            .collect::<Vec<_>>(),
        _ => return None,
    };
    if rows.iter().any(|row| row.len() != dtype.len()) {
        return None;
    }
    Some(NumpyRecArrayData {
        columns: dtype,
        rows,
    })
}

fn value_to_plain_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn normalize_tabular_data<Data, Row>(
    tabular_data: Data,
    options: &TabulateOptions,
) -> Result<NormalizedTable, TabulateError>
where
    Data: IntoIterator<Item = Row>,
    Row: Serialize,
{
    let mut raw_rows = Vec::new();
    for item in tabular_data {
        process_item(
            serde_json::to_value(item)
                .map_err(|err| TabulateError::Serialization(err.to_string()))?,
            &mut raw_rows,
        );
    }

    let mut keys_order = Vec::new();
    let mut seen_keys = HashSet::new();

    let mut row_values = Vec::new();
    let mut default_index: Option<Vec<String>> = None;
    let mut default_index_header: Option<String> = None;
    let mut has_dataframe = false;
    let mut had_object_row = false;
    for value in raw_rows {
        if let Some(recarray) = parse_numpy_recarray(&value) {
            for column in &recarray.columns {
                if seen_keys.insert(column.clone()) {
                    keys_order.push(column.clone());
                }
            }
            for row in recarray.rows {
                row_values.push(RowValue::Array(row));
            }
            continue;
        }

        if let Some(dataframe) = parse_dataframe(value.clone()) {
            has_dataframe = true;
            let DataFrameData {
                columns,
                rows,
                index,
                index_label,
            } = dataframe;

            let row_count = rows.len();

            if default_index.is_none()
                && let Some(index_values) = index
                && index_values.len() == row_count
            {
                let mut converted = Vec::with_capacity(index_values.len());
                for value in index_values {
                    let cell = cell_from_value(value, options, None);
                    converted.push(cell.text);
                }
                default_index = Some(converted);
            }
            if default_index_header.is_none() {
                default_index_header = index_label;
            }

            for value in columns.iter() {
                if seen_keys.insert(value.clone()) {
                    keys_order.push(value.clone());
                }
            }

            for mut row in rows.into_iter() {
                if row.len() < columns.len() {
                    row.resize(columns.len(), Value::Null);
                }
                let entries = columns
                    .iter()
                    .cloned()
                    .zip(row.into_iter())
                    .collect::<Vec<_>>();
                row_values.push(RowValue::Object(entries));
                had_object_row = true;
            }
            continue;
        }

        if is_separator_value(&value) {
            row_values.push(RowValue::Separator);
            continue;
        }
        match value {
            Value::Array(values) => row_values.push(RowValue::Array(values)),
            Value::Object(map) => {
                had_object_row = true;
                row_values.push(RowValue::Object(map.into_iter().collect()));
            }
            other => row_values.push(RowValue::Array(vec![other])),
        }
    }

    let columnar_keys = expand_columnar_object(&mut row_values);

    if had_object_row
        && columnar_keys.is_none()
        && !has_dataframe
        && let Headers::Explicit(values) = &options.headers
        && !values.is_empty()
    {
        return Err(TabulateError::InvalidHeadersForObjects);
    }

    let mut headers = Vec::new();
    if let Some(keys) = &columnar_keys {
        for key in keys {
            if seen_keys.insert(key.clone()) {
                keys_order.push(key.clone());
            }
        }
    }
    let mut first_header_map: Option<Vec<(String, Value)>> = None;
    let header_mapping = match &options.headers {
        Headers::Mapping(entries) => Some(entries.clone()),
        _ => None,
    };
    let mapping_keys: Option<Vec<String>> = header_mapping
        .as_ref()
        .map(|entries| entries.iter().map(|(key, _)| key.clone()).collect());

    if matches!(options.headers, Headers::FirstRow)
        && let Some(index) = row_values
            .iter()
            .position(|row| matches!(row, RowValue::Array(_) | RowValue::Object(_)))
    {
        match row_values.remove(index) {
            RowValue::Array(values) => {
                headers = values
                    .into_iter()
                    .map(|value| value_to_header(&value, options.preserve_whitespace))
                    .collect();
            }
            RowValue::Object(entries) => {
                for (key, _) in &entries {
                    if seen_keys.insert(key.clone()) {
                        keys_order.push(key.clone());
                    }
                }
                first_header_map = Some(entries);
            }
            RowValue::Separator => {}
        }
    }

    for row in &row_values {
        if let RowValue::Object(entries) = row {
            for (key, _) in entries {
                if seen_keys.insert(key.clone()) {
                    keys_order.push(key.clone());
                }
            }
        }
    }

    if let Some(mapping_keys) = &mapping_keys {
        for key in mapping_keys {
            if seen_keys.insert(key.clone()) {
                keys_order.push(key.clone());
            }
        }
    }

    if let Some(entries) = &first_header_map {
        let mut header_lookup = HashMap::new();
        for (key, value) in entries {
            header_lookup.insert(key.clone(), value.clone());
        }
        headers = keys_order
            .iter()
            .map(|key| {
                header_lookup
                    .get(key)
                    .map(|value| value_to_header(value, options.preserve_whitespace))
                    .unwrap_or_else(|| key.clone())
            })
            .collect();
    }

    let mut column_count = 0usize;
    for row in &row_values {
        match row {
            RowValue::Array(values) => column_count = column_count.max(values.len()),
            RowValue::Object(_) => column_count = column_count.max(keys_order.len()),
            RowValue::Separator => {}
        }
    }

    if let Some(mapping) = &header_mapping {
        column_count = column_count.max(mapping.len());
    }

    let candidate_headers = headers;
    let headers = match &options.headers {
        Headers::Explicit(values) => values.clone(),
        Headers::Mapping(entries) => entries.iter().map(|(_, label)| label.clone()).collect(),
        Headers::None => {
            if candidate_headers.is_empty() {
                vec![String::new(); column_count]
            } else {
                candidate_headers.clone()
            }
        }
        Headers::FirstRow => candidate_headers.clone(),
        Headers::Keys => {
            if !keys_order.is_empty() {
                keys_order.clone()
            } else {
                (0..column_count).map(|idx| idx.to_string()).collect()
            }
        }
    };

    let mut headers = headers;
    column_count = column_count.max(headers.len());
    if headers.len() < column_count {
        let pad = column_count - headers.len();
        let mut padded = Vec::with_capacity(column_count);
        padded.extend((0..pad).map(|_| String::new()));
        padded.extend(headers);
        headers = padded;
    }

    let object_columns = if let Some(mapping_keys) = &mapping_keys {
        mapping_keys.clone()
    } else if !keys_order.is_empty() {
        keys_order.clone()
    } else {
        headers.clone()
    };

    let mut rows = Vec::new();
    for row in row_values {
        match row {
            RowValue::Separator => rows.push(RowKind::Separator),
            RowValue::Array(values) => {
                let mut cells = if let Some(keys) = &columnar_keys {
                    let mut map = HashMap::new();
                    for (idx, value) in values.into_iter().enumerate() {
                        if let Some(key) = keys.get(idx) {
                            map.insert(key.clone(), value);
                        }
                    }
                    let mut ordered = Vec::with_capacity(column_count);
                    for (col_idx, key) in object_columns.iter().enumerate() {
                        let value = map.remove(key).unwrap_or(Value::Null);
                        ordered.push(cell_from_value(value, options, Some(col_idx)));
                    }
                    ordered
                } else {
                    values
                        .into_iter()
                        .enumerate()
                        .map(|(col_idx, value)| cell_from_value(value, options, Some(col_idx)))
                        .collect::<Vec<_>>()
                };
                while cells.len() < column_count {
                    cells.push(CellData::empty());
                }
                rows.push(RowKind::Data(cells));
            }
            RowValue::Object(entries) => {
                let mut map = Map::new();
                for (key, value) in entries {
                    map.insert(key, value);
                }
                let mut cells = Vec::with_capacity(column_count);
                for (col_idx, key) in object_columns.iter().enumerate() {
                    let value = map.get(key).cloned().unwrap_or(Value::Null);
                    cells.push(cell_from_value(value, options, Some(col_idx)));
                }
                while cells.len() < column_count {
                    cells.push(CellData::empty());
                }
                rows.push(RowKind::Data(cells));
            }
        }
    }

    Ok(NormalizedTable {
        headers,
        rows,
        default_index,
        default_index_header,
    })
}

fn cell_from_value(value: Value, options: &TabulateOptions, column: Option<usize>) -> CellData {
    let text = match value {
        Value::Null => String::new(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(s) => normalize_cell_text(&s, options.preserve_whitespace),
        other => other.to_string(),
    };
    let parsed = parse_value(&text, options.is_numparse_disabled(column));
    CellData::new(text, parsed)
}

fn value_to_header(value: &Value, preserve_whitespace: bool) -> String {
    match value {
        Value::String(s) => normalize_cell_text(s, preserve_whitespace),
        Value::Null => String::new(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

fn normalize_cell_text(text: &str, preserve_whitespace: bool) -> String {
    if preserve_whitespace {
        text.to_string()
    } else {
        text.trim().to_string()
    }
}

fn is_separator_value(value: &Value) -> bool {
    match value {
        Value::String(text) => is_separator_str(text),
        Value::Array(items) => items.iter().take(2).any(|item| match item {
            Value::String(text) => is_separator_str(text),
            _ => false,
        }),
        _ => false,
    }
}

fn is_separator_str(value: &str) -> bool {
    value.trim() == SEPARATING_LINE
}

fn apply_show_index(
    rows: &mut [RowKind],
    headers: &mut Vec<String>,
    show_index: &ShowIndex,
    disable_numparse: bool,
    default_index: Option<Vec<String>>,
    default_index_header: Option<String>,
) -> Result<(), TabulateError> {
    let data_row_count = rows
        .iter()
        .filter(|row| matches!(row, RowKind::Data(_)))
        .count();

    match show_index {
        ShowIndex::Default => {
            if let Some(values) = default_index {
                if values.len() != data_row_count {
                    return Err(TabulateError::IndexLengthMismatch {
                        expected: data_row_count,
                        found: values.len(),
                    });
                }
                prepend_index_column(
                    rows,
                    headers,
                    values,
                    disable_numparse,
                    default_index_header,
                );
            }
            Ok(())
        }
        ShowIndex::Never => Ok(()),
        ShowIndex::Always => {
            let values = match default_index {
                Some(values) => values,
                None => (0..data_row_count).map(|idx| idx.to_string()).collect(),
            };
            if values.len() != data_row_count {
                return Err(TabulateError::IndexLengthMismatch {
                    expected: data_row_count,
                    found: values.len(),
                });
            }
            prepend_index_column(
                rows,
                headers,
                values,
                disable_numparse,
                default_index_header,
            );
            Ok(())
        }
        ShowIndex::Values(values) => {
            if values.len() != data_row_count {
                return Err(TabulateError::IndexLengthMismatch {
                    expected: data_row_count,
                    found: values.len(),
                });
            }
            prepend_index_column(rows, headers, values.clone(), disable_numparse, None);
            Ok(())
        }
    }
}

fn prepend_index_column(
    rows: &mut [RowKind],
    headers: &mut Vec<String>,
    values: Vec<String>,
    disable_numparse: bool,
    header_label: Option<String>,
) {
    let mut iter = values.into_iter();
    let mut inserted = false;
    for row in rows.iter_mut() {
        if let RowKind::Data(cells) = row
            && let Some(value) = iter.next()
        {
            let parsed = parse_value(&value, disable_numparse);
            cells.insert(0, CellData::new(value, parsed));
            inserted = true;
        }
    }
    if inserted {
        headers.insert(0, header_label.unwrap_or_default());
    }
}

fn apply_wrapping(rows: &mut [RowKind], headers: &mut [String], options: &TabulateOptions) {
    let column_count = headers.len();
    if column_count == 0 {
        return;
    }

    let col_widths = expand_widths(&options.max_col_widths, column_count);
    let header_widths = expand_widths(&options.max_header_col_widths, column_count);

    for (idx, width_opt) in header_widths.iter().copied().enumerate() {
        if let Some(width) = width_opt
            && width > 0
            && max_line_width(&headers[idx], options.enable_widechars) > width
        {
            let wrapped = wrap_text(
                &headers[idx],
                width,
                options.break_long_words,
                options.break_on_hyphens,
            );
            headers[idx] = wrapped.join("\n");
        }
    }

    for row in rows.iter_mut() {
        if let RowKind::Data(cells) = row {
            for (idx, cell) in cells.iter_mut().enumerate() {
                if let Some(width) = col_widths.get(idx).copied().flatten() {
                    if width == 0 {
                        continue;
                    }
                    if max_line_width(&cell.text, options.enable_widechars) <= width {
                        continue;
                    }
                    if !options.is_numparse_disabled(Some(idx))
                        && matches!(cell.value, ParsedValue::Int(_) | ParsedValue::Float(_))
                    {
                        continue;
                    }
                    let wrapped = wrap_text(
                        &cell.text,
                        width,
                        options.break_long_words,
                        options.break_on_hyphens,
                    );
                    if !wrapped.is_empty() {
                        cell.text = wrapped.join("\n");
                        cell.value = ParsedValue::String;
                    }
                }
            }
        }
    }
}

fn expand_widths(spec: &Option<Vec<Option<usize>>>, column_count: usize) -> Vec<Option<usize>> {
    match spec {
        None => vec![None; column_count],
        Some(values) => {
            if values.is_empty() {
                return vec![None; column_count];
            }
            let mut result = Vec::with_capacity(column_count);
            let mut iter = values.iter().cloned();
            let mut last = iter.next().unwrap_or(None);
            result.push(last);
            for _idx in 1..column_count {
                if let Some(value) = iter.next() {
                    last = value;
                }
                result.push(last);
            }
            result
        }
    }
}

fn wrap_text(
    text: &str,
    width: usize,
    break_long_words: bool,
    break_on_hyphens: bool,
) -> Vec<String> {
    if width == 0 {
        return vec![text.to_string()];
    }

    let base = WrapOptions::new(width).break_words(break_long_words);
    let options = if break_on_hyphens {
        base.word_splitter(WordSplitter::HyphenSplitter)
    } else {
        base.word_splitter(WordSplitter::NoHyphenation)
    };

    let mut lines = Vec::new();
    for raw_line in text.split('\n') {
        if raw_line.is_empty() {
            lines.push(String::new());
            continue;
        }
        let wrapped = wrap(raw_line, &options);
        if wrapped.is_empty() {
            lines.push(raw_line.to_string());
        } else {
            lines.extend(wrapped.into_iter().map(|segment| segment.into_owned()));
        }
    }

    if lines.is_empty() {
        vec![text.to_string()]
    } else {
        lines
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnType {
    Int,
    Float,
    Bool,
    String,
}

impl ColumnType {
    fn more_generic(self, other: ColumnType) -> ColumnType {
        use ColumnType::*;
        match (self, other) {
            (String, _) | (_, String) => String,
            (Float, _) | (_, Float) => Float,
            (Int, Int) => Int,
            (Int, Bool) | (Bool, Int) => Int,
            (Bool, Bool) => Bool,
        }
    }
}

struct RenderPlan {
    has_header: bool,
    header_lines: Vec<String>,
    data_lines: Vec<Vec<String>>,
    sequence: Vec<RowMarker>,
    line_above: Option<String>,
    line_below_header: Option<String>,
    line_between_rows: Option<String>,
    line_below: Option<String>,
    separator_fallback: Option<String>,
}

#[derive(Clone, Debug)]
enum RowMarker {
    Data(usize),
    Separator,
}

impl RenderPlan {
    fn build(
        rows: &[RowKind],
        headers: &[String],
        format: &TableFormat,
        options: &TabulateOptions,
        format_name: Option<&str>,
    ) -> Self {
        let mut data_rows = Vec::new();
        let mut sequence = Vec::new();
        for row in rows {
            match row {
                RowKind::Data(cells) => {
                    let index = data_rows.len();
                    data_rows.push(cells.clone());
                    sequence.push(RowMarker::Data(index));
                }
                RowKind::Separator => sequence.push(RowMarker::Separator),
            }
        }

        let row_count = data_rows.len();
        let column_count = headers.len();
        let mut formatted_rows = vec![vec![String::new(); column_count]; row_count];
        let mut column_types = vec![ColumnType::Bool; column_count];

        for col in 0..column_count {
            let mut current_type = ColumnType::Bool;
            let mut seen_non_empty = false;
            for row in &data_rows {
                let cell = &row[col];
                if cell.text.is_empty() && matches!(cell.value, ParsedValue::String) {
                    continue;
                }
                seen_non_empty = true;
                let value_type = match cell.value {
                    ParsedValue::Int(_) => ColumnType::Int,
                    ParsedValue::Float(_) => ColumnType::Float,
                    ParsedValue::Bool(_) => ColumnType::Bool,
                    ParsedValue::String => ColumnType::String,
                };
                current_type = current_type.more_generic(value_type);
                if current_type == ColumnType::String {
                    break;
                }
            }
            if !seen_non_empty {
                current_type = ColumnType::String;
            }
            column_types[col] = if options.is_numparse_disabled(Some(col)) {
                ColumnType::String
            } else {
                current_type
            };
        }

        let float_formats = resolve_formats(&options.float_format, column_count, "g");
        let int_formats = resolve_formats(&options.int_format, column_count, "");
        let missing_values = resolve_missing_values(&options.missing_values, column_count);

        for (row_idx, row) in data_rows.iter().enumerate() {
            for col_idx in 0..column_count {
                formatted_rows[row_idx][col_idx] = format_cell(
                    &row[col_idx],
                    column_types[col_idx],
                    &float_formats[col_idx],
                    &int_formats[col_idx],
                    &missing_values[col_idx],
                );
            }
        }

        let formatted_headers: Vec<String> = headers.to_vec();
        let format_name_lower = format_name.map(|name| name.to_ascii_lowercase());
        let format_name_key = format_name_lower.as_deref().unwrap_or("");
        let supports_multiline = format_name_lower
            .as_deref()
            .map(|name| MULTILINE_FORMATS.contains(&name))
            .unwrap_or(false);

        let mut content_widths = vec![0usize; column_count];
        let mut decimal_widths: Vec<Option<DecimalLayout>> = vec![None; column_count];

        let enable_widechars = options.enable_widechars;

        for col_idx in 0..column_count {
            for row in &formatted_rows {
                content_widths[col_idx] = max(
                    content_widths[col_idx],
                    cell_width(&row[col_idx], supports_multiline, enable_widechars),
                );
            }
            content_widths[col_idx] = max(
                content_widths[col_idx],
                cell_width(
                    &formatted_headers[col_idx],
                    supports_multiline,
                    enable_widechars,
                ),
            );
        }

        if format_name_key == "moinmoin" {
            for (idx, width) in content_widths.iter_mut().enumerate() {
                match column_types.get(idx).copied().unwrap_or(ColumnType::String) {
                    ColumnType::String => *width += 1,
                    _ => *width += 2,
                }
            }
        } else if format_name_key == "colon_grid" {
            for (idx, width) in content_widths.iter_mut().enumerate() {
                if matches!(column_types.get(idx), Some(ColumnType::String)) {
                    *width += 1;
                }
            }
        }

        #[cfg(test)]
        if format_name_key == "moinmoin" {
            dbg!(&content_widths);
            dbg!(&formatted_headers);
            dbg!(&formatted_rows);
        }

        let has_header = headers.iter().any(|h| !h.is_empty());
        let min_padding = if format_name_key == "pretty" {
            0
        } else {
            MIN_PADDING
        };

        if has_header {
            let format_requires_padding = format_name_key.starts_with("latex");
            let apply_min_padding = matches!(format.header_row, RowFormat::Static(_))
                || format.padding == 0
                || format_requires_padding;
            for col_idx in 0..column_count {
                let header_width = cell_width(
                    &formatted_headers[col_idx],
                    supports_multiline,
                    enable_widechars,
                );
                if apply_min_padding {
                    content_widths[col_idx] =
                        max(content_widths[col_idx], header_width + min_padding);
                } else {
                    content_widths[col_idx] = max(content_widths[col_idx], header_width);
                }
            }
        }

        let column_aligns = initialise_alignments(column_count, &column_types, options);
        let header_aligns = initialise_header_alignments(column_count, &column_aligns, options);
        for col_idx in 0..column_count {
            if column_aligns[col_idx] == Alignment::Decimal {
                let mut max_integer_width_value = 0usize;
                let mut max_fraction_width_value = 0usize;
                for row in &formatted_rows {
                    let (integer_width, fraction_width) =
                        max_integer_fraction_width(&row[col_idx], '.', enable_widechars);
                    max_integer_width_value = max(max_integer_width_value, integer_width);
                    max_fraction_width_value = max(max_fraction_width_value, fraction_width);
                }
                let (header_integer, header_fraction) =
                    max_integer_fraction_width(&formatted_headers[col_idx], '.', enable_widechars);
                max_integer_width_value = max(max_integer_width_value, header_integer);
                max_fraction_width_value = max(max_fraction_width_value, header_fraction);
                decimal_widths[col_idx] = Some(DecimalLayout {
                    integer: max_integer_width_value,
                    fraction: max_fraction_width_value,
                });
                let decimal_total = if max_fraction_width_value > 0 {
                    max_integer_width_value + 1 + max_fraction_width_value
                } else {
                    max_integer_width_value
                };
                content_widths[col_idx] = max(content_widths[col_idx], decimal_total);
            }
        }
        let padding = format.padding;
        let column_widths: Vec<usize> = content_widths.iter().map(|w| w + padding * 2).collect();

        #[cfg(test)]
        if format_name_key == "moinmoin" {
            println!("moin widths {:?}", column_widths);
            println!("headers {:?}", formatted_headers);
            println!("rows {:?}", formatted_rows);
        }

        let mut aligned_headers = Vec::with_capacity(column_count);
        for col_idx in 0..column_count {
            let alignment = match header_aligns[col_idx] {
                Alignment::Decimal => Alignment::Right,
                other => other,
            };
            let header_decimal_layout = match header_aligns[col_idx] {
                Alignment::Decimal => None,
                _ => decimal_widths[col_idx],
            };
            let aligned = align_cell(
                &formatted_headers[col_idx],
                content_widths[col_idx],
                alignment,
                '.',
                header_decimal_layout,
                supports_multiline,
                true,
                enable_widechars,
            );
            let padded = apply_padding(&aligned, padding, supports_multiline);
            aligned_headers.push(padded);
        }

        #[cfg(test)]
        if format_name_key == "moinmoin" {
            dbg!(&aligned_headers);
        }

        let mut aligned_rows = vec![Vec::with_capacity(column_count); row_count];
        for col_idx in 0..column_count {
            for row_idx in 0..row_count {
                let cell_text = &formatted_rows[row_idx][col_idx];
                let enforce_left_alignment = supports_multiline
                    || column_aligns[col_idx] != Alignment::Left
                    || !cell_text.contains('\n');
                let aligned = align_cell(
                    cell_text,
                    content_widths[col_idx],
                    column_aligns[col_idx],
                    '.',
                    decimal_widths[col_idx],
                    supports_multiline,
                    enforce_left_alignment,
                    enable_widechars,
                );
                aligned_rows[row_idx].push(apply_padding(&aligned, padding, supports_multiline));
            }
        }

        let header_lines = if has_header {
            render_multiline_row(
                &format.header_row,
                &aligned_headers,
                &column_widths,
                &header_aligns,
                RowAlignment::Top,
                format_name_key,
                supports_multiline,
            )
        } else {
            Vec::new()
        };

        let row_aligns = initialise_row_alignments(row_count, options);

        let mut data_lines = Vec::with_capacity(row_count);
        for (row_idx, row) in aligned_rows.iter().enumerate() {
            let alignment = row_aligns
                .get(row_idx)
                .copied()
                .unwrap_or(RowAlignment::Top);
            data_lines.push(render_multiline_row(
                &format.data_row,
                row,
                &column_widths,
                &column_aligns,
                alignment,
                format_name_key,
                supports_multiline,
            ));
        }

        let hide_line_above = has_header && format.hides("lineabove");
        let hide_line_below_header = has_header && format.hides("linebelowheader");
        let hide_line_between_rows = has_header && format.hides("linebetweenrows");
        let hide_line_below = has_header && format.hides("linebelow");

        let line_above = render_line(
            &format.line_above,
            &column_widths,
            &column_aligns,
            hide_line_above,
        );
        let line_below_header = render_line(
            &format.line_below_header,
            &column_widths,
            &column_aligns,
            hide_line_below_header,
        );
        let line_between_rows = render_line(
            &format.line_between_rows,
            &column_widths,
            &column_aligns,
            hide_line_between_rows,
        );
        let line_below = render_line(
            &format.line_below,
            &column_widths,
            &column_aligns,
            hide_line_below,
        );

        let separator_fallback = if line_between_rows.is_some() {
            None
        } else {
            line_below_header
                .clone()
                .or_else(|| line_below.clone())
                .or_else(|| line_above.clone())
        };

        Self {
            has_header,
            header_lines,
            data_lines,
            sequence,
            line_above,
            line_below_header,
            line_between_rows,
            line_below,
            separator_fallback,
        }
    }

    fn render(self) -> String {
        let RenderPlan {
            has_header,
            header_lines,
            data_lines,
            sequence,
            line_above,
            line_below_header,
            line_between_rows,
            line_below,
            separator_fallback,
        } = self;

        let mut output = Vec::new();
        if let Some(line) = line_above {
            output.push(line);
        }

        if has_header {
            output.extend(header_lines);
            if let Some(line) = line_below_header {
                output.push(line);
            }
        }

        if !sequence.is_empty() {
            for (idx, marker) in sequence.iter().enumerate() {
                match marker {
                    RowMarker::Data(index) => {
                        if let Some(lines) = data_lines.get(*index) {
                            for line in lines {
                                output.push(line.clone());
                            }
                        }
                        if let Some(next_marker) = sequence.get(idx + 1)
                            && matches!(next_marker, RowMarker::Data(_))
                            && let Some(line) = &line_between_rows
                        {
                            output.push(line.clone());
                        }
                    }
                    RowMarker::Separator => {
                        if let Some(line) = &line_between_rows {
                            output.push(line.clone());
                        } else if let Some(line) = &separator_fallback {
                            if !line.is_empty() {
                                output.push(line.clone());
                            } else {
                                output.push(String::new());
                            }
                        } else {
                            output.push(String::new());
                        }
                    }
                }
            }
        }

        if let Some(line) = line_below {
            output.push(line);
        }

        output.join("\n")
    }
}

fn resolve_formats(spec: &FormatSpec, column_count: usize, default: &str) -> Vec<String> {
    match spec {
        FormatSpec::Default => vec![default.to_string(); column_count],
        FormatSpec::Fixed(fmt) => vec![fmt.clone(); column_count],
        FormatSpec::PerColumn(list) => {
            let mut result = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                result.push(
                    list.get(idx)
                        .cloned()
                        .unwrap_or_else(|| default.to_string()),
                );
            }
            result
        }
    }
}

fn resolve_missing_values(spec: &MissingValues, column_count: usize) -> Vec<String> {
    match spec {
        MissingValues::Single(value) => vec![value.clone(); column_count],
        MissingValues::PerColumn(values) => {
            if values.is_empty() {
                return vec![String::new(); column_count];
            }
            let mut result = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                result.push(
                    values
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| values.last().cloned().unwrap()),
                );
            }
            result
        }
    }
}

fn format_cell(
    cell: &CellData,
    column_type: ColumnType,
    float_format: &str,
    int_format: &str,
    missing_value: &str,
) -> String {
    if cell.text.is_empty() {
        return missing_value.to_string();
    }

    match column_type {
        ColumnType::Int => match cell.value {
            ParsedValue::Int(value) => format_int_value(value, int_format),
            _ => cell.text.clone(),
        },
        ColumnType::Float => match cell.value {
            ParsedValue::Float(value) => format_float_value(value, float_format),
            ParsedValue::Int(value) => format_float_value(value as f64, float_format),
            _ => cell.text.clone(),
        },
        ColumnType::Bool => match cell.value {
            ParsedValue::Bool(flag) => format_bool_value(flag),
            _ => cell.text.clone(),
        },
        ColumnType::String => cell.text.clone(),
    }
}

fn format_bool_value(flag: bool) -> String {
    if flag {
        "True".to_string()
    } else {
        "False".to_string()
    }
}

fn format_int_value(value: i128, spec: &str) -> String {
    let mut body = spec.trim().to_string();
    if body.is_empty() {
        return value.to_string();
    }

    let thousands = if body.contains(',') {
        body = body.replace(',', "");
        true
    } else {
        false
    };

    let mut base = 10;
    let mut uppercase = false;
    if let Some(ch) = body.chars().last() {
        match ch {
            'x' => {
                base = 16;
                body.pop();
            }
            'X' => {
                base = 16;
                uppercase = true;
                body.pop();
            }
            'b' | 'B' => {
                base = 2;
                uppercase = ch.is_uppercase();
                body.pop();
            }
            'o' | 'O' => {
                base = 8;
                uppercase = ch.is_uppercase();
                body.pop();
            }
            _ => {}
        }
    }

    let mut formatted = match base {
        16 => {
            if uppercase {
                format!("{:X}", value)
            } else {
                format!("{:x}", value)
            }
        }
        8 => {
            let text = format!("{:o}", value);
            if uppercase { text.to_uppercase() } else { text }
        }
        2 => {
            let sign = if value < 0 { "-" } else { "" };
            let digits = format!("{:b}", value.abs());
            format!("{sign}{digits}")
        }
        _ => value.to_string(),
    };

    if thousands && base == 10 {
        formatted = apply_thousands_to_integer_string(&formatted);
    }

    formatted
}

fn format_float_value(value: f64, spec: &str) -> String {
    let fmt = parse_float_format(spec);

    if value.is_nan() {
        return if fmt.uppercase {
            "NAN".to_string()
        } else {
            "nan".to_string()
        };
    }
    if value.is_infinite() {
        let inf = if value.is_sign_negative() {
            "-inf"
        } else {
            "inf"
        };
        return if fmt.uppercase {
            inf.to_uppercase()
        } else {
            inf.to_string()
        };
    }

    let mut rendered = match fmt.style {
        FloatStyle::Fixed => {
            let precision = fmt.precision.unwrap_or(6);
            if fmt.percent {
                let mut text = format!("{:.*}", precision, value * 100.0);
                text = trim_trailing_zeros(text);
                if text.is_empty() {
                    text = "0".to_string();
                }
                text.push('%');
                if fmt.thousands {
                    apply_thousands_to_number(&text)
                } else {
                    text
                }
            } else {
                format!("{:.*}", precision, value)
            }
        }
        FloatStyle::Exponent => {
            let precision = fmt.precision.unwrap_or(6);
            let formatted = if fmt.uppercase {
                format!("{:.*E}", precision, value)
            } else {
                format!("{:.*e}", precision, value)
            };
            if let Some(pos) = formatted.find('e').or_else(|| formatted.find('E')) {
                let exp_char = formatted.as_bytes()[pos] as char;
                let mantissa = &formatted[..pos];
                let exponent = normalize_exponent(&formatted[pos + 1..]);
                format!("{mantissa}{exp_char}{exponent}")
            } else {
                formatted
            }
        }
        FloatStyle::General => format_float_general(value, fmt.precision, fmt.uppercase),
    };

    if (fmt.style != FloatStyle::Fixed || !fmt.percent) && fmt.thousands {
        rendered = apply_thousands_to_number(&rendered);
    }

    rendered
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FloatStyle {
    General,
    Fixed,
    Exponent,
}

#[derive(Clone, Copy)]
struct ParsedFloatFormat {
    style: FloatStyle,
    precision: Option<usize>,
    thousands: bool,
    uppercase: bool,
    percent: bool,
}

impl Default for ParsedFloatFormat {
    fn default() -> Self {
        Self {
            style: FloatStyle::General,
            precision: None,
            thousands: false,
            uppercase: false,
            percent: false,
        }
    }
}

fn parse_float_format(spec: &str) -> ParsedFloatFormat {
    let mut fmt = ParsedFloatFormat::default();
    if spec.is_empty() {
        return fmt;
    }

    let mut body = spec.trim().to_string();
    if body.contains(',') {
        body = body.replace(',', "");
        fmt.thousands = true;
    }

    if let Some(ch) = body.chars().last() {
        match ch {
            'f' | 'F' => {
                fmt.style = FloatStyle::Fixed;
                fmt.uppercase = ch.is_uppercase();
                body.pop();
            }
            'e' | 'E' => {
                fmt.style = FloatStyle::Exponent;
                fmt.uppercase = ch.is_uppercase();
                body.pop();
            }
            'g' | 'G' => {
                fmt.style = FloatStyle::General;
                fmt.uppercase = ch.is_uppercase();
                body.pop();
            }
            '%' => {
                fmt.style = FloatStyle::Fixed;
                fmt.percent = true;
                body.pop();
            }
            _ => {}
        }
    }

    if let Some(dot) = body.find('.') {
        let precision_part = &body[dot + 1..];
        if !precision_part.is_empty() {
            fmt.precision = precision_part.parse::<usize>().ok();
        }
    }

    fmt
}

fn format_float_general(value: f64, precision: Option<usize>, uppercase: bool) -> String {
    let precision = precision.unwrap_or(6).max(1);
    if value == 0.0 {
        return "0".to_string();
    }

    let abs = value.abs();
    let exponent = if abs == 0.0 {
        0
    } else {
        abs.log10().floor() as i32
    };
    let use_exponent = exponent < -4 || exponent >= precision as i32;

    if use_exponent {
        let formatted = if uppercase {
            format!("{:.*E}", precision - 1, value)
        } else {
            format!("{:.*e}", precision - 1, value)
        };
        if let Some(pos) = formatted.find('e').or_else(|| formatted.find('E')) {
            let exp_char = formatted.as_bytes()[pos] as char;
            let mantissa = trim_trailing_zeros(formatted[..pos].to_string());
            let exponent = normalize_exponent(&formatted[pos + 1..]);
            format!("{mantissa}{exp_char}{exponent}")
        } else {
            formatted
        }
    } else {
        let decimals = (precision as i32 - exponent - 1).max(0) as usize;
        let text = format!("{:.*}", decimals, value);
        trim_trailing_zeros(text)
    }
}

fn trim_trailing_zeros(mut value: String) -> String {
    if let Some(dot) = value.find('.') {
        let mut trim_len = value.len();
        while trim_len > dot && value.as_bytes()[trim_len - 1] == b'0' {
            trim_len -= 1;
        }
        if trim_len > dot && value.as_bytes()[trim_len - 1] == b'.' {
            trim_len -= 1;
        }
        value.truncate(trim_len);
    }
    value
}

fn apply_thousands_to_integer_string(value: &str) -> String {
    let (sign, digits) = if let Some(rest) = value.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = value.strip_prefix('+') {
        ("+", rest)
    } else {
        ("", value)
    };
    let formatted = apply_thousands_to_integer(digits);
    format!("{sign}{formatted}")
}

fn apply_thousands_to_integer(digits: &str) -> String {
    if digits.len() <= 3 {
        return digits.to_string();
    }

    let mut result = String::new();
    let chars: Vec<char> = digits.chars().collect();
    let mut index = chars.len() % 3;
    if index == 0 {
        index = 3;
    }
    result.extend(&chars[..index]);
    while index < chars.len() {
        result.push(',');
        result.extend(&chars[index..index + 3]);
        index += 3;
    }
    result
}

fn apply_thousands_to_number(text: &str) -> String {
    if text.contains('e') || text.contains('E') {
        return text.to_string();
    }
    let (body, suffix) = if let Some(stripped) = text.strip_suffix('%') {
        (stripped, "%")
    } else {
        (text, "")
    };
    let (sign, rest) = if let Some(stripped) = body.strip_prefix('-') {
        ("-", stripped)
    } else if let Some(stripped) = body.strip_prefix('+') {
        ("+", stripped)
    } else {
        ("", body)
    };
    let mut parts = rest.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    let formatted_int = apply_thousands_to_integer(int_part);
    let mut result = String::new();
    result.push_str(sign);
    result.push_str(&formatted_int);
    if let Some(frac) = frac_part
        && !frac.is_empty()
    {
        result.push('.');
        result.push_str(frac);
    }
    result.push_str(suffix);
    result
}

fn normalize_numeric_candidate(input: &str) -> String {
    // If the input contains ", " (comma followed by space), it's likely a list of values
    // rather than a single number with thousands separators. Don't normalize it.
    if input.contains(", ") {
        return input.to_string();
    }

    // Otherwise, strip separators (commas, underscores, spaces) for number parsing
    input
        .chars()
        .filter(|ch| *ch != ',' && *ch != '_' && *ch != ' ')
        .collect()
}

fn normalize_exponent(exponent: &str) -> String {
    let (sign, digits) = if exponent.starts_with(['+', '-']) {
        exponent.split_at(1)
    } else {
        ("+", exponent)
    };
    let trimmed = digits.trim_start_matches('0');
    let core = if trimmed.is_empty() { "0" } else { trimmed };
    let padded = if core.len() < 2 {
        format!("{:0>2}", core)
    } else {
        core.to_string()
    };
    format!("{sign}{padded}")
}

fn initialise_alignments(
    column_count: usize,
    column_types: &[ColumnType],
    options: &TabulateOptions,
) -> Vec<Alignment> {
    let mut aligns = vec![Alignment::Left; column_count];
    let num_align = options.num_align.unwrap_or(Alignment::Decimal);
    let str_align = options.str_align.unwrap_or(Alignment::Left);

    for (idx, col_type) in column_types.iter().enumerate() {
        aligns[idx] = match col_type {
            ColumnType::Int | ColumnType::Float => num_align,
            _ => str_align,
        };
    }

    if let Some(global) = options.col_global_align {
        aligns.iter_mut().for_each(|align| *align = global);
    }

    for (idx, custom) in options.col_align.iter().enumerate() {
        if let Some(alignment) = custom
            && idx < aligns.len()
        {
            aligns[idx] = *alignment;
        }
    }

    aligns
}

fn initialise_header_alignments(
    column_count: usize,
    column_aligns: &[Alignment],
    options: &TabulateOptions,
) -> Vec<Alignment> {
    let mut aligns = if let Some(global) = options.headers_global_align {
        vec![global; column_count]
    } else {
        column_aligns.to_vec()
    };

    for (idx, custom) in options.headers_align.iter().enumerate() {
        if idx >= aligns.len() {
            break;
        }
        if let Some(spec) = custom {
            match spec {
                HeaderAlignment::Align(alignment) => {
                    aligns[idx] = *alignment;
                }
                HeaderAlignment::SameAsColumn => {
                    if let Some(column_alignment) = column_aligns.get(idx) {
                        aligns[idx] = *column_alignment;
                    }
                }
            }
        }
    }

    aligns
}

fn initialise_row_alignments(row_count: usize, options: &TabulateOptions) -> Vec<RowAlignment> {
    let default = options.row_global_align.unwrap_or(RowAlignment::Top);
    let mut aligns = vec![default; row_count];
    for (idx, custom) in options.row_align.iter().enumerate() {
        if idx >= aligns.len() {
            break;
        }
        if let Some(alignment) = custom {
            aligns[idx] = *alignment;
        }
    }
    aligns
}

fn apply_padding(text: &str, padding: usize, supports_multiline: bool) -> String {
    if padding == 0 {
        return text.to_string();
    }
    let pad = " ".repeat(padding);
    if supports_multiline {
        split_cell_lines(text)
            .into_iter()
            .map(|line| format!("{pad}{line}{pad}"))
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        format!("{pad}{text}{pad}")
    }
}

fn split_cell_lines(text: &str) -> Vec<&str> {
    text.split('\n').collect()
}

fn cell_width(text: &str, supports_multiline: bool, enable_widechars: bool) -> usize {
    if supports_multiline {
        max_line_width(text, enable_widechars)
    } else {
        full_text_width(text, enable_widechars)
    }
}

fn max_line_width(text: &str, enable_widechars: bool) -> usize {
    split_cell_lines(text)
        .into_iter()
        .map(|line| visible_width(line, enable_widechars))
        .max()
        .unwrap_or(0)
}

fn full_text_width(text: &str, enable_widechars: bool) -> usize {
    let lines = split_cell_lines(text);
    if lines.is_empty() {
        return 0;
    }
    let mut total = 0usize;
    let last_index = lines.len().saturating_sub(1);
    for (idx, line) in lines.iter().enumerate() {
        total += visible_width(line, enable_widechars);
        if idx < last_index {
            total += 1; // account for the newline separating the lines
        }
    }
    total
}

fn max_integer_fraction_width(
    text: &str,
    decimal_marker: char,
    enable_widechars: bool,
) -> (usize, usize) {
    split_cell_lines(text)
        .into_iter()
        .map(|line| integer_fraction_width_line(line, decimal_marker, enable_widechars))
        .fold((0, 0), |(max_int, max_frac), (int_w, frac_w)| {
            (max(max_int, int_w), max(max_frac, frac_w))
        })
}

fn integer_fraction_width_line(
    value: &str,
    decimal_marker: char,
    enable_widechars: bool,
) -> (usize, usize) {
    match value.find(decimal_marker) {
        Some(pos) => {
            let integer_width = visible_width(&value[..pos], enable_widechars);
            let fraction_width =
                visible_width(&value[pos + decimal_marker.len_utf8()..], enable_widechars);
            (integer_width, fraction_width)
        }
        None => (visible_width(value, enable_widechars), 0),
    }
}

fn render_line(
    spec: &LineFormat,
    col_widths: &[usize],
    col_aligns: &[Alignment],
    suppressed: bool,
) -> Option<String> {
    if suppressed {
        return None;
    }
    match spec {
        LineFormat::None => None,
        LineFormat::Static(line) => {
            let mut out = String::new();
            out.push_str(line.begin.as_ref());
            for (idx, width) in col_widths.iter().enumerate() {
                if idx > 0 {
                    out.push_str(line.separator.as_ref());
                }
                out.push_str(&line.fill.as_ref().repeat(*width));
            }
            out.push_str(line.end.as_ref());
            if out.is_empty() { None } else { Some(out) }
        }
        LineFormat::Text(text) => {
            let owned = text.clone().into_owned();
            if owned.is_empty() { None } else { Some(owned) }
        }
        LineFormat::Dynamic(func) => {
            let rendered = func(col_widths, col_aligns);
            if rendered.is_empty() {
                None
            } else {
                Some(rendered)
            }
        }
    }
}

fn render_multiline_row(
    spec: &RowFormat,
    cells: &[String],
    col_widths: &[usize],
    col_aligns: &[Alignment],
    row_alignment: RowAlignment,
    format_name: &str,
    supports_multiline: bool,
) -> Vec<String> {
    match spec {
        RowFormat::None => Vec::new(),
        RowFormat::Dynamic(func) => vec![func(cells, col_widths, col_aligns)],
        RowFormat::Static(_) => {
            let split_cells: Vec<Vec<String>> = cells
                .iter()
                .map(|cell| {
                    split_cell_lines(cell)
                        .into_iter()
                        .map(|line| line.to_string())
                        .collect()
                })
                .collect();
            let line_count = split_cells
                .iter()
                .map(|lines| lines.len())
                .max()
                .unwrap_or(0);
            if line_count <= 1 || !supports_multiline {
                return vec![render_row(spec, cells, col_widths, col_aligns)];
            }
            let mut per_column_lines: Vec<Vec<String>> = Vec::with_capacity(cells.len());
            for (col_idx, lines) in split_cells.iter().enumerate() {
                let width = *col_widths.get(col_idx).unwrap_or(&0);
                per_column_lines.push(pad_cell_lines(
                    lines,
                    width,
                    line_count,
                    row_alignment,
                    format_name,
                ));
            }
            let mut output = Vec::with_capacity(line_count);
            for line_idx in 0..line_count {
                let line_cells = per_column_lines
                    .iter()
                    .map(|column_lines| column_lines[line_idx].clone())
                    .collect::<Vec<_>>();
                output.push(render_row(spec, &line_cells, col_widths, col_aligns));
            }
            output
        }
    }
}

fn pad_cell_lines(
    lines: &[String],
    width: usize,
    total_lines: usize,
    alignment: RowAlignment,
    format_name: &str,
) -> Vec<String> {
    let mut result = Vec::with_capacity(total_lines);
    let blank = " ".repeat(width);
    let line_count = lines.len();
    if line_count >= total_lines {
        result.extend_from_slice(&lines[..total_lines]);
        return result;
    }
    let padding = total_lines - line_count;
    match alignment {
        RowAlignment::Top => {
            result.extend_from_slice(lines);
            result.extend((0..padding).map(|_| blank.clone()));
        }
        RowAlignment::Bottom => {
            let treat_as_top = lines.len() <= 1 && format_name.starts_with("pipe");
            if treat_as_top {
                result.extend_from_slice(lines);
                result.extend((0..padding).map(|_| blank.clone()));
            } else {
                result.extend((0..padding).map(|_| blank.clone()));
                result.extend_from_slice(lines);
            }
        }
        RowAlignment::Center => {
            let top = padding / 2;
            let bottom = padding - top;
            result.extend((0..top).map(|_| blank.clone()));
            result.extend_from_slice(lines);
            result.extend((0..bottom).map(|_| blank.clone()));
        }
    }
    result
}

fn render_row(
    spec: &RowFormat,
    cells: &[String],
    col_widths: &[usize],
    col_aligns: &[Alignment],
) -> String {
    match spec {
        RowFormat::None => String::new(),
        RowFormat::Static(row) => {
            let mut out = String::new();
            out.push_str(row.begin.as_ref());
            for (idx, cell) in cells.iter().enumerate() {
                if idx > 0 {
                    out.push_str(row.separator.as_ref());
                }
                out.push_str(cell);
            }
            out.push_str(row.end.as_ref());
            out.trim_end().to_string()
        }
        RowFormat::Dynamic(func) => func(cells, col_widths, col_aligns),
    }
}
