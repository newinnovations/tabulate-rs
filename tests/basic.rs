use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Value, json};
use tabulate_rs::{
    Alignment, FormatSpec, Headers, RowAlignment, SEPARATING_LINE, ShowIndex, TabulateError,
    TabulateOptions, simple_separated_format, tabulate,
};

fn planet_data() -> Vec<Vec<&'static str>> {
    vec![
        vec!["Sun", "696000", "1989100000"],
        vec!["Earth", "6371", "5973.6"],
        vec!["Moon", "1737", "73.5"],
        vec!["Mars", "3390", "641.85"],
    ]
}

#[test]
fn simple_table_default() {
    let data = planet_data();
    let options = TabulateOptions::new();
    let output = tabulate(data, options).expect("tabulation succeed");

    let expected = "-----  ------  -------------\nSun    696000     1.9891e+09\nEarth    6371  5973.6\nMoon     1737    73.5\nMars     3390   641.85\n-----  ------  -------------";
    assert_eq!(output, expected);
}

#[test]
fn simple_table_with_index() {
    let data = planet_data();
    let options = TabulateOptions::new().show_index(ShowIndex::Always);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "-  -----  ------  -------------\n0  Sun    696000     1.9891e+09\n1  Earth    6371  5973.6\n2  Moon     1737    73.5\n3  Mars     3390   641.85\n-  -----  ------  -------------";
    assert_eq!(output, expected);
}

#[test]
fn custom_table_format_simple_separator() {
    let data = vec![vec![1, 2], vec![3, 4]];
    let options = TabulateOptions::new().table_format_custom(simple_separated_format("!"));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "1!2\n3!4";
    assert_eq!(output, expected);
}

#[test]
fn custom_table_format_with_headers() {
    let data = vec![
        vec!["A".to_string(), "B".to_string()],
        vec!["1".to_string(), "2".to_string()],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format_custom(simple_separated_format("|"));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "  A|  B\n  1|  2";
    assert_eq!(output, expected);
}

#[test]
fn headers_taken_from_first_data_row() {
    let data = vec![
        vec!["Planet", "Radius", "Mass"],
        vec!["Earth", "6371", "5973.6"],
        vec!["Mars", "3390", "641.85"],
    ];
    let options = TabulateOptions::new().headers(Headers::FirstRow);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Planet      Radius     Mass\n--------  --------  -------\nEarth         6371  5973.6\nMars          3390   641.85";
    assert_eq!(output, expected);
}

#[test]
fn headers_from_dictionary_keys() {
    let mut earth = BTreeMap::new();
    earth.insert("planet", "Earth");
    earth.insert("radius", "6371");

    let mut mars = BTreeMap::new();
    mars.insert("planet", "Mars");
    mars.insert("radius", "3390");

    let data = vec![earth, mars];
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "planet      radius\nEarth         6371\nMars          3390";
    assert_eq!(output, expected);
}

#[test]
fn headers_mapping_for_dictionary_rows() {
    let mut earth = BTreeMap::new();
    earth.insert("planet", "Earth");
    earth.insert("radius", "6371");

    let mut mars = BTreeMap::new();
    mars.insert("planet", "Mars");
    mars.insert("radius", "3390");

    let data = vec![earth, mars];
    let options = TabulateOptions::new()
        .headers_mapping([("planet", "Planet"), ("radius", "Radius")])
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Planet      Radius\nEarth         6371\nMars          3390";
    assert_eq!(output, expected);
}

#[test]
fn headers_mapping_for_dictionary_rows_includes_missing_keys() {
    let mut earth = BTreeMap::new();
    earth.insert("planet", "Earth");
    earth.insert("radius", "6371");

    let mut mars = BTreeMap::new();
    mars.insert("planet", "Mars");
    mars.insert("radius", "3390");

    let data = vec![earth, mars];
    let options = TabulateOptions::new().headers_mapping([
        ("radius", "Radius"),
        ("bonus", "Bonus"),
        ("planet", "Planet"),
    ]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "  Radius  Bonus    Planet\n--------  -------  --------\n    6371           Earth\n    3390           Mars";
    assert_eq!(output, expected);
}

#[test]
fn headers_firstrow_for_dictionary_rows() {
    let mut head = BTreeMap::new();
    head.insert("planet", "Planet");
    head.insert("radius", "Radius");

    let mut earth = BTreeMap::new();
    earth.insert("planet", "Earth");
    earth.insert("radius", "6371");

    let mut mars = BTreeMap::new();
    mars.insert("planet", "Mars");
    mars.insert("radius", "3390");

    let data = vec![head, earth, mars];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Planet      Radius\nEarth         6371\nMars          3390";
    assert_eq!(output, expected);
}

#[test]
fn custom_show_index_values() {
    let data = planet_data();
    let options = TabulateOptions::new().show_index(ShowIndex::Values(vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
        "D".to_string(),
    ]));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "-  -----  ------  -------------\nA  Sun    696000     1.9891e+09\nB  Earth    6371  5973.6\nC  Moon     1737    73.5\nD  Mars     3390   641.85\n-  -----  ------  -------------";
    assert_eq!(output, expected);
}

#[test]
fn separating_line_in_plain_format() {
    let mut data = planet_data();
    data.insert(1, vec![SEPARATING_LINE]);

    let options = TabulateOptions::new().table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Sun    696000     1.9891e+09\n\nEarth    6371  5973.6\nMoon     1737    73.5\nMars     3390   641.85";
    assert_eq!(output, expected);
}

#[test]
fn float_format_fixed_precision() {
    let data = vec![vec![json!("1.23456789")], vec![json!(1.0)]];
    let options = TabulateOptions::new()
        .table_format("plain")
        .float_format(FormatSpec::Fixed(".3f".into()));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "1.235\n1.000";
    assert_eq!(output, expected);
}

#[test]
fn float_format_with_thousands() {
    let data = vec![
        vec![json!("1.23456789")],
        vec![json!(1.0)],
        vec![json!("1,234.56")],
    ];
    let options = TabulateOptions::new()
        .table_format("plain")
        .float_format(FormatSpec::Fixed(".3f".into()));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "   1.235\n   1.000\n1234.560";
    assert_eq!(output, expected);
}

#[test]
fn float_format_per_column() {
    let data = vec![vec![json!(0.12345), json!(0.12345), json!(0.12345)]];
    let options = TabulateOptions::new()
        .table_format("plain")
        .float_format(FormatSpec::PerColumn(vec![".1f".into(), ".3f".into()]));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "0.1  0.123  0.12345";
    assert_eq!(output, expected);
}

#[test]
fn missing_values_single_placeholder() {
    let data = vec![
        vec![json!("Alice"), json!(10)],
        vec![json!("Bob"), Value::Null],
    ];
    let options = TabulateOptions::new()
        .table_format("plain")
        .missing_value("n/a");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Alice   10\nBob    n/a";
    assert_eq!(output, expected);
}

#[test]
fn missing_values_per_column() {
    let data = vec![
        vec![json!("Alice"), json!("Bob"), json!("Charlie")],
        vec![Value::Null, Value::Null, Value::Null],
    ];
    let options = TabulateOptions::new()
        .table_format("plain")
        .missing_values(vec![String::from("n/a"), String::from("?")]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Alice  Bob  Charlie\nn/a    ?    ?";
    assert_eq!(output, expected);
}

#[test]
fn wrapping_respects_max_col_widths() {
    let data = vec![
        vec!["Planet", "Description"],
        vec!["Mercury", "Nearest planet to the Sun"],
        vec!["Venus", "Has a thick atmosphere"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .max_col_widths(vec![None, Some(10)]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "+----------+---------------+\n",
        "| Planet   | Description   |\n",
        "+==========+===============+\n",
        "| Mercury  | Nearest       |\n",
        "|          | planet to     |\n",
        "|          | the Sun       |\n",
        "+----------+---------------+\n",
        "| Venus    | Has a         |\n",
        "|          | thick         |\n",
        "|          | atmosphere    |\n",
        "+----------+---------------+"
    );
    assert_eq!(output, expected);
}

#[test]
fn empty_data_returns_empty_string() {
    let data: Vec<Vec<String>> = Vec::new();
    let output = tabulate(data, TabulateOptions::new()).expect("empty data produces table");
    assert!(output.is_empty());
}

#[test]
fn empty_data_with_headers_uses_header_row() {
    let data: Vec<Vec<String>> = Vec::new();
    let options =
        TabulateOptions::new().headers(Headers::Explicit(vec!["A".to_string(), "B".to_string()]));
    let output = tabulate(data, options).expect("empty data with headers succeeds");
    let expected = "A    B\n---  ---";
    assert_eq!(output, expected);
}

#[test]
fn empty_data_with_showindex_stays_empty() {
    let data: Vec<Vec<String>> = Vec::new();
    let options = TabulateOptions::new().show_index(ShowIndex::Always);
    let output = tabulate(data, options).expect("empty data with index succeeds");
    assert!(output.is_empty());
}

#[test]
fn explicit_headers_align_to_last_columns() {
    let data = planet_data();
    let options = TabulateOptions::new().headers(Headers::Explicit(vec![String::from("Name")]));
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "                        Name\n-----  ------  -------------\nSun    696000     1.9891e+09\nEarth    6371  5973.6\nMoon     1737    73.5\nMars     3390   641.85";
    assert_eq!(output, expected);
}

#[test]
fn row_alignment_bottom_for_multiline_rows() {
    let data = vec![
        vec!["Name", "Description"],
        vec!["Mercury", "nearest\nplanet"],
        vec!["Venus", "second\nplanet"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .row_global_align(RowAlignment::Bottom);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "+---------+---------------+\n",
        "| Name    | Description   |\n",
        "+=========+===============+\n",
        "|         | nearest       |\n",
        "| Mercury | planet        |\n",
        "+---------+---------------+\n",
        "|         | second        |\n",
        "| Venus   | planet        |\n",
        "+---------+---------------+"
    );
    assert_eq!(output, expected);
}

#[test]
fn row_alignment_mixed_per_row() {
    let data = vec![
        vec!["Name", "Description"],
        vec!["Mercury", "nearest\nplanet"],
        vec!["Venus", "second\nplanet"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .row_global_align(RowAlignment::Top)
        .row_alignments([Some(RowAlignment::Top), Some(RowAlignment::Bottom)]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "+---------+---------------+\n",
        "| Name    | Description   |\n",
        "+=========+===============+\n",
        "| Mercury | nearest       |\n",
        "|         | planet        |\n",
        "+---------+---------------+\n",
        "|         | second        |\n",
        "| Venus   | planet        |\n",
        "+---------+---------------+"
    );
    assert_eq!(output, expected);
}

#[test]
fn col_alignment_override_for_numeric_columns() {
    let data = vec![
        vec!["Name", "Score"],
        vec!["Alice", "10"],
        vec!["Bob", "1000"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("plain")
        .col_alignments([Some(Alignment::Left), Some(Alignment::Left)]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Name    Score\nAlice   10\nBob     1000";
    assert_eq!(output, expected);
}

#[test]
fn pipe_format_uses_alignment_markers() {
    let data = vec![
        vec!["Name", "Score"],
        vec!["Alice", "10"],
        vec!["Bob", "1000"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("pipe")
        .col_alignments([Some(Alignment::Left), Some(Alignment::Right)]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected =
        "| Name   |   Score |\n|:-------|--------:|\n| Alice  |      10 |\n| Bob    |    1000 |";
    assert_eq!(output, expected);
}

#[test]
fn dictionary_of_iterables_transposed_into_rows() {
    let mut columns = BTreeMap::new();
    columns.insert("planet", vec!["Mercury", "Venus"]);
    columns.insert("radius", vec!["2440", "6052"]);

    let data = vec![columns];
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "planet      radius\nMercury       2440\nVenus         6052";
    assert_eq!(output, expected);
}

#[derive(Serialize)]
struct Person<'a> {
    name: &'a str,
    age: u8,
}

#[derive(Serialize)]
struct DataFrameFixture {
    #[serde(rename = "__tabulate_dataframe__")]
    marker: bool,
    columns: Vec<String>,
    data: Vec<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_label: Option<String>,
}

impl DataFrameFixture {
    fn new<T: Into<String>>(
        columns: Vec<T>,
        data: Vec<Vec<Value>>,
        index: Option<Vec<Value>>,
        index_label: Option<T>,
    ) -> Self {
        DataFrameFixture {
            marker: true,
            columns: columns.into_iter().map(Into::into).collect(),
            data,
            index,
            index_label: index_label.map(Into::into),
        }
    }
}

#[test]
fn struct_rows_use_field_names_with_keys_headers() {
    let data = vec![
        Person {
            name: "Alice",
            age: 30,
        },
        Person {
            name: "Bob",
            age: 25,
        },
    ];
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!("name      age\n", "Alice      30\n", "Bob        25");
    assert_eq!(output, expected);
}

#[test]
fn struct_rows_respect_headers_mapping_order() {
    let data = vec![
        Person {
            name: "Alice",
            age: 30,
        },
        Person {
            name: "Bob",
            age: 25,
        },
    ];
    let options = TabulateOptions::new().headers_mapping([
        ("age", "Age"),
        ("nickname", "Nickname"),
        ("name", "Name"),
    ]);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "  Age  Nickname    Name\n-----  ----------  ------\n   30              Alice\n   25              Bob";
    assert_eq!(output, expected);
}

#[test]
fn dataframe_default_showindex_with_label() {
    let df = DataFrameFixture::new(
        vec!["planet", "radius"],
        vec![
            vec![json!("Mercury"), json!(2440)],
            vec![json!("Venus"), json!(6052)],
        ],
        Some(vec![json!("a"), json!("b")]),
        Some("idx"),
    );
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .table_format("plain");
    let output = tabulate(vec![df], options).expect("tabulation succeed");
    let expected =
        "idx    planet      radius\na      Mercury       2440\nb      Venus         6052";
    assert_eq!(output, expected);
}

#[test]
fn dataframe_showindex_never() {
    let df = DataFrameFixture::new(
        vec!["planet", "radius"],
        vec![
            vec![json!("Mercury"), json!(2440)],
            vec![json!("Venus"), json!(6052)],
        ],
        Some(vec![json!(0), json!(1)]),
        Some("idx"),
    );
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .table_format("plain")
        .show_index(ShowIndex::Never);
    let output = tabulate(vec![df], options).expect("tabulation succeed");
    let expected = "planet      radius\nMercury       2440\nVenus         6052";
    assert_eq!(output, expected);
}

#[test]
fn columnar_dictionary_supports_custom_index_values() {
    let mut columns = BTreeMap::new();
    columns.insert("planet".to_string(), vec!["Mercury", "Venus"]);
    columns.insert("radius".to_string(), vec!["2440", "6052"]);

    let data = vec![columns];
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .show_index(ShowIndex::Values(vec!["I".to_string(), "II".to_string()]))
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "    planet      radius\n",
        "I   Mercury       2440\n",
        "II  Venus         6052"
    );
    assert_eq!(output, expected);
}

#[test]
fn columnar_dictionary_rejects_mismatched_index_values() {
    let mut columns = BTreeMap::new();
    columns.insert("planet".to_string(), vec!["Mercury", "Venus"]);
    columns.insert("radius".to_string(), vec!["2440", "6052"]);

    let data = vec![columns];
    let options = TabulateOptions::new()
        .headers(Headers::Keys)
        .show_index(ShowIndex::Values(vec!["I".to_string()]));
    let error = tabulate(data, options).expect_err("index length mismatch should error");
    assert!(matches!(error, TabulateError::IndexLengthMismatch { .. }));
}

#[test]
fn explicit_headers_for_dictionary_rows_error() {
    let mut first = BTreeMap::new();
    first.insert("name".to_string(), json!("Alice"));
    first.insert("age".to_string(), json!(30));

    let mut second = BTreeMap::new();
    second.insert("name".to_string(), json!("Bob"));
    second.insert("age".to_string(), json!(25));

    let data = vec![first, second];
    let options = TabulateOptions::new().headers(Headers::Explicit(vec![
        "Name".to_string(),
        "Age".to_string(),
    ]));
    let error = tabulate(data, options).expect_err("dict rows require mapping headers");
    assert!(matches!(error, TabulateError::InvalidHeadersForObjects));
}

#[test]
fn explicit_headers_for_columnar_dictionary() {
    let mut columns = BTreeMap::new();
    columns.insert("planet".to_string(), vec!["Mercury", "Venus"]);
    columns.insert("radius".to_string(), vec!["2440", "6052"]);

    let data = vec![columns];
    let options = TabulateOptions::new()
        .headers(Headers::Explicit(vec![
            "Planet".to_string(),
            "Radius".to_string(),
        ]))
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Planet      Radius\nMercury       2440\nVenus         6052";
    assert_eq!(output, expected);
}

#[test]
fn columnar_dictionary_respects_headers_mapping_order() {
    let mut columns = BTreeMap::new();
    columns.insert("planet".to_string(), vec!["Mercury", "Venus"]);
    columns.insert("radius".to_string(), vec!["2440", "6052"]);

    let data = vec![columns];
    let options = TabulateOptions::new()
        .headers_mapping([
            ("radius", "Radius"),
            ("bonus", "Bonus"),
            ("planet", "Planet"),
        ])
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected =
        "  Radius  Bonus    Planet\n    2440           Mercury\n    6052           Venus";
    assert_eq!(output, expected);
}

#[test]
fn headers_global_alignment_center() {
    let data = vec![
        vec!["Name", "Score"],
        vec!["Alice", "10"],
        vec!["Bob", "1000"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .headers_global_align(Alignment::Center);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "+--------+---------+\n",
        "|  Name  |  Score  |\n",
        "+========+=========+\n",
        "| Alice  |      10 |\n",
        "+--------+---------+\n",
        "| Bob    |    1000 |\n",
        "+--------+---------+"
    );
    assert_eq!(output, expected);
}

#[test]
fn col_global_alignment_right() {
    let data = vec![vec!["Alice", "10"], vec!["Bob", "1000"]];
    let options = TabulateOptions::new()
        .table_format("plain")
        .col_global_align(Alignment::Right);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Alice    10\n  Bob  1000";
    assert_eq!(output, expected);
}

#[test]
fn preserve_whitespace_option() {
    let data = vec![
        vec!["Name", "Value"],
        vec!["  Alice", " 10"],
        vec!["Bob  ", "5"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("plain")
        .preserve_whitespace(true);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Name       Value\n  Alice       10\nBob            5";
    assert_eq!(output, expected);
}

#[test]
fn ansi_sequences_do_not_inflate_width() {
    let data = vec![
        vec!["Name", "Value"],
        vec!["\u{1b}[31mRed\u{1b}[0m", "10"],
        vec!["Plain", "5"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("plain");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = "Name      Value\n\u{1b}[31mRed\u{1b}[0m          10\nPlain         5";
    assert_eq!(output, expected);
}

#[test]
fn wide_characters_take_full_width() {
    let data = vec![
        vec!["Name", "Note"],
        vec!["寿司", "おいしい"],
        vec!["カレー", "辛い"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid");
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "+--------+--------+\n",
        "| Name   | Note   |\n",
        "+========+========+\n",
        "| 寿司     | おいしい   |\n",
        "+--------+--------+\n",
        "| カレー    | 辛い     |\n",
        "+--------+--------+"
    );
    assert_eq!(output, expected);
}

#[test]
fn wide_characters_with_widechars_option() {
    let data = vec![
        vec!["Name", "Note"],
        vec!["寿司", "おいしい"],
        vec!["カレー", "辛い"],
    ];
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .enable_widechars(true);
    let output = tabulate(data, options).expect("tabulation succeed");
    let expected = concat!(
        "+--------+----------+\n",
        "| Name   | Note     |\n",
        "+========+==========+\n",
        "| 寿司   | おいしい |\n",
        "+--------+----------+\n",
        "| カレー | 辛い     |\n",
        "+--------+----------+"
    );
    assert_eq!(output, expected);
}
#[test]
fn outline_formats_support_multiline_wrapping() {
    // Test that outline formats properly handle multiline cells with max_col_widths
    let data = vec![
        vec!["Column A", "Column B", "Long Column"],
        vec![
            "Row 1",
            "Data B1",
            "This is a very long text that should wrap into multiple lines when max_col_widths is applied",
        ],
        vec!["Row 2", "Data B2", "Short text"],
    ];

    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("rounded_outline")
        .max_col_widths(vec![None, None, Some(30)]);

    let output = tabulate(data, options).expect("tabulation succeed");

    // Verify that the output contains multiple lines for the wrapped row
    // and that all columns extend properly
    let lines: Vec<&str> = output.lines().collect();

    // Should have more than 5 lines (header separator, wrapped row takes multiple lines, etc.)
    assert!(lines.len() > 5, "Output should have multiple lines for wrapped content");

    // Check that row with long text has proper structure across multiple lines
    // by verifying that we have vertical bars (│) at consistent positions
    let row_lines: Vec<&str> = lines.iter()
        .skip(3) // Skip top border, header, and header separator
        .take(4) // Take the wrapped row lines
        .copied()
        .collect();

    // All lines of the wrapped row should start and end with │
    for line in &row_lines {
        if !line.starts_with('╰') && !line.starts_with('╭') {
            assert!(line.starts_with('│'), "Line should start with │: {}", line);
            assert!(line.ends_with('│'), "Line should end with │: {}", line);
        }
    }
}

#[test]
fn all_outline_formats_support_multiline() {
    // Verify all outline formats handle multiline cells correctly
    let formats = vec![
        "simple_outline",
        "rounded_outline",
        "heavy_outline",
        "mixed_outline",
        "double_outline",
        "fancy_outline",
    ];

    let data = vec![
        vec!["A", "B"],
        vec!["X", "Line 1\nLine 2\nLine 3"],
    ];

    for format in formats {
        let options = TabulateOptions::new()
            .headers(Headers::FirstRow)
            .table_format(format);

        let output = tabulate(data.clone(), options)
            .unwrap_or_else(|_| panic!("Format {} should work", format));

        // The output should have multiple lines (more than 4: top, header, sep, bottom)
        let line_count = output.lines().count();
        assert!(
            line_count > 4,
            "Format {} should expand multiline cells (got {} lines)",
            format,
            line_count
        );
    }
}

#[test]
fn comma_separated_values_preserve_spacing() {
    let data = vec![
        vec!["Numbers", "Description"],
        vec!["1, 2, 3, 4, 5, 6, 7, 8, 9, 10", "Test"],
    ];

    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .max_col_widths(vec![Some(15), None]);

    let output = tabulate(data, options).expect("tabulation succeed");
    println!("Output:\n{}", output);

    // The numbers should still contain commas and spaces
    assert!(output.contains(","), "Output should contain commas");
    assert!(!output.contains("12345678910"), "Numbers should not be concatenated");
}

#[test]
fn thousands_separators_still_parse_as_numbers() {
    // Verify that legitimate thousands separators still work for parsing
    let data = vec![
        vec!["Population", "GDP"],
        vec!["1,000", "5,000,000"],
        vec!["2000", "10000000"],
    ];

    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("plain");

    let output = tabulate(data, options).expect("tabulation succeed");
    println!("Output:\n{}", output);

    // Verify the numbers are right-aligned (indicating they were parsed as numbers)
    // "1,000" should parse as 1000 and "5,000,000" should parse as 5000000
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() >= 3, "Should have header and data rows");

    // Check that numbers are right-aligned by looking for leading spaces
    let row1 = lines[1];
    assert!(row1.trim_start() != row1, "First data row should have leading spaces (right-aligned)");
    assert!(row1.contains("1000") || row1.contains("1,000"), "Should contain parsed number");
}
