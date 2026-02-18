use tabulate_rs::{Headers, TabulateOptions, tabulate};

fn main() {
    let data = vec![
        vec!["Numbers", "Description"],
        vec!["1, 2, 3, 4, 5, 6, 7, 8, 9, 10", "Test"],
    ];

    // Test with wrapping
    let options = TabulateOptions::new()
        .headers(Headers::FirstRow)
        .table_format("grid")
        .max_col_widths(vec![Some(15), None]);

    let output = tabulate(data, options).expect("tabulation succeed");
    println!("{}", output);
}
