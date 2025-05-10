#[derive(Default, Debug)]
pub struct WorkloadStats {
    pub total_lines: usize,
    pub put_success: usize,
    pub put_fail: usize,
    pub get_success: usize,
    pub get_fail: usize,
    pub delete_success: usize,
    pub delete_fail: usize,
    pub range_success: usize,
    pub range_fail: usize,
    pub parse_errors: usize,
    pub unknown_commands: usize,
}
