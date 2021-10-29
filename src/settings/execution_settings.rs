use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionSettings;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_serde_execution_settings() {
        let settings = ExecutionSettings;
        assert_tokens(&settings, &vec![Token::UnitStruct { name: "ExecutionSettings" }]);
    }
}
