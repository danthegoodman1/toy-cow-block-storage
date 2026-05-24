#[derive(Debug, Default)]
pub struct StorageState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageCommand {
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEffect {
    Noop,
}

impl StorageState {
    pub fn step(&mut self, command: StorageCommand) -> Vec<StorageEffect> {
        match command {
            StorageCommand::Noop => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_step_is_deterministic_and_side_effect_free() {
        let mut first = StorageState;
        let mut second = StorageState;

        assert_eq!(
            first.step(StorageCommand::Noop),
            Vec::<StorageEffect>::new()
        );
        assert_eq!(
            second.step(StorageCommand::Noop),
            Vec::<StorageEffect>::new()
        );
    }
}
