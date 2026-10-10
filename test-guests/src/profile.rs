#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Debug,
    ReleaseSplit,
}

impl Profile {
    pub const ALL: [Profile; 2] = [Profile::Debug, Profile::ReleaseSplit];

    pub fn name(self) -> &'static str {
        match self {
            Profile::Debug => "debug",
            Profile::ReleaseSplit => "release-split",
        }
    }
}
