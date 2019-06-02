## pmdk

Rust wrapper for pmdk

## Checklist for publishing repository on crates.io

1. publish only from master branch
2. the last commit is version bump that updates version number in TWO files
   Cargo.toml and src/lib.rs
3. cargo fmt -- --check is clean
4. cargo clippy is clean
5. cargo test is clean
6. version tagged with annotated tag that equals version number
   `git tag -a -m 0.x.y 0.x.y`
7. cargo publish
