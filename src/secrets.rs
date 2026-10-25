//! Credential values in a target's `vars`: a literal, or a reference `env:NAME` / `file:/path`.

use anyhow::{anyhow, bail, Context, Result};

/// Whether `v` is a reference rather than a literal.
pub fn is_reference(v: &str) -> bool {
    v.starts_with("env:") || v.starts_with("file:")
}

/// The value `v` designates. A file's trailing newline is dropped.
pub fn resolve(v: &str) -> Result<String> {
    if let Some(name) = v.strip_prefix("env:") {
        return std::env::var(name).map_err(|_| anyhow!("environment variable `{name}` is not set"));
    }
    if let Some(path) = v.strip_prefix("file:") {
        let s = std::fs::read_to_string(path).with_context(|| format!("reading secret file `{path}`"))?;
        return Ok(s.trim_end_matches(['\n', '\r']).to_string());
    }
    Ok(v.to_string())
}

/// Like [`resolve`], but the value must be a reference.
pub fn resolve_reference(field: &str, v: &str) -> Result<String> {
    if !is_reference(v) {
        bail!("`{field}` must be a reference (`env:NAME` or `file:/path`), not a literal");
    }
    resolve(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        assert_eq!(resolve("plain").unwrap(), "plain");
        std::env::set_var("LAZYWF_TEST_SECRET_X", "s3cret");
        assert_eq!(resolve("env:LAZYWF_TEST_SECRET_X").unwrap(), "s3cret");
        assert!(resolve("env:LAZYWF_TEST_SECRET_MISSING").is_err());
        let dir = std::env::temp_dir().join(format!("lazywf-secret-{}", std::process::id()));
        std::fs::write(&dir, "abc\n").unwrap();
        assert_eq!(resolve(&format!("file:{}", dir.display())).unwrap(), "abc");
        std::fs::remove_file(&dir).ok();
        assert!(resolve_reference("secret_key_ref", "literal").is_err());
    }
}
