//! Credential values in a target's `vars`: a literal, or a reference `env:NAME` / `file:/path`.
//!
//! Whoever can write the target table chooses both a target's endpoint and its credentials, so
//! references are confined: `env:` cannot name lazywf's own settings (`LAZYWF_*`, which hold
//! its database password), and `file:` must resolve inside a directory of `LAZYWF_SECRET_DIRS`
//! (`:`-separated, default `/run/secrets:/var/run/secrets`).

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

pub const DEFAULT_SECRET_DIRS: &str = "/run/secrets:/var/run/secrets";

/// Whether `v` is a reference rather than a literal.
pub fn is_reference(v: &str) -> bool {
    v.starts_with("env:") || v.starts_with("file:")
}

fn secret_dirs() -> Vec<PathBuf> {
    let dirs = std::env::var("LAZYWF_SECRET_DIRS").unwrap_or_else(|_| DEFAULT_SECRET_DIRS.into());
    dirs.split(':')
        .filter(|d| !d.trim().is_empty())
        .filter_map(|d| std::fs::canonicalize(d.trim()).ok())
        .collect()
}

fn allowed_file(path: &str) -> Result<PathBuf> {
    let real = std::fs::canonicalize(path).with_context(|| format!("secret file `{path}`"))?;
    let dirs = secret_dirs();
    if !dirs.iter().any(|d| real.starts_with(d)) {
        bail!(
            "secret file `{path}` is outside the secret directories ({}); add its directory to LAZYWF_SECRET_DIRS",
            std::env::var("LAZYWF_SECRET_DIRS").unwrap_or_else(|_| DEFAULT_SECRET_DIRS.into())
        );
    }
    Ok(real)
}

/// The value `v` designates. A file's trailing newline is dropped.
pub fn resolve(v: &str) -> Result<String> {
    if let Some(name) = v.strip_prefix("env:") {
        if name.to_ascii_uppercase().starts_with("LAZYWF_") {
            bail!("`env:{name}`: lazywf's own settings cannot be target credentials");
        }
        return std::env::var(name)
            .map_err(|_| anyhow!("environment variable `{name}` is not set"));
    }
    if let Some(path) = v.strip_prefix("file:") {
        let real = allowed_file(path)?;
        let s = std::fs::read_to_string(&real)
            .with_context(|| format!("reading secret file `{path}`"))?;
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

/// Whether `dir` is among the secret directories (for messages and tests).
pub fn is_secret_dir(dir: &Path) -> bool {
    std::fs::canonicalize(dir).is_ok_and(|d| secret_dirs().contains(&d))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        assert_eq!(resolve("plain").unwrap(), "plain");
        std::env::set_var("LWF_UNIT_SECRET_X", "s3cret");
        assert_eq!(resolve("env:LWF_UNIT_SECRET_X").unwrap(), "s3cret");
        assert!(resolve("env:LWF_UNIT_SECRET_MISSING").is_err());
        std::env::set_var("LAZYWF_PASSWORD_UNIT", "db");
        assert!(resolve("env:LAZYWF_PASSWORD_UNIT").is_err());
        assert!(resolve("env:lazywf_password_unit").is_err());
        assert!(resolve_reference("secret_key_ref", "literal").is_err());

        let base = std::env::temp_dir().join(format!("lazywf-secrets-{}", std::process::id()));
        let allowed = base.join("allowed");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::write(allowed.join("pw"), "abc\n").unwrap();
        std::fs::write(base.join("outside"), "no").unwrap();
        std::env::set_var("LAZYWF_SECRET_DIRS", allowed.display().to_string());
        assert!(is_secret_dir(&allowed));
        assert_eq!(
            resolve(&format!("file:{}/pw", allowed.display())).unwrap(),
            "abc"
        );
        // Outside, through `..`, or through a symlink out of the directory: refused.
        assert!(resolve(&format!("file:{}/outside", base.display())).is_err());
        assert!(resolve(&format!("file:{}/../outside", allowed.display())).is_err());
        std::os::unix::fs::symlink(base.join("outside"), allowed.join("link")).ok();
        assert!(resolve(&format!("file:{}/link", allowed.display())).is_err());
        assert!(resolve("file:/proc/self/environ").is_err());
        std::fs::remove_dir_all(&base).ok();
    }
}
