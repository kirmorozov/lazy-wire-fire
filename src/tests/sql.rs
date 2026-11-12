use super::*;

#[test]
fn placeholders_skip_literals_and_comments() {
    let sql = "SELECT ':keys', `a:keys`, \"x:y\" /* :no */ -- :no\n FROM t \
                   WHERE id IN (:keys) AND p = :old.parent_id AND q = :new.`odd col` # :no\n AND @a := 1";
    let names: Vec<_> = placeholders(sql).into_iter().map(|p| p.name).collect();
    assert_eq!(names, vec!["keys", "old.parent_id", "new.odd col"]);
}

#[test]
fn expansion_counts() {
    let (s, names) = expand(
        "SELECT x AS _key FROM t WHERE id IN (:keys) OR p = :new.id",
        |n| {
            if n == "keys" {
                3
            } else {
                1
            }
        },
    );
    assert_eq!(s, "SELECT x AS _key FROM t WHERE id IN (?, ?, ?) OR p = ?");
    assert_eq!(names, vec!["keys", "keys", "keys", "new.id"]);
    // Escaped quotes do not end literals early.
    let (s, n) = expand(
        "SELECT 'it''s :keys', 'a\\':keys' WHERE k IN (:keys)",
        |_| 1,
    );
    assert_eq!(s, "SELECT 'it''s :keys', 'a\\':keys' WHERE k IN (?)");
    assert_eq!(n.len(), 1);
}

#[test]
fn statements_split() {
    let parts = split_statements("SELECT 1 AS _key; SELECT ';' AS x -- ; c\n;\n\n ; /* only */ ;");
    assert_eq!(parts, vec!["SELECT 1 AS _key", "SELECT ';' AS x -- ; c"]);
}

#[test]
fn ddl() {
    use DdlEffect::*;
    let d = |q: &str| ddl_tables(q, "app");
    assert_eq!(
        d("TRUNCATE TABLE t"),
        vec![("app".into(), "t".into(), Replace)]
    );
    assert_eq!(
        d("RENAME TABLE `t` TO old_t, s.new_t TO t"),
        vec![
            ("app".into(), "t".into(), Replace),
            ("app".into(), "old_t".into(), Replace),
            ("s".into(), "new_t".into(), Replace),
            ("app".into(), "t".into(), Replace)
        ]
    );
    assert_eq!(
        d("ALTER TABLE t ADD INDEX i (a)"),
        vec![("app".into(), "t".into(), Reshape)]
    );
    assert_eq!(
        d("ALTER TABLE t RENAME COLUMN a TO b"),
        vec![("app".into(), "t".into(), Reshape)]
    );
    assert_eq!(
        d("ALTER TABLE t RENAME TO u"),
        vec![
            ("app".into(), "t".into(), Replace),
            ("app".into(), "u".into(), Replace)
        ]
    );
    assert_eq!(
        d("DROP TABLE IF EXISTS a, `b`"),
        vec![
            ("app".into(), "a".into(), Replace),
            ("app".into(), "b".into(), Replace)
        ]
    );
    assert_eq!(
        d("CREATE TABLE IF NOT EXISTS x (a int)"),
        vec![("app".into(), "x".into(), Reshape)]
    );
    assert_eq!(
        d("CREATE OR REPLACE TABLE x (a int)"),
        vec![("app".into(), "x".into(), Replace)]
    );
    assert_eq!(
        d("CREATE UNIQUE INDEX i ON x (a)"),
        vec![("app".into(), "x".into(), Reshape)]
    );
    assert_eq!(
        d("DROP DATABASE shop"),
        vec![("shop".into(), "*".into(), Replace)]
    );
    assert_eq!(
        d("/*!40000 TRUNCATE t */"),
        vec![("app".into(), "t".into(), Replace)]
    );
    assert!(d("GRANT SELECT ON *.* TO u").is_empty());
    assert!(d("CREATE DATABASE x").is_empty());
}

#[test]
fn online_schema_change_statements() {
    let n = |s: &str, t: &str| (s.to_string(), t.to_string());
    assert_eq!(
        table_like(
            "create /* gh-ost */ table `app`.`_t_gho` like `app`.`t`",
            "x"
        ),
        Some((n("app", "_t_gho"), n("app", "t")))
    );
    assert_eq!(
        table_like("CREATE TABLE IF NOT EXISTS t2 (LIKE s.t1)", "app"),
        Some((n("app", "t2"), n("s", "t1")))
    );
    assert_eq!(table_like("CREATE TABLE t2 (a INT)", "app"), None);
    assert_eq!(table_like("CREATE TEMPORARY TABLE t2 LIKE t", "app"), None);
    assert_eq!(
        renames(
            "rename /* gh-ost */ table `app`.`t` to `app`.`_t_del`, `app`.`_t_gho` to `app`.`t`",
            "x"
        ),
        vec![
            (n("app", "t"), n("app", "_t_del")),
            (n("app", "_t_gho"), n("app", "t"))
        ]
    );
    assert_eq!(
        renames("RENAME TABLE t TO u", "app"),
        vec![(n("app", "t"), n("app", "u"))]
    );
    assert!(renames("ALTER TABLE t RENAME TO u", "app").is_empty());
}

#[test]
fn executable_comments_are_marked() {
    // The probe's shape: the body ends at the `*/` inside the literal, so the server runs
    // `INSERT` as a second statement. The lexer must see the body as executable.
    let probe = "/*!SELECT '*/' */; INSERT INTO t VALUES (21)";
    assert!(has_executable_comment(probe));
    assert!(has_executable_comment("/*!50000 SELECT 1 */"));
    assert!(has_executable_comment("/*M!100000 SELECT 1 */"));
    assert!(has_executable_comment("SELECT 1 /*!; DROP TABLE t */"));
    assert!(has_executable_comment("/*!40000 TRUNCATE t */"));
    // Hints and ordinary comments are not executable.
    assert!(!has_executable_comment("SELECT /*+ NO_INDEX(t) */ 1"));
    assert!(!has_executable_comment("SELECT 1 /* plain ; comment */"));
    assert!(!has_executable_comment("SELECT 1 -- /*! not one"));
    // Inside literals and quoted identifiers the text is data.
    assert!(!has_executable_comment("SELECT '/*! x */' AS a"));
    assert!(!has_executable_comment("SELECT \"/*!\" AS `a /*! b`"));
    assert!(!has_executable_comment(
        "INSERT INTO t VALUES ('a\\'/*!x*/')"
    ));
    // An executable comment is not a statement break for the splitter either way.
    assert_eq!(split_statements("SELECT '/*!' AS a; SELECT 2").len(), 2);
}

#[test]
fn expressions() {
    let e = expression(
        "status IN (200, 204) AND JSON_CONTAINS(body, 'false', '$.errors') -- ; ?\n/* , */",
    )
    .unwrap();
    assert_eq!(
        e.words,
        ["status", "IN", "200", "204", "AND", "JSON_CONTAINS", "body"].map(String::from)
    );
    assert_eq!(e.calls, ["IN", "JSON_CONTAINS"].map(String::from));
    assert!(!e.variables);
    assert!(expression("`text` = 'a;b' AND @x = 1").unwrap().variables);
    for bad in [
        "",
        " -- nothing",
        "1; DROP TABLE t",
        "status = ?",
        "status = :x",
        "errors == false",
        "200, 204",
        "1) OR (1",
        "(1",
    ] {
        assert!(expression(bad).is_err(), "{bad}");
    }
    assert!(expression("a != 1 AND b <= 2 AND c <=> NULL").is_ok());
}
