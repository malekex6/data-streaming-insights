from dataflow.merge_job import MERGE_FILES, SETUP_FILES, render, split_statements


def test_every_sql_file_renders_without_placeholders():
    for name in SETUP_FILES + MERGE_FILES + ["checks.sql"]:
        sql = render(name, "my-project", "my_dataset")
        assert "${" not in sql and "`my-project.my_dataset." in sql


def test_checks_split_into_five_read_only_selects():
    statements = list(split_statements(render("checks.sql", "p", "d")))
    assert len(statements) == 5
    for st in statements:
        code = " ".join(l for l in st.splitlines() if not l.strip().startswith("--")).upper()
        assert not any(w in code for w in ("INSERT ", "UPDATE ", "DELETE ", "MERGE ", "CREATE "))
