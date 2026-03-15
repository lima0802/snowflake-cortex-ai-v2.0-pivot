"""
DIA v2 — Snowflake Connection Test
====================================
Validates Snowflake connectivity before running the full stack.

Usage (local):
    pip install snowflake-connector-python python-dotenv
    python test/test_snowflake_connection.py

Usage (Docker):
    docker-compose -f test/docker-compose.test.yml run --rm snowflake-test
"""

import os
import sys
import time

from dotenv import load_dotenv

# Load .env from project root (one level up from test/)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ok(msg: str):
    print(f"  [PASS] {msg}")

def _fail(msg: str):
    print(f"  [FAIL] {msg}")

def _info(msg: str):
    print(f"  [INFO] {msg}")

def section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_env_vars() -> bool:
    """1. Verify all required environment variables are present."""
    section("1. Environment Variables")
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
    ]
    missing = []
    for var in required:
        val = os.getenv(var)
        if val:
            _ok(f"{var} = {'*' * min(len(val), 4)}{'val[4:]' if len(val) > 4 else ''}")
        else:
            _fail(f"{var} is not set")
            missing.append(var)

    if missing:
        print(f"\n  Missing vars: {missing}")
        print("  → Copy .env.example to .env and fill in your credentials.")
        return False
    return True


def test_import() -> bool:
    """2. Verify snowflake-connector-python is installed."""
    section("2. Package Import")
    try:
        import snowflake.connector  # noqa: F401
        _ok("snowflake-connector-python imported successfully")
        return True
    except ImportError as e:
        _fail(f"Import error: {e}")
        print("  → Run: pip install snowflake-connector-python")
        return False


def test_basic_connection() -> bool:
    """3. Open a connection to Snowflake."""
    section("3. Basic Connection")
    try:
        import snowflake.connector

        conn_params = {
            "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
            "user":      os.getenv("SNOWFLAKE_USER"),
            "password":  os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database":  os.getenv("SNOWFLAKE_DATABASE"),
            "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
            "role":      os.getenv("SNOWFLAKE_ROLE"),
            "login_timeout": 15,
        }

        _info(f"Connecting to account: {conn_params['account']}")
        _info(f"User: {conn_params['user']}  |  Role: {conn_params['role']}")
        _info(f"Warehouse: {conn_params['warehouse']}  |  DB: {conn_params['database']}.{conn_params['schema']}")

        t0 = time.time()
        conn = snowflake.connector.connect(**conn_params)
        elapsed = time.time() - t0
        _ok(f"Connection opened in {elapsed:.2f}s")

        conn.close()
        _ok("Connection closed cleanly")
        return True

    except Exception as e:
        _fail(f"Connection failed: {e}")
        return False


def test_execute_query() -> bool:
    """4. Run a simple query and fetch results."""
    section("4. Query Execution")
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            login_timeout=15,
        )
        cur = conn.cursor()

        # Current version
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        _ok(f"Snowflake version: {version}")

        # Current context
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        row = cur.fetchone()
        _ok(f"User:      {row[0]}")
        _ok(f"Role:      {row[1]}")
        _ok(f"Database:  {row[2]}")
        _ok(f"Schema:    {row[3]}")
        _ok(f"Warehouse: {row[4]}")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        _fail(f"Query failed: {e}")
        return False


def test_list_tables() -> bool:
    """5. List tables in the configured schema."""
    section("5. Schema / Table Discovery")
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            login_timeout=15,
        )
        cur = conn.cursor()

        db     = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")

        cur.execute(f"SHOW TABLES IN {db}.{schema}")
        tables = cur.fetchall()

        if tables:
            _ok(f"Found {len(tables)} table(s) in {db}.{schema}:")
            for t in tables[:10]:          # show max 10
                _info(f"  • {t[1]}")       # column 1 = table name
            if len(tables) > 10:
                _info(f"  … and {len(tables) - 10} more")
        else:
            _info(f"No tables found in {db}.{schema} (schema may be empty — that's OK)")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        _fail(f"Table listing failed: {e}")
        return False


def test_semantic_stage() -> bool:
    """6. Check that the semantic model stage exists (DIA requirement)."""
    section("6. Semantic Model Stage")
    stage_name = os.getenv("SNOWFLAKE_SEMANTIC_STAGE", "SEMANTIC_MODELS")
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            login_timeout=15,
        )
        cur = conn.cursor()

        cur.execute("SHOW STAGES")
        stages = cur.fetchall()
        stage_names = [s[1].upper() for s in stages]   # column 1 = stage name

        if stage_name.upper() in stage_names:
            _ok(f"Stage '{stage_name}' found")
        else:
            _info(f"Stage '{stage_name}' not found yet. Available: {stage_names or 'none'}")
            _info("  → Run the setup script to create it, or this is fine for a fresh env.")

        cur.close()
        conn.close()
        return True      # non-fatal — stage may not exist on first run

    except Exception as e:
        _fail(f"Stage check failed: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  DIA v2 — Snowflake Connection Test Suite")
    print("  VML MAP × Volvo Cars Corporation")
    print("=" * 60)

    results = {
        "Environment Variables": test_env_vars(),
        "Package Import":        test_import(),
        "Basic Connection":      test_basic_connection(),
        "Query Execution":       test_execute_query(),
        "Table Discovery":       test_list_tables(),
        "Semantic Stage":        test_semantic_stage(),
    }

    # Summary
    section("Summary")
    passed = sum(results.values())
    total  = len(results)
    for name, ok in results.items():
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")

    print(f"\n  Result: {passed}/{total} tests passed")

    if passed == total:
        print("\n  Snowflake connection is healthy. Ready to run DIA v2.")
        sys.exit(0)
    elif passed >= 4:
        print("\n  Core connectivity is working. Minor issues above (non-blocking).")
        sys.exit(0)
    else:
        print("\n  Connection issues detected. Fix the failures above before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()
