"""Local-only Superset configuration for this demo.

SECRET_KEY signs the session cookies and encrypts the warehouse password stored in superset.db,
so it is random per install: scripts/local.sh generates $SUPERSET_HOME/secret_key once.
"""
import os

_HOME = os.environ.get("SUPERSET_HOME", os.path.join(os.getcwd(), ".superset"))
_SECRET_KEY_FILE = os.path.join(_HOME, "secret_key")
try:
    with open(_SECRET_KEY_FILE) as f:
        SECRET_KEY = f.read().strip()
except FileNotFoundError:
    SECRET_KEY = ""
if not SECRET_KEY:
    raise RuntimeError(
        f"{_SECRET_KEY_FILE} is missing or empty: run Superset through scripts/local.sh, which generates it."
    )
SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(_HOME, "superset.db")
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    # No chart or dataset uses Jinja; leaving it on would only widen what a stolen session can run
    "ENABLE_TEMPLATE_PROCESSING": False,
}
ROW_LIMIT = 100_000
# No security headers on this loopback-only demo. Turn Talisman on if Superset is ever served
# beyond localhost.
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
