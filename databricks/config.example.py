# ============================================================
# Databricks Configuration — copy to config.py and fill in
# ============================================================

# Workspace URL — e.g. https://adb-1234567890.12.azuredatabricks.net
# or https://your-company.cloud.databricks.com
DATABRICKS_HOST = "https://your-workspace.azuredatabricks.net"

# Personal Access Token
# Databricks UI → User Settings → Developer → Access Tokens → Generate
DATABRICKS_TOKEN = "dapi..."

# ── Cluster config ─────────────────────────────────────────
# Option A: provide an existing cluster ID to reuse it
#   Find ID: Databricks UI → Compute → click cluster → copy Cluster ID
#   Leave empty to use Option B (ephemeral job clusters — cheaper, recommended)
DATABRICKS_CLUSTER_ID = ""

# Option B: new job cluster created per pipeline run
#   Find valid versions: Databricks UI → Compute → Create Cluster → Runtime dropdown
DATABRICKS_SPARK_VERSION = "13.3.x-scala2.12"

#   Find valid node types for your cloud:
#   Azure: Standard_DS3_v2, Standard_DS4_v2, Standard_D8s_v3
#   AWS:   i3.xlarge, m5.xlarge, c5.2xlarge
#   GCP:   n2-standard-4, n2-standard-8
DATABRICKS_NODE_TYPE = "Standard_DS3_v2"

# ── Groq API ───────────────────────────────────────────────
# https://console.groq.com → API Keys
GROQ_API_KEY = "your-groq-api-key"
