# ============================================================
# Unified Orchestrator Configuration
# Copy this file to config.py and fill in your credentials.
# config.py is gitignored — never commit real secrets.
# ============================================================

# ── Azure Service Principal (ADF + Blob management) ──────────
AZURE_TENANT_ID     = "your-tenant-id"
AZURE_CLIENT_ID     = "your-client-id"
AZURE_CLIENT_SECRET = "your-client-secret"

# ── Azure Data Factory ───────────────────────────────────────
AZURE_SUBSCRIPTION_ID = "your-subscription-id"
AZURE_RESOURCE_GROUP  = "your-resource-group"
AZURE_DATA_FACTORY    = "your-data-factory-name"

# ── Azure Blob Storage (landing + staged + curated zones) ────
AZURE_STORAGE_ACCOUNT = "your-storage-account-name"
AZURE_STORAGE_KEY     = "your-storage-account-key"

# ── Azure Databricks (compute plane) ─────────────────────────
# Workspace URL e.g. https://adb-1234567890.12.azuredatabricks.net
DATABRICKS_HOST  = "https://your-workspace.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi..."

# Cluster strategy for ADF Databricks Notebook activity:
#   - Leave DATABRICKS_CLUSTER_ID empty → ADF creates an ephemeral
#     job cluster per run (cheaper, recommended).
#   - Set an existing cluster ID to reuse interactive cluster.
DATABRICKS_CLUSTER_ID    = ""
DATABRICKS_SPARK_VERSION = "13.3.x-scala2.12"
DATABRICKS_NODE_TYPE     = "Standard_DS3_v2"

# Workspace folder where generated notebooks are uploaded.
DATABRICKS_NOTEBOOK_BASE = "/Shared/unified_orchestrator"

# ── Planner backend selection ────────────────────────────────
# Which LLM serves the planner agent:
#   "ollama" → local fine-tuned model (see planner_agent/model/README_OLLAMA.md)
#   "groq"   → Groq cloud LLaMA (legacy)
PLANNER_BACKEND = "ollama"

# ── Ollama (local fine-tuned planner) ────────────────────────
OLLAMA_HOST   = "http://localhost:11434"   # `ollama serve` endpoint
PLANNER_MODEL = "planner-agent"            # model name from `ollama create`

# ── Groq (planner LLM — fallback / legacy) ───────────────────
GROQ_API_KEY = "your-groq-api-key"
