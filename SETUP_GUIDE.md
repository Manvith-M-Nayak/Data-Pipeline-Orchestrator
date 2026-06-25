# Resource Setup Guide

Complete step-by-step instructions for creating every Azure resource and API key this project needs, and exactly where each value goes in `unified/config.py`.

> **Azure for Students:** You get $100 free credit with no credit card required. All resources in this guide are available on the student tier. Watch the Databricks cost warning in step 6 — it is the only resource that can drain your credit quickly if left running.

---

## Prerequisites

- An active Azure subscription (including Azure for Students — sign up at azure.microsoft.com/en-us/free/students)
- Python 3.9 or later (`python --version` to check)
- Node.js 18 or later for the frontend (`node --version` to check — download from nodejs.org if missing)
- A Groq account (free at console.groq.com)

---

## 1. Azure Service Principal

The service principal is a non-human identity that the project uses to authenticate to Azure APIs. One principal is shared across ADF, Storage, and Databricks.

> **Portal name change:** Azure Active Directory was renamed to **Microsoft Entra ID** in the Azure portal. Search for "Microsoft Entra ID" — not "Azure Active Directory" — or the search will return no results.

### Steps

1. Go to **portal.azure.com**
2. In the top search bar, type **Microsoft Entra ID** and click it
3. In the left sidebar, click **App registrations**
4. Click **+ New registration**
5. Fill in:
   - **Name**: `data-pipeline-orchestrator` (or any name you prefer)
   - **Supported account types**: Choose based on your account type:
     - **University/organization email** (e.g. `name@university.edu`) → select **Accounts in this organizational directory only**
     - **Personal Microsoft account** (e.g. `name@outlook.com`) → select **Accounts in any organizational directory and personal Microsoft accounts**
   - **Redirect URI**: leave blank
6. Click **Register**

### Collect values

After registration, you land on the app overview page.

| Value | Where on the page | Config key |
|---|---|---|
| `AZURE_TENANT_ID` | Overview → **Directory (tenant) ID** | `unified/config.py` |
| `AZURE_CLIENT_ID` | Overview → **Application (client) ID** | `unified/config.py` |

### Create a client secret

1. In the left sidebar of your app, click **Certificates & secrets**
2. Click **+ New client secret**
3. Set a description (e.g. `pipeline-secret`) and choose an expiry (24 months recommended)
4. Click **Add**
5. **Copy the Value immediately** — it is only shown once

| Value | Where | Config key |
|---|---|---|
| `AZURE_CLIENT_SECRET` | The **Value** column (copy before leaving the page) | `unified/config.py` |

---

## 2. Azure Subscription ID

1. In the portal search bar, type **Subscriptions**
2. Click on your subscription
3. The **Subscription ID** is shown on the overview page (a UUID)

| Value | Where | Config key |
|---|---|---|
| `AZURE_SUBSCRIPTION_ID` | Subscriptions → your sub → Overview → **Subscription ID** | `unified/config.py` |

---

## 3. Resource Group

A resource group is a container that holds related Azure resources. ADF, Storage, and Databricks should all live in the same resource group.

### Steps

1. In the portal search bar, type **Resource groups**
2. Click **+ Create**
3. Fill in:
   - **Subscription**: select your subscription
   - **Resource group**: `data-pipeline-rg` (or any name)
   - **Region**: choose the region closest to you (e.g. East US, West Europe)
4. Click **Review + create** → **Create**

| Value | Where | Config key |
|---|---|---|
| `AZURE_RESOURCE_GROUP` | The name you chose above | `unified/config.py` |

---

## 4. Azure Data Factory

### Steps

1. In the portal search bar, type **Data factories**
2. Click **+ Create**
3. Fill in:
   - **Subscription**: your subscription
   - **Resource group**: `data-pipeline-rg`
   - **Name**: `my-data-factory` (must be globally unique — add your initials or a number)
   - **Region**: same region as your resource group
   - **Version**: V2
4. Click **Review + create** → **Create**
5. Wait for deployment to complete (about 1–2 minutes)

| Value | Where | Config key |
|---|---|---|
| `AZURE_DATA_FACTORY` | The name you chose above | `unified/config.py` |
| `ADF_FACTORY_NAME` | Same name | `unified/monitor_agent` (bridged automatically from config.py) |

### Verify

Go to **Data factories** in the portal and confirm your factory appears in the list.

---

## 5. Azure Storage Account

The project uses Blob Storage for landing zones (raw, bronze, silver containers).

### Steps

1. In the portal search bar, type **Storage accounts**
2. Click **+ Create**
3. Fill in:
   - **Subscription**: your subscription
   - **Resource group**: `data-pipeline-rg`
   - **Storage account name**: `pipelinestorage` + your initials (e.g. `pipelinestoragemn`) — must be 3–24 lowercase letters and numbers, globally unique
   - **Region**: same region
   - **Performance**: Standard
   - **Redundancy**: LRS (Locally-redundant storage) — cheapest option for development
4. Click **Review** → **Create**
5. Wait for deployment

### Collect the storage account name

| Value | Where | Config key |
|---|---|---|
| `AZURE_STORAGE_ACCOUNT` | The name you chose above (e.g. `pipelinestoragemn`) | `unified/config.py` |

### Collect the storage account key

1. Go to your storage account in the portal
2. In the left sidebar, click **Security + networking** → **Access keys**
3. Click **Show keys**
4. Copy **key1** → **Key** value (a long base64 string)

| Value | Where | Config key |
|---|---|---|
| `AZURE_STORAGE_KEY` | Access keys → key1 → **Key** | `unified/config.py` |

---

## 6. Azure Databricks Workspace

> **Azure for Students — cost warning:** Databricks charges for VM compute time while a cluster is running. The default `Standard_DS3_v2` node costs ~$0.10–0.20/hour. Ephemeral job clusters (the default in this project) shut down automatically after each run, so you will not be charged while idle. Do **not** create an interactive all-purpose cluster and leave it running — it will drain your $100 credit in a few days.

> **Quota issue:** Student subscriptions often have a low vCPU quota (4–10 cores per region). `Standard_DS3_v2` uses 4 cores. If you get a quota error during cluster creation, see the "Request a quota increase" note below or switch to `Standard_DS2_v2` (2 cores, 7 GB RAM) which is smaller but sufficient for testing.

### Steps

1. In the portal search bar, type **Azure Databricks**
2. Click **+ Create**
3. Fill in:
   - **Subscription**: your subscription
   - **Resource group**: `data-pipeline-rg`
   - **Workspace name**: `pipeline-databricks`
   - **Region**: same region — if Databricks is not available in your region, try **East US** or **West Europe**
   - **Pricing tier**: **Standard** (sufficient for development — do not choose Premium)
4. Click **Review + create** → **Create**
5. Deployment takes 3–5 minutes

### If you get a quota error

1. In the portal search bar, type **Quotas**
2. Click **My quotas**
3. Filter by your subscription and search for `DSv2`
4. Click the pencil icon next to the quota and request an increase to 8 cores
5. Student quota increases are usually auto-approved within minutes

### Collect the workspace URL

1. Go to your Databricks resource in the portal
2. Click **Launch Workspace** — this opens the Databricks UI
3. Copy the URL from your browser: it looks like `https://adb-1234567890123456.12.azuredatabricks.net`

| Value | Where | Config key |
|---|---|---|
| `DATABRICKS_HOST` | Browser URL after clicking Launch Workspace | `unified/config.py` |

### Generate a personal access token

1. Inside the Databricks workspace (the UI that opened above)
2. Click your username in the top-right corner → **Settings**
3. Click **Developer** in the left sidebar
4. Next to **Access tokens**, click **Manage**
5. Click **Generate new token**
6. Set a comment (e.g. `pipeline-token`) and a lifetime (90 days is fine)
7. Click **Generate**
8. **Copy the token immediately** — shown only once. Starts with `dapi`

| Value | Where | Config key |
|---|---|---|
| `DATABRICKS_TOKEN` | The token you just generated (starts with `dapi`) | `unified/config.py` |

### Cluster settings

Leave `DATABRICKS_CLUSTER_ID` empty. The project creates ephemeral job clusters per run, which is cheaper than keeping an interactive cluster running.

The defaults below work for most cases:

```python
DATABRICKS_CLUSTER_ID    = ""                      # empty = ephemeral job cluster
DATABRICKS_SPARK_VERSION = "13.3.x-scala2.12"     # LTS runtime
DATABRICKS_NODE_TYPE     = "Standard_DS3_v2"       # 14 GB RAM, 4 vCPUs — use Standard_DS2_v2 if quota is tight
DATABRICKS_NOTEBOOK_BASE = "/Shared/unified_orchestrator"
```

---

## 7. RBAC Role Assignments

The service principal you created in step 1 needs permission to control ADF, Storage, and Databricks. Assign these three roles.

### 7a. Data Factory Contributor (on ADF)

1. In the portal, go to your Data Factory resource
2. In the left sidebar, click **Access control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. Search for and select **Data Factory Contributor**
5. Click **Next**
6. For **Assign access to**, select **User, group, or service principal**
7. Click **+ Select members**
8. Search for the name of your app registration (e.g. `data-pipeline-orchestrator`)
9. Select it and click **Select**
10. Click **Review + assign** → **Review + assign**

### 7b. Storage Blob Data Contributor (on Storage Account)

1. In the portal, go to your Storage Account
2. In the left sidebar, click **Access control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. Search for and select **Storage Blob Data Contributor**
5. Follow the same steps 5–10 above, selecting your service principal

### 7c. Contributor (on Databricks workspace)

1. In the portal, go to your Azure Databricks resource (not the workspace UI, the portal resource page)
2. In the left sidebar, click **Access control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. Search for and select **Contributor**
5. Follow the same steps 5–10 above, selecting your service principal

> **If you cannot assign roles:** Student subscriptions where you are not the Owner of the subscription will block role assignments. If you see "You do not have permission to create role assignments", go to **Subscriptions → your sub → Access control (IAM)** and confirm your own role is **Owner**. If you signed up via Azure for Students directly (not through a university-provided subscription), you are the Owner and this will work.

---

## 8. Groq API Key

Groq provides the LLM used by the planner, monitor, and self-healing agents.

### Steps

1. Go to **console.groq.com**
2. Sign up or log in (free tier is sufficient)
3. In the left sidebar, click **API Keys**
4. Click **Create API Key**
5. Give it a name (e.g. `pipeline-key`)
6. Copy the key (starts with `gsk_`)

| Value | Where | Config key |
|---|---|---|
| `GROQ_API_KEY` | Groq Console → API Keys → your key | `unified/config.py` |

---

## 9. Fill in `unified/config.py`

Copy the example file and fill in all values collected above:

```bash
cp unified/config.example.py unified/config.py
```

Then open `unified/config.py` and replace every placeholder:

```python
# ── Azure Service Principal ──────────────────────────────────
AZURE_TENANT_ID     = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"   # step 1
AZURE_CLIENT_ID     = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"   # step 1
AZURE_CLIENT_SECRET = "your-secret-value-from-step-1"          # step 1

# ── Azure Subscription + Resource Group ─────────────────────
AZURE_SUBSCRIPTION_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" # step 2
AZURE_RESOURCE_GROUP  = "data-pipeline-rg"                      # step 3

# ── Azure Data Factory ───────────────────────────────────────
AZURE_DATA_FACTORY = "my-data-factory"                          # step 4

# ── Azure Blob Storage ───────────────────────────────────────
AZURE_STORAGE_ACCOUNT = "pipelinestoragemn"                     # step 5
AZURE_STORAGE_KEY     = "your-long-base64-key"                  # step 5

# ── Azure Databricks ─────────────────────────────────────────
DATABRICKS_HOST       = "https://adb-1234567890.12.azuredatabricks.net"  # step 6
DATABRICKS_TOKEN      = "dapixxxxxxxxxxxxxxxxxxxx"              # step 6
DATABRICKS_CLUSTER_ID = ""
DATABRICKS_SPARK_VERSION = "13.3.x-scala2.12"
DATABRICKS_NODE_TYPE     = "Standard_DS3_v2"
DATABRICKS_NOTEBOOK_BASE = "/Shared/unified_orchestrator"

# ── Groq ─────────────────────────────────────────────────────
GROQ_API_KEY = "gsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxx"               # step 8
```

> **Note:** `unified/config.py` is already in `.gitignore`. Never commit it.

The unified backend (`unified/main.py`) automatically bridges all values from `config.py` into environment variables, so the monitor agent services pick them up without any extra configuration. You do **not** need a separate `.env` file.

---

## 10. Verify Everything Works

Run these checks in order. Each one is independent and fast.

### Check 1 — Python can read config

```bash
cd unified
python3 -c "import config; print('Tenant:', config.AZURE_TENANT_ID[:8], '...')"
```

Expected: prints first 8 chars of your tenant ID.

### Check 2 — Database initializes

```bash
python3 -c "
import asyncio, sys
sys.path.insert(0, '.')
import config, os
os.environ.setdefault('AZURE_TENANT_ID', config.AZURE_TENANT_ID)
from monitor_agent.services.db_service import DBService
asyncio.run(DBService().initialize())
print('DB OK')
"
```

Expected: `DB OK`. Creates `unified/data/adf_monitor.db`.

### Check 3 — Azure token

```bash
python3 -c "
import asyncio, sys, config, os
sys.path.insert(0, '.')
os.environ['AZURE_TENANT_ID']     = config.AZURE_TENANT_ID
os.environ['AZURE_CLIENT_ID']     = config.AZURE_CLIENT_ID
os.environ['AZURE_CLIENT_SECRET'] = config.AZURE_CLIENT_SECRET
os.environ['ADF_FACTORY_NAME']    = config.AZURE_DATA_FACTORY
os.environ['AZURE_SUBSCRIPTION_ID'] = config.AZURE_SUBSCRIPTION_ID
os.environ['AZURE_RESOURCE_GROUP']  = config.AZURE_RESOURCE_GROUP
from monitor_agent.services.adf_service import ADFService
token = asyncio.run(ADFService()._get_token())
print('Token OK:', token[:20], '...')
"
```

Expected: prints first 20 chars of an access token.

### Check 4 — ADF connection

```bash
python3 -c "
import asyncio, sys, config, os
sys.path.insert(0, '.')
os.environ['AZURE_TENANT_ID']       = config.AZURE_TENANT_ID
os.environ['AZURE_CLIENT_ID']       = config.AZURE_CLIENT_ID
os.environ['AZURE_CLIENT_SECRET']   = config.AZURE_CLIENT_SECRET
os.environ['ADF_FACTORY_NAME']      = config.AZURE_DATA_FACTORY
os.environ['AZURE_SUBSCRIPTION_ID'] = config.AZURE_SUBSCRIPTION_ID
os.environ['AZURE_RESOURCE_GROUP']  = config.AZURE_RESOURCE_GROUP
from monitor_agent.services.adf_service import ADFService
runs = asyncio.run(ADFService().get_active_pipeline_runs())
print(f'ADF OK — {len(runs)} active run(s)')
"
```

Expected: `ADF OK — 0 active run(s)` (or more if pipelines are running).

### Check 5 — Groq connection

```bash
python3 -c "
import asyncio, sys, config, os
sys.path.insert(0, '.')
os.environ['GROQ_API_KEY'] = config.GROQ_API_KEY
from monitor_agent.services.groq_service import GroqService
result = asyncio.run(GroqService().predict_runtime('test', []))
print('Groq OK:', result.get('confidence'))
"
```

Expected: `Groq OK: low` (or similar).

### Check 6 — Full backend

```bash
cd unified
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Open `http://localhost:8000/api/health` in your browser.  
Expected response: `{"status":"ok","agents":["planner","executor","monitor"]}`

Open `http://localhost:8000/docs` for the full interactive API.

### Check 7 — Frontend

```bash
cd unified/frontend
npm install
npm run dev
```

Open `http://localhost:5173`.  
Expected: sidebar with Live, Logs, Anomalies, Predictions, Planner, Executor pages.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `AuthenticationFailed` or `401` | Wrong tenant/client/secret | Re-check steps 1–2, re-generate secret if expired |
| `ResourceNotFound` or `404` on ADF | Wrong factory name or resource group | Check `AZURE_DATA_FACTORY` and `AZURE_RESOURCE_GROUP` exactly match portal |
| `Authorization_RequestDenied` | Missing RBAC role | Repeat step 7 for the relevant resource |
| `InvalidAuthenticationTokenTenant` | Tenant ID mismatch | `AZURE_TENANT_ID` must be the **Directory (tenant) ID** from Entra ID, not the Subscription ID |
| `AADSTS700016: Application not found` | Personal account tenant mismatch | In app registration, change supported account types to include personal accounts (step 1) |
| `StorageErrorCode.AuthorizationPermissionMismatch` | Storage role missing | Assign Storage Blob Data Contributor (step 7b) |
| `OperationNotAllowed: Operation results in exceeding quota` | Student vCPU quota hit | Switch `DATABRICKS_NODE_TYPE` to `Standard_DS2_v2` or request quota increase |
| `groq.AuthenticationError` | Wrong or missing Groq key | Re-check `GROQ_API_KEY` in config.py |
| `ModuleNotFoundError: No module named 'groq'` | Dependencies not installed | Run `pip install -r unified/requirements.txt` |
| `node: command not found` | Node.js not installed | Download from nodejs.org — install v18 LTS or later |
| `python: command not found` | macOS uses `python3` | Use `python3` — all commands in this guide already use `python3` |
| Role assignment blocked — "You do not have permission" | Not subscription Owner | Confirm you are Owner at Subscriptions → Access control (IAM) |

---

## Cost Estimates (Student Tier)

| Resource | Approximate cost | Student $100 credit impact |
|---|---|---|
| Azure Data Factory | ~$1/1000 pipeline runs | Negligible for dev usage |
| Azure Storage (LRS) | ~$0.02/GB/month | Negligible — test data is tiny |
| Azure Databricks (DS3_v2, ephemeral) | ~$0.10–0.20/hour per cluster | Low — cluster only runs during pipeline execution |
| Azure Databricks (DS2_v2, ephemeral) | ~$0.05–0.10/hour per cluster | Even lower — use if quota is tight |
| Groq API | Free | $0 |

Ephemeral job clusters (the default — `DATABRICKS_CLUSTER_ID = ""`) spin up for each run and shut down automatically, so you only pay for actual compute time.

**To avoid unexpected charges:** do not create an all-purpose interactive cluster in the Databricks UI and leave it running. The project never requires one.
