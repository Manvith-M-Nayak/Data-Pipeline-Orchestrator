import os
from dotenv import load_dotenv

load_dotenv()

# Azure Storage
AZURE_STORAGE_ACCOUNT  = os.environ.get("AZURE_STORAGE_ACCOUNT", "")
AZURE_STORAGE_KEY      = os.environ.get("AZURE_STORAGE_KEY", "")

# Azure AD / Service Principal
AZURE_TENANT_ID        = os.environ.get("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID        = os.environ.get("AZURE_CLIENT_ID", "")
AZURE_CLIENT_SECRET    = os.environ.get("AZURE_CLIENT_SECRET", "")
AZURE_SUBSCRIPTION_ID  = os.environ.get("AZURE_SUBSCRIPTION_ID", "")

# Azure Resource Group & Data Factory
AZURE_RESOURCE_GROUP   = os.environ.get("AZURE_RESOURCE_GROUP", "adf-demo-rg")
AZURE_DATA_FACTORY     = os.environ.get("AZURE_DATA_FACTORY", "adf-monitor-demo")

# Groq
GROQ_API_KEY           = os.environ.get("GROQ_API_KEY", "")