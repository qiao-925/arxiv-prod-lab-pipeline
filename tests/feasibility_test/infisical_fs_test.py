from infisical_sdk import InfisicalSDKClient, ListSecretsResponse
from dotenv import load_dotenv

import os
load_dotenv()

client = InfisicalSDKClient(host="https://app.infisical.com") # host is optional, defaults to https://app.infisical.com

client.auth.universal_auth.login(
  "5942e700-9df1-496c-9cb7-e64c9ebd56a2",
  "a22debd08d67d3b4d4824bb51a2698429a270381b422a5be091b77581aa0525f"
)

def main():
    secrets = client.secrets.list_secrets(
        project_id="6affa6e7-d0f8-4949-af25-a0568b5359aa",
        environment_slug=os.getenv("ENV"),
        secret_path="/",
        expand_secret_references=True,  # Optional
        view_secret_value=True,  # Optional
        recursive=False,  # Optional
        include_imports=True,  # Optional
        tag_filters=[]  # Optional
    );

    secrets_map = {s.secretKey: s.secretValue.strip() for s in secrets.secrets}

    print(secrets_map)

if __name__ == "__main__":
    main()