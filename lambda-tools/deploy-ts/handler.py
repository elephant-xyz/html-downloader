import os
import boto3
import datetime

lambda_client = boto3.client("lambda")


def handler(event, context):
    # Target Lambda name can be provided via env var; falls back to the provided default
    target_function = os.getenv("TARGET_FUNCTION_NAME", "downloader")
    var_name = os.getenv("VAR_NAME", "DEPLOY_TS")

    # Get current env vars from target Lambda
    cfg = lambda_client.get_function_configuration(FunctionName=target_function)
    env_vars = (cfg.get("Environment", {}) or {}).get("Variables", {}) or {}

    # Generate new UTC timestamp
    new_value = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # Update the env var
    env_vars[var_name] = new_value
    lambda_client.update_function_configuration(
        FunctionName=target_function,
        Environment={"Variables": env_vars},
    )

    return {
        "updated": True,
        "function": target_function,
        "var": var_name,
        "value": new_value,
    }



