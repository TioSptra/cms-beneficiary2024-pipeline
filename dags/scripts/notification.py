import logging
import os
from dotenv import load_dotenv

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)
load_dotenv(dotenv_path=os.path.join("/opt/airflow/keys", "key_discord.env"))

def discord_notification(context):
    import requests

    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    logical_date = context.get("data_interval_start").strftime("%Y-%m-%d %H:%M:%S")
    task_instance_state = task_instance.state

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    message = f"DAG: {dag_id}\nTask: {task_id}\nExecution Time: {logical_date}"
    if task_instance_state == "success":
        message += "\nStatus: ✅ Success \n"
    elif task_instance_state == "failed":
        message += "\nStatus: ❌ Failed \n"
    elif task_instance_state == "skipped":
        message += "\nStatus: ⏭️ Skipped \n"
    elif task_instance_state == "up_for_retry":
        message += "\nStatus: 🔄 Up for Retry \n"
    elif task_instance_state == "up_for_reschedule":
        message += "\nStatus: ⏳ Up for Reschedule \n"
    else:
        message += "\nStatus: ❓ Unknown \n"

    response = requests.post(webhook_url, json={"content": message})

    if response.status_code != 204:
        logging.error(f"Failed to send Discord notification: {response.status_code} - {response.text}")