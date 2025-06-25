import os
import time
import json
import threading
import requests
import mysql.connector
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ====== Logging ======
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ====== Load Config (只包含 mysql 和 proxy 配置) ======
with open('config.json', 'r') as f:
    CONFIG = json.load(f)

# ====== Set Proxy ======
if CONFIG.get('use_proxy', False):
    os.environ['http_proxy'] = CONFIG.get('http_proxy', '')
    os.environ['https_proxy'] = CONFIG.get('https_proxy', '')
    logging.info("HTTP proxy enabled.")
else:
    logging.info("HTTP proxy disabled.")

MYSQL_CONFIG = CONFIG['mysql']

# ====== MySQL Operations ======

def get_all_tenant_settings():
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()
    query = """
        SELECT t.tenant_code,
               s.api_credentials, 
               s.gdrive_source_parent_folder_id, 
               s.gdrive_scan_interval, 
               s.web_hook_url, 
               t.id as tenant_id 
        FROM smepaf.tenant_settings s 
        JOIN smepaf.tenants t ON s.tenant_id = t.id
        WHERE s.api_credentials IS NOT NULL AND t.status = 'ACTIVE'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    tenants = []
    for row in rows:
        (tenant_code,
         service_account_json, 
         source_parent_folder_id, 
         scan_interval, 
         webhook_url, 
         tenant_id) = row

        tenants.append({
            "tenant_code": tenant_code,
            "service_account_info": json.loads(service_account_json),
            "source_parent_folder_id": source_parent_folder_id,
            "scan_interval": scan_interval or 60,
            "webhook_url": webhook_url,
            "tenant_id": tenant_id
        })
    return tenants

# ====== Google Drive API ======

def authenticate_google_drive(service_account_info):
    creds = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def list_subfolders(service, parent_folder_id):
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def list_files_in_folder(service, folder_id):
    query = (
        f"'{folder_id}' in parents and trashed = false and "
        "mimeType != 'application/vnd.google-apps.folder' and "
        "not appProperties has {{ key='processed' and value='true' }}"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def get_file_metadata(service, file_id):
    fields = "id, name, webViewLink, thumbnailLink, appProperties"
    file = service.files().get(fileId=file_id, fields=fields).execute()
    return file

def mark_file_as_processed(service, file_id):
    service.files().update(
        fileId=file_id,
        body={"appProperties": {"processed": "true"}},
        fields="id"
    ).execute()
    logging.info(f"Marked file as processed: {file_id}")

# ====== Webhook processing ======

def process_file_with_n8n(service, file, folder, tenant_settings):
    file_id = file['id']
    file_name = file['name']

    metadata = get_file_metadata(service, file_id)
    # Defensive check: In theory, it will not be triggered because it is excluded in list()
    if metadata.get("appProperties", {}).get("processed") == "true":
        logging.info(f"[{tenant_settings['tenant_code']}] Already processed: {file_name}")
        return

    payload = {
        "tenantId": tenant_settings['tenant_id'],
        "fileId": file_id,
        "fileName": file_name,
        "folderId": folder['id'],
        "folderName": folder['name'],
        "webViewLink": metadata.get("webViewLink"),
        "thumbnailLink": metadata.get("thumbnailLink")
    }

    try:
        response = requests.post(tenant_settings['webhook_url'], json=payload)
        response.raise_for_status()
        logging.info(f"[{tenant_settings['tenant_code']}] Webhook success: {file_name}")
        mark_file_as_processed(service, file_id)
    except Exception as e:
        logging.error(f"[{tenant_settings['tenant_code']}] Webhook failed for {file_name}: {e}")

# ====== Per-Tenant Scan Loop ======

def scan_loop(tenant_settings):
    tenant_code = tenant_settings['tenant_code']
    logging.info(f"Starting scan thread for tenant {tenant_code}")

    service = authenticate_google_drive(tenant_settings['service_account_info'])
    scan_interval = tenant_settings['scan_interval']
    parent_folder_id = tenant_settings['source_parent_folder_id']

    while True:
        try:
            logging.info(f"[{tenant_code}] Scanning at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            subfolders = list_subfolders(service, parent_folder_id)

            for folder in subfolders:
                logging.info(f"[{tenant_code}] Subfolder: {folder['name']}")
                files = list_files_in_folder(service, folder['id'])

                if not files:
                    continue

                for file in files:
                    process_file_with_n8n(service, file, folder, tenant_settings)

            logging.info(f"[{tenant_code}] Waiting {scan_interval} seconds...")
            time.sleep(scan_interval)

        except Exception as e:
            logging.error(f"[{tenant_code}] Fatal error in scan loop: {e}")
            time.sleep(scan_interval)

# ====== Entry Point ======

if __name__ == '__main__':
    tenants = get_all_tenant_settings()
    if not tenants:
        logging.error("No tenant settings found. Exiting.")
        exit(1)

    for tenant_settings in tenants:
        thread = threading.Thread(target=scan_loop, args=(tenant_settings,))
        thread.daemon = True
        thread.start()

    while True:
        time.sleep(3600)
