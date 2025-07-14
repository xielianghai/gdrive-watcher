import os
import time
import json
import threading
import requests
import mysql.connector
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime

# ====== Logging ======
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ====== Load Config ======
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
        WHERE s.api_credentials IS NOT NULL AND t.status = 'ACTIVE' AND t.tenant_type != 0
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    tenants = []
    for row in rows:
        (tenant_code, service_account_json, source_parent_folder_id,
         scan_interval, webhook_url, tenant_id) = row

        tenants.append({
            "tenant_code": tenant_code,
            "service_account_info": json.loads(str(service_account_json)),
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

def list_folders_and_files(service, folder_id, current_path, current_name):
    all_items = []

    # 查询当前文件夹下的文件
    query_files = (
        f"'{folder_id}' in parents and trashed = false and "
        "mimeType != 'application/vnd.google-apps.folder' and "
        "not appProperties has { key='processed' and value='true' }"
    )
    files = service.files().list(q=query_files, fields="files(id, name, mimeType, size, createdTime)").execute().get('files', [])
    for f in files:
        all_items.append({
            "type": "file",
            "file": f,
            "path": f"{current_path}/{f['name']}".strip("/"),
            "parent_folder_id": folder_id,
            "parent_folder_name": current_name
        })

    # 查询当前文件夹下的子文件夹
    query_folders = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed = false"
    folders = service.files().list(q=query_folders, fields="files(id, name)").execute().get('files', [])
    for folder in folders:
        sub_path = f"{current_path}/{folder['name']}".strip("/")
        all_items.extend(list_folders_and_files(service, folder['id'], sub_path, folder['name']))

    return all_items

def get_file_metadata(service, file_id):
    fields = "id, name, mimeType, size, createdTime, webViewLink, thumbnailLink, appProperties"
    file = service.files().get(fileId=file_id, fields=fields).execute()

    def mime_to_extension(mime_type):
        mapping = {
            'application/pdf': 'PDF',
            'image/jpeg': 'JPG',
            'image/png': 'PNG',
        }
        return mapping.get(mime_type, 'UNKNOWN')

    # 修正 createdTime 的格式
    raw_time = file.get("createdTime")
    upload_time = None
    if raw_time:
        try:
            # 自动剥离 Z 和毫秒，输出为 YYYY-MM-DD HH:MM:SS
            upload_time = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            # 若没有毫秒部分
            upload_time = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

    return {
        "id": file["id"],
        "name": file["name"],
        "file_type": mime_to_extension(file.get("mimeType", "")),
        "file_size": int(file.get("size", 0)),
        "upload_date": upload_time,
        "webViewLink": file.get("webViewLink"),
        "thumbnailLink": file.get("thumbnailLink"),
        "appProperties": file.get("appProperties", {})
    }

def mark_file_as_processed(service, file_id):
    service.files().update(
        fileId=file_id,
        body={"appProperties": {"processed": "true"}},
        fields="id"
    ).execute()
    logging.info(f"Marked file as processed: {file_id}")

# ====== Webhook processing ======
def process_file_with_n8n(service, file, file_path, tenant_settings, folder_id, folder_name):
    file_id = file['id']
    file_name = file['name']
    upload_method = 'GDrive'

    metadata = get_file_metadata(service, file_id)

    if metadata.get("appProperties", {}).get("processed") == "true":
        logging.info(f"[{tenant_settings['tenant_code']}] Already processed: {file_name}")
        return

    logging.info(f"[{tenant_settings['tenant_code']}] Start handle file: {file_name}")
    payload = {
        "tenantId": tenant_settings['tenant_id'],
        "fileId": file_id,
        "fileName": file_name,
        "filePath": file_path,
        "fileType": metadata.get("file_type"),
        "fileSize": metadata.get("file_size"),
        "uploadDate": metadata.get("upload_date"),
        "uploadMethod": upload_method,
        "folderId": folder_id,
        "folderName": folder_name,
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

    # 获取 source folder 的名字（用于 filePath 根目录）
    source_folder = service.files().get(fileId=parent_folder_id, fields="name").execute()
    source_name = source_folder.get("name", "root")

    while True:
        try:
            logging.info(f"[{tenant_code}] Scanning at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            items = list_folders_and_files(service, parent_folder_id, current_path=source_name, current_name=source_name)

            for item in items:
                if item["type"] == "file":
                    process_file_with_n8n(
                        service,
                        item["file"],
                        item["path"],
                        tenant_settings,
                        folder_id=item["parent_folder_id"],
                        folder_name=item["parent_folder_name"]
                    )

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
