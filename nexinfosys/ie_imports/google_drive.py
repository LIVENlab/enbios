from __future__ import print_function

import io
import logging
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.readonly']


def export_xlsx_to_memory(service, file_id):
    def process_request(request):
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logging.debug("Download %d%%." % int(status.progress() * 100))
        return fh

    try:
        request = service.files().get_media(fileId=file_id)
        return process_request(request)
    except HttpError as e:
        try:
            request = service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            return process_request(request)
        except HttpError as e:
            return None


def create_service(credentials_file, scopes, pickled_token_file):
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(pickled_token_file):
        with open(pickled_token_file, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(pickled_token_file, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def download_xlsx_file_id(credentials_path, token_path, file_id):
    service = create_service(credentials_path, SCOPES, token_path)
    return export_xlsx_to_memory(service, file_id)


def main():
    # How to get your "credentials.json" file:
    #
    # * Go to http://console.developers.google.com/apis/library
    # * Create project
    # * Go to "APIs y servicios" -> "Credenciales"
    # * Select "Crear credenciales" -> "Ayúdame a elegir"
    # * Select
    # ** ¿Qué API estás utilizando?: Google Drive API
    # ** ¿Desde dónde llamarás a la API?: Otra UI
    # ** ¿A qué tipo de datos accederás?: Datos de usuario


    # File ID. Go to any Google Calc or XLSX (open it converting to Google Calc first), then take the file ID from the
    # URL. It has 44 or 33 characters long.
    file_id = '--------------------------------------------'
    file_id = '---------------------------------'
    fh = download_xlsx_file_id('/home/rnebot/Dropbox/nis-backend-config/credentials.json', '/home/rnebot/Dropbox/nis-backend-config/token.pickle', file_id)
    if fh:
        with open("/home/rnebot/d.xlsx", 'wb') as out:
            out.write(fh.getvalue())
    else:
        print("Download failed!")


if __name__ == '__main__':
    main()
