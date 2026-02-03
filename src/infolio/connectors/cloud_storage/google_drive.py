import io
import os

from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.http import MediaIoBaseDownload

from infolio.utils import get_logger

logger = get_logger(__name__)

class GoogleDrive:
    """Connector for interacting with Google Drive via a service account."""

    def __init__(self, service_account_file: str|None = None) -> None:
        """
        Initialize the Google Drive connector.

        Parameters
        ----------
        service_account_file : str, optional
            Path to the service account JSON key file.
            If not provided, falls back to the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
        """
        self.service_account_file = service_account_file or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not self.service_account_file:
            raise ValueError("Service account file must be provided or set via GOOGLE_APPLICATION_CREDENTIALS.")

        self._scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.activity.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        self.service = self._authenticate()

    def _authenticate(self) -> Resource:
        """Authenticate the Google Drive service using the provided service account file."""
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=self._scopes
        )

        return build("drive", "v3", credentials=creds)

    def search(self, query: str) -> list:
        """
        Search for folders/files that the authenticated service account has access to based on the query parameter.

        Parameters
        ----------
        query : str
            Query string for filtering the file search.

        Returns
        -------
        list
            A list of dictionaries containing the file metadata found through the search query.
        """
        files = []
        page_token = None
        while True:
            response = (
                self.service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="nextPageToken, files(id, name, mimeType, size)",
                    pageToken=page_token
                ).execute()
            )
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        return files

    def read_file(self, file_id: str) -> bytes:
        """
        Download a file from Google Drive and return as a BytesIO stream.

        For Google Sheets, exports the file as Excel format.
        The MIME type is automatically determined from the file metadata.

        Parameters
        ----------
        file_id : str
            The Google Drive file ID

        Returns
        -------
        bytes
            File content as bytes.
        """
        # Get file metadata to determine MIME type
        file_metadata = self.service.files().get(
            fileId=file_id,
            fields="mimeType"
        ).execute()

        mime_type = file_metadata.get("mimeType")

        # For Google Sheets, we need to export instead of download
        if mime_type == "application/vnd.google-apps.spreadsheet":
            request = self.service.files().export_media(
                fileId=file_id,
                mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            request = self.service.files().get_media(fileId=file_id)

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()

        return file.getvalue()

    def create_folder(self, folder_name: str, parent_folder_id: str) -> str:
        """
        Create a folder in Google Drive under the specified parent folder.

        If a folder with the same name already exists, returns its ID.

        Parameters
        ----------
        folder_name : str
            Name of the folder to create.
        parent_folder_id : str
            Google Drive parent folder ID where the folder should be created.

        Returns
        -------
        str
            The Google Drive folder ID of the created or existing folder.
        """
        logger.info(f"Creating folder '{folder_name}' in parent folder: {parent_folder_id}")

        # Check if folder already exists
        resp = self.service.files().list(
            q=(
                f"'{parent_folder_id}' in parents "
                f"and name = '{folder_name}' "
                "and mimeType = 'application/vnd.google-apps.folder' "
                "and trashed = false"
            ),
            spaces="drive",
            fields="files(id, name)",
        ).execute()

        files = resp.get("files", [])
        if files:
            folder_id = files[0]["id"]
            logger.info(f"Folder '{folder_name}' already exists with ID: {folder_id}")
            return folder_id

        # Create the folder
        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        created = self.service.files().create(body=metadata, fields="id").execute()
        folder_id = created["id"]
        logger.info(f"Folder '{folder_name}' created with ID: {folder_id}")
        return folder_id

    def find_or_create_subfolder(self, parent_folder_id: str, subfolder_name: str) -> str:
        """
        Find a subfolder by name under a parent folder, or create it if missing.

        Parameters
        ----------
        parent_folder_id : str
            Google Drive folder ID of the parent folder.
        subfolder_name : str
            Name of the subfolder to find or create.

        Returns
        -------
        str
            The Google Drive folder ID of the subfolder.
        """
        logger.info(f"Finding or creating subfolder '{subfolder_name}' in parent folder: {parent_folder_id}")

        resp = self.service.files().list(
            q=(
                f"'{parent_folder_id}' in parents "
                f"and name = '{subfolder_name}' "
                "and mimeType = 'application/vnd.google-apps.folder' "
                "and trashed = false"
            ),
            spaces="drive",
            fields="files(id, name)",
        ).execute()

        files = resp.get("files", [])
        if files:
            folder_id = files[0]["id"]
            logger.info(f"Subfolder '{subfolder_name}' already exists with ID: {folder_id}")
            return folder_id

        metadata = {
            "name": subfolder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        created = self.service.files().create(body=metadata, fields="id").execute()
        folder_id = created["id"]
        logger.info(f"Subfolder '{subfolder_name}' created with ID: {folder_id}")
        return folder_id
