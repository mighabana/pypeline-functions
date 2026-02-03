import os

import google.auth
import gspread
import polars as pl
from googleapiclient.discovery import build
from gspread.utils import rowcol_to_a1
from gspread_dataframe import set_with_dataframe
from gspread_formatting import CellFormat, NumberFormat, TextFormat, format_cell_range

from infolio.utils import get_logger

logger = get_logger(__name__)


class GoogleSheets:
    """
    Connector for interacting with Google Sheets via service account.

    This connector provides functionality to create, find, and manage
    Google Sheets spreadsheets and worksheets.
    """

    def __init__(self, service_account_file: str|None = None) -> None:
        """
        Initialize the Google Sheets connector.

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
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        self._credentials, _ = google.auth.default(scopes=self._scopes)
        self.gc = gspread.authorize(self._credentials)
        self.drive_service = build("drive", "v3", credentials=self._credentials)
        self.sheets_service = build("sheets", "v4", credentials=self._credentials)

    def find_or_create_spreadsheet(
        self, sheet_name: str, folder_id: str
    ) -> gspread.Spreadsheet:
        """
        Find an existing spreadsheet by name in a folder, or create a new one.

        Parameters
        ----------
        sheet_name : str
            Name of the spreadsheet to find or create.
        folder_id : str
            Google Drive folder ID where the spreadsheet should be located.

        Returns
        -------
        gspread.Spreadsheet
            The found or newly created spreadsheet.
        """
        search = self.drive_service.files().list(
            q=(
                f"name = '{sheet_name}' "
                f"and '{folder_id}' in parents "
                f"and mimeType = 'application/vnd.google-apps.spreadsheet'"
            ),
            spaces="drive",
            fields="files(id, name)",
        ).execute()

        if search.get("files"):
            spreadsheet = self.gc.open_by_key(search["files"][0]["id"])
            logger.info(f"Found existing spreadsheet: {sheet_name}")
            return spreadsheet

        spreadsheet = self.gc.create(sheet_name)
        self.drive_service.files().update(
            fileId=spreadsheet.id,
            addParents=folder_id,
            removeParents="root",
            fields="id, parents",
        ).execute()
        logger.info(f"Created new spreadsheet: {sheet_name}")
        return spreadsheet

    def delete_spreadsheet(self, sheet_name: str, folder_id: str) -> None:
        """
        Delete an existing spreadsheet by name in a folder.

        Parameters
        ----------
        sheet_name : str
            Name of the spreadsheet to delete.
        folder_id : str
            Google Drive folder ID where the spreadsheet is located.
        """
        search = self.drive_service.files().list(
            q=(
                f"name = '{sheet_name}' "
                f"and '{folder_id}' in parents "
                f"and mimeType = 'application/vnd.google-apps.spreadsheet' "
                "and trashed = false"
            ),
            spaces="drive",
            fields="files(id, name)",
        ).execute()

        for f in search.get("files", []):
            logger.info(f"Deleting existing file: {f['name']} (ID: {f['id']})")
            self.drive_service.files().delete(fileId=f["id"]).execute()

    def bold_headers(self, worksheet: gspread.Worksheet, num_columns: int) -> None:
        """
        Make the first row (headers) bold.

        Parameters
        ----------
        worksheet : gspread.Worksheet
            The worksheet to style.
        num_columns : int
            Number of columns to format in the header row.
        """
        end_a1 = rowcol_to_a1(1, num_columns)
        header_range = f"A1:{end_a1}"
        format_cell_range(worksheet, header_range, CellFormat(textFormat=TextFormat(bold=True)))
        logger.debug(f"Bolded headers in range: {header_range}")

    def normalize_percentage_values(self, worksheet: gspread.Worksheet, range_str: str) -> None:
        """
        Normalize percentage-like text values in a range to numeric values.

        Strips '%' and commas, then converts to decimal ratios (e.g., "50%" -> 0.5).

        Parameters
        ----------
        worksheet : gspread.Worksheet
            The worksheet containing the values.
        range_str : str
            A1 notation range (e.g., "K2:M10") to normalize.
        """
        rows = worksheet.get(range_str)
        scaled = []
        for r in rows:
            new_r = []
            for cell in r:
                if cell is None or str(cell).strip() == "":
                    new_r.append("")
                    continue
                s = str(cell).strip().replace(",", "").replace("%", "")
                try:
                    new_r.append(float(s) / 100.0)
                except ValueError:
                    new_r.append(cell)
            scaled.append(new_r)

        worksheet.update(values=scaled, range_name=range_str, value_input_option="RAW")
        logger.debug(f"Normalized percentage values in range: {range_str}")

    def format_number_range(
        self,
        worksheet: gspread.Worksheet,
        range_str: str,
        number_format_pattern: str = '[Green]#,##0.##%;[Red]-#,##0.00%;""',
    ) -> None:
        """
        Format a range of cells with number formatting and optional color coding.

        Parameters
        ----------
        worksheet : gspread.Worksheet
            The worksheet to style.
        range_str : str
            A1 notation range (e.g., "K2:M10") to format.
        number_format_pattern : str, optional
            Number format pattern (e.g., '[Green]#,##0.##%;[Red]-#,##0.00%;""').
            Defaults to percentage format with green/red colors.
        """
        format_cell_range(
            worksheet,
            range_str,
            CellFormat(
                numberFormat=NumberFormat(
                    type="NUMBER",
                    pattern=number_format_pattern,
                )
            ),
        )
        logger.debug(f"Formatted number range: {range_str} with pattern: {number_format_pattern}")

    def resize_columns(
        self, worksheet: gspread.Worksheet, spreadsheet_id: str, num_columns: int
    ) -> None:
        """
        Auto-resize columns in a worksheet to fit their content.

        Parameters
        ----------
        worksheet : gspread.Worksheet
            The worksheet to resize.
        spreadsheet_id : str
            The spreadsheet ID containing the worksheet.
        num_columns : int
            Number of columns to resize (starting from column 0).
        """
        requests = [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": num_columns,
                    }
                }
            }
        ]
        self.sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()
        logger.debug(f"Resized {num_columns} columns in worksheet: {worksheet.title}")

    def df_to_gsheet(
        self,
        df: pl.DataFrame,
        folder_id: str,
        sheet_name: str,
        replace: bool = False
    ) -> bool:
        """
        Write a Polars DataFrame to a Google Sheet.

        Creates a new spreadsheet in the specified folder, or updates an existing
        one if `replace=True`.

        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame to write to Google Sheets.
        folder_id : str
            Google Drive folder ID where the spreadsheet should be located.
        sheet_name : str
            Name of the spreadsheet to create or update.
        replace : bool, optional
            If True, replace the contents of an existing spreadsheet with the
            same name. If False and the spreadsheet exists, returns False without
            making changes. Defaults to False.

        Returns
        -------
        bool
            True if the DataFrame was successfully written, False otherwise.
        """
        if df.is_empty():
            logger.warning("Empty dataframe. Google Sheet was not created.")
            return False

        response = self.drive_service.files().list(
            q=f"""
                name = '{sheet_name}'
                and '{folder_id}' in parents
                and mimeType='application/vnd.google-apps.spreadsheet'
            """,
            spaces="drive",
            fields="files(id, name)"
        ).execute()

        # Convert Polars to Pandas for gspread_dataframe compatibility
        # Cast all columns to string to ensure proper conversion
        df_str = df.cast(dict.fromkeys(df.columns, pl.Utf8))
        pandas_df = df_str.to_pandas()

        for file in response.get("files", []):
            if replace:
                sh = self.gc.open_by_key(file["id"])
                worksheet = sh.get_worksheet(0)
                worksheet.clear()
                set_with_dataframe(worksheet, pandas_df)
                logger.info(f"Replaced contents of spreadsheet: {sheet_name}")
                return True
            else:
                logger.warning("File already exists. If you wish to replace the file content set `replace=True`.")
                return False

        sh = self.gc.create(sheet_name)
        file_id = sh.id
        self.drive_service.files().update(
            fileId=file_id,
            addParents=folder_id,
            removeParents="root",
            fields="id, parents"
        ).execute()

        worksheet = sh.get_worksheet(0)
        set_with_dataframe(worksheet, pandas_df)
        logger.info(f"Created new spreadsheet: {sheet_name}")

        return True

    def gsheet_to_df(
        self,
        folder_id: str,
        sheet_name: str,
        schema: dict[str, pl.DataType] | None = None
    ) -> pl.DataFrame:
        """
        Read a Google Sheet into a Polars DataFrame.

        Parameters
        ----------
        folder_id : str
            Google Drive folder ID where the spreadsheet is located.
        sheet_name : str
            Name of the spreadsheet to read.
        schema : dict[str, pl.DataType], optional
            A dictionary mapping column names to Polars data types for schema
            enforcement. If not provided, types are inferred automatically.

        Returns
        -------
        pl.DataFrame
            The spreadsheet data as a Polars DataFrame. Returns an empty
            DataFrame if the spreadsheet is not found.

        """
        response = self.drive_service.files().list(
            q=f"""
                name = '{sheet_name}'
                and '{folder_id}' in parents
                and mimeType='application/vnd.google-apps.spreadsheet'
            """,
            spaces="drive",
            fields="files(id, name)"
        ).execute()

        files = response.get("files", None)

        if files:
            for file in files:
                sh = self.gc.open_by_key(file["id"])
                worksheet = sh.get_worksheet(0)
                records = worksheet.get_all_records()

                if not records:
                    return pl.DataFrame()

                df = pl.DataFrame(records)

                if schema:
                    df = df.cast(schema)

                return df

        return pl.DataFrame()
