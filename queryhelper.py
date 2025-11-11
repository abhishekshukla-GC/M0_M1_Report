import snowflake.connector as sc
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import tempfile

# Try to import streamlit for secrets management
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    st = None

def get_decryption_key():
    """Get decryption key from Streamlit secrets or environment variable."""
    if STREAMLIT_AVAILABLE:
        try:
            return st.secrets.get("encryption_key", None)
        except (AttributeError, FileNotFoundError, KeyError):
            pass
    
    # Fall back to environment variable
    return os.getenv("ENCRYPTION_KEY")

def decrypt_file(encrypted_path: Path, key: str) -> bytes:
    """Decrypt a file using Fernet encryption."""
    if not encrypted_path.exists():
        return None
    
    try:
        if isinstance(key, str):
            key_bytes = key.encode()
        else:
            key_bytes = key
        
        fernet = Fernet(key_bytes)
        
        with open(encrypted_path, 'rb') as f:
            encrypted_data = f.read()
        
        decrypted_data = fernet.decrypt(encrypted_data)
        return decrypted_data
    except Exception as e:
        print(f"Warning: Failed to decrypt {encrypted_path.name}: {e}")
        return None

def get_credential_file_path(filename: str):
    """
    Get the path to a credential file, checking for encrypted version first.
    Returns (file_path, is_encrypted).
    """
    encrypted_path = Path(f"{filename}.encrypted")
    plain_path = Path(filename)
    
    if encrypted_path.exists():
        return (encrypted_path, True)
    elif plain_path.exists():
        return (plain_path, False)
    else:
        return (None, False)

def load_credential_file(filename: str) -> Path:
    """
    Load a credential file, decrypting if necessary.
    Returns path to the file (may be temporary if decrypted).
    """
    file_path, is_encrypted = get_credential_file_path(filename)
    
    if file_path is None:
        return None
    
    if not is_encrypted:
        # Plain file, return as-is
        return file_path
    
    # Encrypted file, need to decrypt
    key = get_decryption_key()
    if not key:
        if STREAMLIT_AVAILABLE:
            try:
                st.warning(f"⚠️ Encrypted file `{filename}.encrypted` found but no decryption key in Streamlit secrets.")
                st.info("💡 Add `encryption_key` to Streamlit Cloud secrets or `.streamlit/secrets.toml` for local development.")
            except:
                pass
        print(f"Warning: Encrypted file {filename}.encrypted found but no decryption key available.")
        print("Falling back to plain file if it exists...")
        plain_path = Path(filename)
        if plain_path.exists():
            return plain_path
        return None
    
    decrypted_data = decrypt_file(file_path, key)
    if decrypted_data is None:
        # Try plain file as fallback
        plain_path = Path(filename)
        if plain_path.exists():
            return plain_path
        return None
    
    # Write decrypted data to temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=f"_{filename}")
    temp_file.write(decrypted_data)
    temp_file.close()
    return Path(temp_file.name)

def get_env_var(key: str, default: str = None) -> str:
    """
    Get environment variable with priority:
    1. Streamlit secrets (if available)
    2. Environment variable
    3. Default value
    """
    if STREAMLIT_AVAILABLE:
        try:
            # Try to get from secrets (supports nested keys with dot notation)
            secrets = st.secrets
            for part in key.lower().split('.'):
                secrets = secrets.get(part, {})
            if isinstance(secrets, str):
                return secrets
        except (AttributeError, FileNotFoundError, KeyError, TypeError):
            pass
    
    return os.getenv(key, default)

def load_encrypted_env():
    """Decrypt and load .env.encrypted file if it exists."""
    env_encrypted = Path(".env.encrypted")
    env_plain = Path(".env")
    
    # If plain .env exists, use it (for local dev)
    if env_plain.exists():
        load_dotenv(env_plain)
        return
    
    # If encrypted .env exists, decrypt and load it
    if env_encrypted.exists():
        key = get_decryption_key()
        if not key:
            # Don't use st.error here as Streamlit may not be initialized at import time
            print("⚠️ Found .env.encrypted but no decryption key.")
            print("💡 Add 'encryption_key' to Streamlit Cloud secrets or set ENCRYPTION_KEY environment variable.")
            return
        
        try:
            decrypted_data = decrypt_file(env_encrypted, key)
            if decrypted_data:
                # Parse the decrypted .env content and set environment variables
                env_content = decrypted_data.decode('utf-8')
                for line in env_content.splitlines():
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    # Parse KEY=VALUE format
                    if '=' in line:
                        key_part, value_part = line.split('=', 1)
                        key_part = key_part.strip()
                        value_part = value_part.strip()
                        # Remove quotes if present
                        if value_part.startswith('"') and value_part.endswith('"'):
                            value_part = value_part[1:-1]
                        elif value_part.startswith("'") and value_part.endswith("'"):
                            value_part = value_part[1:-1]
                        # Set environment variable
                        os.environ[key_part] = value_part
                return
        except Exception as e:
            # Don't use st.error here as Streamlit may not be initialized at import time
            print(f"⚠️ Failed to decrypt .env.encrypted: {e}")
            print("💡 Verify your encryption_key in Streamlit secrets matches the one used to encrypt the file.")
    
    # Fallback: try loading plain .env if it exists
    if env_plain.exists():
        load_dotenv(env_plain)

# Load environment variables (from encrypted or plain .env)
load_encrypted_env()

def sfFetch(query): 
    df=pd.DataFrame()
    conn_params = {
        'account': get_env_var('SNOWFLAKE_ACCOUNT') or os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': get_env_var('SNOWFLAKE_USER') or os.getenv('SNOWFLAKE_USER'),
        'warehouse': get_env_var('SNOWFLAKE_WAREHOUSE') or os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': get_env_var('SNOWFLAKE_DATABASE') or os.getenv('SNOWFLAKE_DATABASE'),
        'schema': get_env_var('SNOWFLAKE_SCHEMA') or os.getenv('SNOWFLAKE_SCHEMA')}
    
    # Handle private key file (may be encrypted)
    private_key_file = get_env_var('SNOWFLAKE_PRIVATE_KEY_FILE') or os.getenv('SNOWFLAKE_PRIVATE_KEY_FILE')
    if private_key_file:
        key_file_path = load_credential_file(private_key_file)
        if key_file_path:
            conn_params['private_key_file'] = str(key_file_path)
        else:
            conn_params['private_key_file'] = private_key_file
    
    conn_params['private_key_file_pwd'] = get_env_var('SNOWFLAKE_PRIVATE_KEY_FILE_PWD') or os.getenv('SNOWFLAKE_PRIVATE_KEY_FILE_PWD')
    
    ctx = sc.connect(**conn_params)
    cs = ctx.cursor()
    try:
        cs.execute(query)
        results = cs.fetchall()
        columns = [desc[0] for desc in cs.description]  
        df = pd.DataFrame(results, columns=columns)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cs.close()
        ctx.close()

    return df
    
import pygsheets

# Load credentials.json (may be encrypted)
_credentials_path = load_credential_file('credentials.json')
if _credentials_path:
    gc = pygsheets.authorize(service_file=str(_credentials_path))
else:
    # Fallback to plain file or try to get from secrets
    if STREAMLIT_AVAILABLE:
        try:
            # Try to get credentials from secrets as JSON string
            creds_json = st.secrets.get("google_credentials", {})
            if creds_json:
                # Write to temporary file
                import json
                temp_creds = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_credentials.json')
                json.dump(creds_json, temp_creds)
                temp_creds.close()
                gc = pygsheets.authorize(service_file=temp_creds.name)
            else:
                gc = pygsheets.authorize(service_file='credentials.json')
        except (AttributeError, FileNotFoundError, KeyError):
            gc = pygsheets.authorize(service_file='credentials.json')
    else:
        gc = pygsheets.authorize(service_file='credentials.json')




def read_google_sheet(sheet_link, sheet_name=None):
    """
    Read data from a specified Google Sheet and convert it into a DataFrame.
    If no sheet name is provided, read all sheets in the workbook.

    :param sheet_link: Link to the Google Sheet
    :param sheet_name: Name of the specific sheet to read (optional)
    :return: DataFrame containing the sheet data or a dictionary of DataFrames for all sheets
    """
    sh = gc.open_by_url(sheet_link)
    
    # If no sheet name is provided, read all sheets
    if sheet_name is None:
        all_sheets = {}
        for wks in sh.worksheets():
            data = wks.get_all_values()
            if not data:
                all_sheets[wks.title] = pd.DataFrame()
            else:
                df = pd.DataFrame(data[1:], columns=data[0])
                df.columns = df.columns.str.strip()
                df.replace('', pd.NA, inplace=True)
                df.dropna(axis=1, how='all', inplace=True)
                df.dropna(axis=0, how='all', inplace=True)
                empty_header_cols = df.columns == ''
                df.drop(columns=df.columns[empty_header_cols], inplace=True)
                all_sheets[wks.title] = df
        return all_sheets

    # If a specific sheet name is provided
    wks = sh.worksheet_by_title(sheet_name)
    data = wks.get_all_values()
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data[1:], columns=data[0])
    df.columns = df.columns.str.strip()
    df.replace('', pd.NA, inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)
    empty_header_cols = df.columns == ''
    df.drop(columns=df.columns[empty_header_cols], inplace=True)

    return df


def upsert_to_google_sheet(sheet_link: str, sheet_name: str, df: pd.DataFrame):
    sh = gc.open_by_url(sheet_link)
    try:
        wks = sh.worksheet("title", sheet_name)
    except pygsheets.WorksheetNotFound:
        wks = sh.add_worksheet(sheet_name, rows=df.shape[0] + 1, cols=df.shape[1])

    # read everything in the sheet
    all_vals = wks.get_all_values(returnas="matrix")
    # if empty or only headers, do a full write
    if len(all_vals) <= 1 or (len(all_vals) == 1 and all_vals[0] == [""] * len(all_vals[0])):
        wks.clear()
        wks.set_dataframe(df, (1, 1))
        return f"Sheet '{sheet_name}' was empty—wrote full DataFrame."

    # otherwise append below existing data
    start_row = len(all_vals) + 1
    needed_rows = start_row - wks.rows
    needed_cols = df.shape[1] - wks.cols

    if needed_rows > 0:
        wks.add_rows(needed_rows)
    if needed_cols > 0:
        wks.add_cols(needed_cols)

    # **Use set_dataframe with extend=True to append**
    wks.set_dataframe(
        df,
        (start_row, 1),      # row, col
        copy_index=False,
        copy_head=False,
        extend=True
    )
    return f"Sheet '{sheet_name}' had existing data—appended {len(df)} rows."







def write_to_google_sheet(sheet_link, sheet_name, df):
    """
    Write a DataFrame to a specified Google Sheet using pygsheets.

    :param sheet_link: Link to the Google Sheet
    :param sheet_name: Name of the specific sheet to write to
    :param df: DataFrame to write
    :return: Confirmation message
    """
    sh = gc.open_by_url(sheet_link)
    
    try:
        # Try to get the worksheet by title
        wks = sh.worksheet('title', sheet_name)
    except pygsheets.WorksheetNotFound:
        rows, cols = df.shape
        wks = sh.add_worksheet(sheet_name, rows=rows + 1, cols=cols)

    wks.clear()
    
    wks.set_dataframe(df, (1, 1))  
    return f"DataFrame written to sheet '{sheet_name}'."


def append_to_google_sheet(sheet_link, sheet_name, df):
    """
    Append a DataFrame as new rows in a specified Google Sheet.

    :param sheet_link: Link to the Google Sheet
    :param sheet_name: Name of the specific sheet to append to
    :param df: DataFrame to append
    :return: Confirmation message
    """
    sh = gc.open_by_url(sheet_link)
    wks = sh.worksheet_by_title(sheet_name)

    last_row = len(wks.get_all_values())
    
    wks.append_table(df.values.tolist(), start='A' + str(last_row + 1))
    return f"DataFrame appended to sheet '{sheet_name}'."

from sqlalchemy import create_engine
import pandas as pd


def fetch_query_results(query):
    """
    Connects to a PostgreSQL database and executes the provided query.

    Parameters:
        query (str): The SQL query to execute.
        db_url (str): The database connection URL.

    Returns:
        pd.DataFrame: The results of the query as a DataFrame.
    """
    db_url = get_env_var('fetch_query_url') or os.getenv('fetch_query_url')

    try:
        if not db_url:
            print("Warning: fetch_query_url not configured")
            return pd.DataFrame()
        engine = create_engine(db_url)
        df = pd.read_sql_query(query, engine)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def fetch_catalog_results(query):
    """
    Connects to a PostgreSQL database and executes the provided query.

    Parameters:
        query (str): The SQL query to execute.
        db_url (str): The database connection URL.

    Returns:
        pd.DataFrame: The results of the query as a DataFrame.
    """
    db_url = get_env_var('catalog_url') or os.getenv('catalog_url')

    try:
        if not db_url:
            print("Warning: catalog_url not configured")
            return pd.DataFrame()
        engine = create_engine(db_url)
        df = pd.read_sql_query(query, engine)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def fetchcms(query):
    """
    Connects to a PostgreSQL database and executes the provided query.

    Parameters:
        query (str): The SQL query to execute.
        db_url (str): The database connection URL.

    Returns:
        pd.DataFrame: The results of the query as a DataFrame.

    """
    db_url = get_env_var('cms_url') or os.getenv('cms_url')

    try:
        if not db_url:
            print("Warning: cms_url not configured")
            return pd.DataFrame()
        engine = create_engine(db_url)
        df = pd.read_sql_query(query, engine)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()



import requests
from bs4 import BeautifulSoup
import calendar

class IndianFestivals:
    """
    Scrape all major Indian festivals for a given year and return them
    as a pandas DataFrame with columns 'date' and 'festival_name'.
    """

    def __init__(self, year: int):
        self.year = year
        url = (
            f"https://panchang.astrosage.com/calendars/indiancalendar"
            f"?language=en&date={year}"
        )
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        # each table corresponds to one month of festivals
        self.festival_tables = soup.find_all('table')

    def get_festivals_in_a_year(self, month: int = None) -> pd.DataFrame:
        """
        Return a DataFrame of festivals for the year (or specific month).

        Parameters
        ----------
        month : int, optional
            If provided (1-12), only festivals from that month are returned.

        Returns
        -------
        pd.DataFrame
            Columns:
            - date           : datetime of the festival
            - festival_name  : name of the festival
        """
        records = []

        # list of month names for filtering
        month_names = list(calendar.month_name)[1:]  # ['January', 'February', ...]

        filter_name = None
        if month is not None:
            if not (1 <= month <= 12):
                raise ValueError("Month should be between 1 and 12")
            filter_name = month_names[month - 1]

        for table in self.festival_tables:
            # extract month name from table header
            header = table.find('thead').find('th').get_text(strip=True)
            table_month = header.split()[0]
            if filter_name and table_month != filter_name:
                continue

            # iterate each festival row
            for row in table.find('tbody').find_all('tr'):
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue
                date_part = cols[0].get_text(strip=True).split()[0]
                name = cols[1].get_text(strip=True)
                # build full date string and parse
                full_date = f"{date_part} {table_month} {self.year}"
                try:
                    dt = pd.to_datetime(full_date, format="%d %B %Y")
                except Exception:
                    # skip invalid parses
                    continue
                records.append({
                    'date': dt,
                    'festival_name': name
                })

        df = pd.DataFrame(records)
        if df.empty:
            return df
        return df.sort_values('date').reset_index(drop=True)
    

import re

def get_india_gov_festivals(year: int) -> pd.DataFrame:
    """
    Scrape india.gov.in calendar pages for a full year and return
    a DataFrame of (date, festival_name).
    """
    records = []
    for m in range(1, 13):
        url = f"https://www.india.gov.in/calendar?date={year}-{m:02d}"
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # look for every <td class="single-day" ... data-date="YYYY-MM-DD">
        for td in soup.find_all("td", class_=re.compile(r"\bsingle-day\b")):
            date_str = td.get("data-date")
            if not date_str:
                continue

            for fest_div in td.find_all("div", class_=re.compile(r".*Cal$")):
                name = fest_div.get_text(strip=True)
                if name:
                    records.append({
                        "date": pd.to_datetime(date_str),
                        "festival_name": name
                    })

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("date").reset_index(drop=True)
    return df
