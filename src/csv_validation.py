# %%
import errno
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
import os
from table_to_csv import TableToCsv

dotenv_path = os.path.join(os.path.dirname(__file__)+"/../", '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

conn = sqlalchemy.create_engine(
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/antimicrobial_system")

table = TableToCsv(conn, 1).table


# %%
temp_df = pd.read_csv("../upload_file/Test_data_complete_with_vitek.csv")

# %%
"""Validate Amount minimun."""


def validate_amount(df: pd.DataFrame):

    ROW_MINIMUN = 100
    if len(df) < ROW_MINIMUN:
        return [f"File must have minimun {ROW_MINIMUN} row."]
    else:
        return []

##########################################


"""Validate Columns."""


def validate_columns(df: pd.DataFrame):
    columns_require = ["hn", "date_of_submission", "report_issued_date",
                       "species", "bacteria_genus", "submitted_sample"]

    columns_require_startwith = ["S/I/R_", "ans_"]

    error = []
    cols = set(df.columns)
    cols_remainer = cols - set(columns_require)
    if len(cols_remainer) != len(cols) - len(columns_require):
        error.append("File columns must have " +
                     ", ".join([f'"{col}"' for col in columns_require]) + ".")
    for startwith in columns_require_startwith:
        cols_startwith = [
            col for col in cols_remainer if col.startswith(startwith)]
        if len(cols_startwith) == 0:
            error.append(
                f'File columns must have columns name startwith "{startwith}" .')
        cols_remainer = cols_remainer - set(cols_startwith)
    if len(cols_remainer) > 0:
        error.append(
            "Columns " + ", ".join([f'"{col}"' for col in cols_remainer]) + " not used.")
    return error

##########################################


"""Validate Row Duplicate."""


def validate_duplicate_row(df: pd.DataFrame):
    error = []

    check_duplicate = ["hn", "date_of_submission", "species",
                       "submitted_sample", "bacteria_genus", "report_issued_date"]
    
    columns_cast_date = ["date_of_submission" , "report_issued_date"] 

    hash_db = table[check_duplicate].apply(
        lambda r: hash(tuple(str(c).lower() for c in r)), axis=1).values

    def check_exist(s):
        for col in columns_cast_date :
            s[col] = pd.to_datetime(s[col])
        return hash(tuple(str(c).lower() for c in s)) in hash_db

    result = df[check_duplicate].duplicated(keep=False)
    if result.sum() > 0:
        error.append(
            f'row is/are duplicate in same file at {", ".join(get_true_index_str(result))}.')

    result = df[check_duplicate].apply(check_exist,axis=1)

    if result.sum() > 0:
        error.append(
            f'row is/are duplicate in database at {", ".join(get_true_index_str(result))}.')

    return error
##########################################


"""Validate Columns Value."""



def get_true_index_str(series: pd.Series):
    return series[series].index.map(str)


def validate_column_value(df: pd.DataFrame):
    check_columns_blank = ["hn", "date_of_submission",
                           "species", "bacteria_genus", "report_issued_date"]

    check_startwith_blank = ["ans_"]

    check_startwith_bool = ["ans_"]

    def check_date_misformat(date_str: str):
        try:
            pd.to_datetime(date_str)
            return False
        except:
            return True

    error = []
    for chk_blank in check_columns_blank:
        result = (df[chk_blank] == "") | (df[chk_blank].isnull())
        if result.sum() > 0:
            error.append(
                f'columns "{chk_blank}" have empty at {", ".join(get_true_index_str(result))}.')
    for chk_sw_blank in check_startwith_blank:
        for chk_blank in df.columns[df.columns.str.startswith(chk_sw_blank)]:
            result = (df[chk_blank] == "") | (df[chk_blank].isnull())
            if result.sum() > 0:
                error.append(
                    f'columns "{chk_blank}" have empty at {", ".join(get_true_index_str(result))}.')

    dateformat_check_columns = ["date_of_submission", "report_issued_date"]
    for chk_date in dateformat_check_columns:
        result = df[chk_date].apply(check_date_misformat)
        if result.sum() > 0:
            error.append(
                f'columns "{chk_date}" have not valid date at {", ".join(get_true_index_str(result))}.')

    for chk_sw_bool in check_startwith_bool:
        for chk_bool in df.columns[df.columns.str.startswith(chk_sw_bool)]:
            result = df[chk_bool].apply(lambda x: str(
                x).lower() not in ["true", "false"])
            if result.sum() > 0:
                error.append(
                    f'columns "{chk_bool}" have non-boolean at {", ".join(get_true_index_str(result))}.')

    return error
##########################################


def csv_validation(id: int, filename: str, vitek_id: int, path: str):
    df = pd.read_csv(path)

# %%
