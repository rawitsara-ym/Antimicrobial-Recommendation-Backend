import pandas as pd


class UploadValidator:
    ROW_MINIMUN = 100

    def get_true_index_str(self, series: pd.Series):
        return series[series].index.map(str)

    def __init__(self, db) -> None:
        self.db = db

    def validate_amount(self, df: pd.DataFrame):
        """Validate Amount minimun."""

        if len(df) < self.ROW_MINIMUN:
            return [f"File must have minimun {self.ROW_MINIMUN} row."]
        else:
            return []

    def validate_columns(self, df: pd.DataFrame):
        """Validate Columns."""

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
                    f'File columns must has columns name startwith "{startwith}" .')
            cols_remainer = cols_remainer - set(cols_startwith)
        if len(cols_remainer) > 0:
            error.append(
                "Columns " + ", ".join([f'"{col}"' for col in cols_remainer]) + " not used.")
        return error

    def validate_duplicate_row(self, df: pd.DataFrame):
        """Validate Row Duplicate."""

        error = []

        check_duplicate = ["hn", "date_of_submission", "species",
                           "submitted_sample", "bacteria_genus", "report_issued_date"]

        columns_cast_date = ["date_of_submission", "report_issued_date"]

        hash_db = self.db.table[check_duplicate].apply(
            lambda r: hash(tuple(str(c).lower() for c in r)), axis=1).values

        def check_exist(s):
            for col in columns_cast_date:
                s[col] = pd.to_datetime(s[col])
            return hash(tuple(str(c).lower() for c in s)) in hash_db

        result = df[check_duplicate].duplicated(keep=False)
        if result.sum() > 0:
            error.append(
                f'row is/are duplicate in same file at {", ".join(self.get_true_index_str(result))}.')

        result = df[check_duplicate].apply(check_exist, axis=1)

        if result.sum() > 0:
            error.append(
                f'row is/are duplicate in database at {", ".join(self.get_true_index_str(result))}.')

        return error

    def validate_column_value(self, df: pd.DataFrame):
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
                    f'columns "{chk_blank}" have empty at {", ".join(self.get_true_index_str(result))}.')
        for chk_sw_blank in check_startwith_blank:
            for chk_blank in df.columns[df.columns.str.startswith(chk_sw_blank)]:
                result = (df[chk_blank] == "") | (df[chk_blank].isnull())
                if result.sum() > 0:
                    error.append(
                        f'columns "{chk_blank}" have empty at {", ".join(self.get_true_index_str(result))}.')

        dateformat_check_columns = ["date_of_submission", "report_issued_date"]
        for chk_date in dateformat_check_columns:
            result = df[chk_date].apply(check_date_misformat)
            if result.sum() > 0:
                error.append(
                    f'columns "{chk_date}" have not valid date at {", ".join(self.get_true_index_str(result))}.')

        for chk_sw_bool in check_startwith_bool:
            for chk_bool in df.columns[df.columns.str.startswith(chk_sw_bool)]:
                result = df[chk_bool].apply(lambda x: str(
                    x).lower() not in ["true", "false"])
                if result.sum() > 0:
                    error.append(
                        f'columns "{chk_bool}" have non-boolean at {", ".join(self.get_true_index_str(result))}.')

        return error

    def validate(self, table: pd.DataFrame):
        validate_array = [
            ("amount", self.validate_amount),
            ("column", self.validate_columns),
            ("duplicate", self.validate_duplicate_row),
            ("value", self.validate_column_value),
        ]
        for validate in validate_array:
            errors = validate[1](table)
            if len(errors) > 0:
                return (False, validate[0], errors)

        return (True,)
