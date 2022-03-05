import pandas as pd

import itertools


class UploadValidator:
    ROW_MINIMUN = 300

    def get_true_index_str(self, series: pd.Series):
        def intervals_extract(iterable):
            iterable = sorted(set(iterable))
            for key, group in itertools.groupby(enumerate(iterable),
                                                lambda t: t[1] - t[0]):
                group = list(group)
                if group[0][1] != group[-1][1]:
                    yield str(group[0][1]) + " - " + str(group[-1][1])
                else:
                    yield str(group[0][1])
        return list(intervals_extract(series[series].index))

    def __init__(self, db, vitek_id) -> None:
        self.db = db
        self.vitek_id = vitek_id

    def validate_amount(self, df: pd.DataFrame):
        """Validate Amount minimun."""

        if len(df) < self.ROW_MINIMUN:
            return [f"ไฟล์ต้องประกอบไปด้วยแถวอย่างน้อย {self.ROW_MINIMUN} แถว"]
        else:
            return []

    def validate_columns(self, df: pd.DataFrame):
        """Validate Columns."""

        columns_require = ["hn", "date_of_submission", "report_issued_date",
                           "species", "bacteria_genus", "submitted_sample", "vitek_id"]

        columns_require_startwith = ["S/I/R_", "ans_"]

        error = []
        cols = set(df.columns)
        cols_remainer = cols - set(columns_require)
        if len(cols_remainer) != len(cols) - len(columns_require):
            error.append("ไฟล์ต้องประกอบด้วยคอลัมน์ " +
                         ", ".join(columns_require))
        startwith_error = []
        for startwith in columns_require_startwith:
            cols_startwith = [
                col for col in cols_remainer if col.startswith(startwith)]
            if len(cols_startwith) == 0:
                startwith_error.append(startwith)
            cols_remainer = cols_remainer - set(cols_startwith)
        if len(startwith_error) > 0:
            error.append(
                "ไฟล์ต้องมีคอลัมน์ที่ขึ้นต้นด้วย " + ', '.join(startwith_error))
        if len(cols_remainer) > 0:
            error.append(
                "ไฟล์มีคอลัมน์ที่ไม่ต้องการ ได้แก่ " + ", ".join(cols_remainer))
        return error

    def validate_column_value(self, df: pd.DataFrame):
        check_columns_blank = ["hn", "date_of_submission",
                               "species", "bacteria_genus", "report_issued_date", "vitek_id"]

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
                    f'คอลัมน์ {chk_blank} มีค่าว่างที่แถว {", ".join(self.get_true_index_str(result))}')
        for chk_sw_blank in check_startwith_blank:
            for chk_blank in df.columns[df.columns.str.startswith(chk_sw_blank)]:
                result = (df[chk_blank] == "") | (df[chk_blank].isnull())
                if result.sum() > 0:
                    error.append(
                        f'คอลัมน์ {chk_blank} มีค่าว่างที่แถว {", ".join(self.get_true_index_str(result))}')

        dateformat_check_columns = ["date_of_submission", "report_issued_date"]
        for chk_date in dateformat_check_columns:
            result = df[chk_date].apply(check_date_misformat)
            if result.sum() > 0:
                error.append(
                    f'คอลัมน์ {chk_date} มีค่าที่ไม่ใช่รูปแบบวันที่ที่แถว {", ".join(self.get_true_index_str(result))}')

        for chk_sw_bool in check_startwith_bool:
            for chk_bool in df.columns[df.columns.str.startswith(chk_sw_bool)]:
                result = df[chk_bool].apply(lambda x: str(
                    x).lower() not in ["true", "false"])
                if result.sum() > 0:
                    error.append(
                        f'คอลัมน์ {chk_bool} มีค่าที่ไม่ใช่ True หรือ False ที่แถว {", ".join(self.get_true_index_str(result))}')

        # Check vitek_id
        vitek = ["GN", "GP"]
        result = (df[df["vitek_id"].notnull()]["vitek_id"].apply(
            str.upper) != vitek[self.vitek_id - 1])
        if result.sum() > 0:
            error.append(
                f'คอลัมน์ vitek_id มีค่าที่ไม่ใช่ {vitek[self.vitek_id - 1]} ที่แถว {", ".join(self.get_true_index_str(result))}')
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
                f'ไฟล์มีแถวที่ซ้ำกัน ดังนี้ {", ".join(self.get_true_index_str(result))}')

        result = df[check_duplicate].apply(check_exist, axis=1)

        if result.sum() > 0:
            error.append(
                f'ไฟล์มีแถวที่ซ้ำกับฐานข้อมูล ดังนี้ {", ".join(self.get_true_index_str(result))}')

        return error

    def check_newvalue(self, df: pd.DataFrame):
        """Check New Value."""
        warning = []
        check_newvalue_cell = ["bacteria_genus", "submitted_sample"]

        for chk in check_newvalue_cell:

            old_value = set(row for row in self.db.table[chk].values)

            input_value = set(row for row in df[chk].values)

            new_value = input_value - old_value

            if len(new_value) > 0:
                warning.append('มีการเพิ่ม "' + chk +
                               '" ได้แก่ ' + ", ".join(new_value))

        in_cols_sir = set(c for c in df.columns if c.startswith("S/I/R_"))
        old_cols_sir = set(
            c for c in self.db.table.columns if c.startswith("S/I/R_"))
        new_cols_sir = in_cols_sir - old_cols_sir
        new_cols_sir = [
            col for col in new_cols_sir if df[col].notna().sum() > 0]
        if len(new_cols_sir) > 0:
            warning.append(
                'มีการเพิ่ม Antimicrobial S/I/R ได้แก่ ' + ", ".join(new_cols_sir))

        in_cols_ans = set(c for c in df.columns if c.startswith("ans_"))
        old_cols_ans = set(
            c for c in self.db.table.columns if c.startswith("ans_"))
        new_cols_ans = in_cols_ans - old_cols_ans
        new_cols_ans = [
            col for col in new_cols_ans if df[col].sum() > 0]
        if len(new_cols_ans) > 0:
            warning.append(
                'มีการเพิ่ม Antimicrobial Answer ได้แก่ ' + ", ".join(new_cols_ans))

        return warning

    def validate(self, table: pd.DataFrame):
        validate_array = [
            ("amount", self.validate_amount),
            ("column", self.validate_columns),
            ("value", self.validate_column_value),
            ("duplicate", self.validate_duplicate_row),
        ]
        for validate in validate_array:
            errors = validate[1](table)
            if len(errors) > 0:
                return (False, validate[0], errors)
        warning = self.check_newvalue(table)
        if len(warning) > 0:
            return (True, "warning", warning)
        return (True, "success")

# %%
