# %%

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine
import sqlalchemy
from sklearn.model_selection import train_test_split


class UploadTranformation:
    def __init__(self, vitek_id: int, conn: Engine) -> None:
        self.conn = conn
        self.vitek_id = vitek_id

    def tranform_species(self, series: pd.Series):
        series = series.str.strip().str.lower()

        species = {row[1]: row[0] for row in pd.read_sql_query(
            "SELECT id , name FROM public.species", self.conn).values}

        def clean_species(s: str):
            if s not in species.keys():
                s = "other"
            return species[s]

        return series.map(clean_species)

    def tranform_submitted_sample(self, series: pd.Series):
        series = series.str.strip().str.lower()

        submitted_sample = {row[1]: row[0] for row in pd.read_sql_query(
            "SELECT id , name FROM public.submitted_sample", self.conn).values}

        def clean_submitted_sample(sample: str):

            nonlocal submitted_sample
            sample = sample.lower().strip()

            # special case
            if sample == 'unk' or sample == '':
                return 'unknown'
            if 'opened wound' in sample:
                return 'open wound'
            if 'bited wound' in sample:
                return 'bited wound'

            # ตัดวงเล็บ, /, at, or
            cut_list1 = ["(", "/", '@', ' at ', ' or ']
            for cut in cut_list1:
                if cut in sample:
                    sample = sample[:sample.index(cut)].strip()

            # ตัด right, left
            cut_list2 = ['right ', 'rt.', 'r.',
                         'rt ', 'left ', 'lt.', 'l.', 'lt ']
            for cut in cut_list2:
                if cut in sample:
                    sample = sample.replace(cut, '').strip()

            if sample not in submitted_sample.keys():
                with self.conn.connect() as con:
                    query = sqlalchemy.text(
                        "INSERT INTO public.submitted_sample(name) VALUES (:name);")
                    con.execute(query, name=sample)
                submitted_sample = {row[1]: row[0] for row in pd.read_sql_query(
                    "SELECT id , name FROM public.submitted_sample", self.conn).values}
            return submitted_sample[sample]

        return series.map(clean_submitted_sample)

    def tranform_bacteria_genus(self, series: pd.Series):
        series = series.str.strip().str.lower()

        bacteria_genus = {row[1]: row[0] for row in pd.read_sql_query(
            "SELECT id , name FROM public.bacteria_genus", self.conn).values}

        def clean_bacteria_genus(bact: str):

            nonlocal bacteria_genus

            bact = bact.split()[0]

            if bact not in bacteria_genus.keys():
                with self.conn.connect() as con:
                    query = sqlalchemy.text(
                        "INSERT INTO public.bacteria_genus(name) VALUES (:name);")
                    con.execute(query, name=bact)
                bacteria_genus = {row[1]: row[0] for row in pd.read_sql_query(
                    "SELECT id , name FROM public.bacteria_genus", self.conn).values}
            return bacteria_genus[bact]

        return series.map(clean_bacteria_genus)

    def tranform_sir(self, df: pd.DataFrame):
        cols_sir = set(col for col in df.columns if col.startswith("S/I/R"))
        sir = df[cols_sir].reset_index().melt(
            id_vars="index", var_name="name", value_name="sir_sub_type")
        sir = sir[sir["sir_sub_type"].isin(['+', '-', 'S', 'I', 'R'])]
        sir["name"] = sir["name"].str.replace("S/I/R_", "")

        query_sir = sqlalchemy.text(
            "SELECT id , name , sir_type_id FROM public.antimicrobial_sir WHERE vitek_id = :v_id")
        sir_name = {row[1]: row[0] for row in pd.read_sql_query(
            query_sir, self.conn, params={"v_id": self.vitek_id}).values}

        sir_type = {row[0]: row[2] for row in pd.read_sql_query(
            query_sir, self.conn, params={"v_id": self.vitek_id}).values}

        def clean_tranform_sir_name(anti: str):

            nonlocal sir_name
            nonlocal sir_type

            if anti not in sir_name.keys():

                agg = sir[sir["name"] == anti]["sir_sub_type"].value_counts()
                sir_count = 0
                pn_count = 0
                for sym, count in agg.items():
                    if sym in "SIR":
                        sir_count += count
                    elif sym in "+-":
                        pn_count += count
                if sir_count > pn_count:
                    sir_type = 2
                elif sir_count < pn_count:
                    sir_type = 1
                else:
                    return 0  # ผิดพลาด

                with self.conn.connect() as con:
                    query = sqlalchemy.text(
                        "INSERT INTO public.antimicrobial_sir(vitek_id , name , sir_type_id) VALUES (:v_id,:name,:type);")
                    con.execute(query, v_id=self.vitek_id,
                                name=anti, type=sir_type)

                sir_name = {row[1]: row[0] for row in pd.read_sql_query(
                    query_sir, self.conn, params={"v_id": self.vitek_id}).values}

                sir_type = {row[0]: row[2] for row in pd.read_sql_query(
                    query_sir, self.conn, params={"v_id": self.vitek_id}).values}

            return sir_name[anti]

        sir["name"] = sir["name"].map(clean_tranform_sir_name)

        sir = sir.rename(
            {"index": "report_id", "name": "antimicrobial_id"}, axis=1)

        sir_sub = {row[1]: (row[0], row[2]) for row in pd.read_sql_query(
            "SELECT id , symbol , sir_type_id FROM public.sir_sub_type", self.conn).values}

        sir_sub_id = []

        for _id in sir.index:
            result = sir.loc[_id, "sir_sub_type"]
            if result == "+":
                result = "POS"
            elif result == "-":
                result = "NEG"

            if sir_type[sir.loc[_id, "antimicrobial_id"]] == sir_sub[result][1]:
                result = sir_sub[result][0]
            else:
                result = None
            sir_sub_id.append(result)

        sir["sir_sub_id"] = sir_sub_id
        sir = sir.drop("sir_sub_type", axis=1)

        return sir

    def tranform_answer(self, df: pd.DataFrame):
        cols_ans = set(col for col in df.columns if col.startswith("ans_"))

        ans = df[cols_ans].reset_index().melt(
            id_vars="index", var_name="name", value_name="value")

        ans["name"] = ans["name"].str.replace("ans_", "")

        ans["value"] = ans["value"].astype('bool')

        ans = ans[ans["value"]]

        query_ans = sqlalchemy.text(
            "SELECT id , name FROM public.antimicrobial_answer WHERE vitek_id = :v_id")

        ans_name = {row[1]: row[0] for row in pd.read_sql_query(
            query_ans, self.conn, params={"v_id": self.vitek_id}).values}

        def clean_answer(ans: str):

            nonlocal ans_name

            if ans not in ans_name.keys():
                with self.conn.connect() as con:
                    query = sqlalchemy.text(
                        "INSERT INTO public.antimicrobial_answer(vitek_id,name) VALUES (:v_id,:name);")
                    con.execute(query, v_id=self.vitek_id, name=ans)
                ans_name = {row[1]: row[0] for row in pd.read_sql_query(
                    query_ans, self.conn, params={"v_id": self.vitek_id}).values}
            return ans_name[ans]

        ans["name"] = ans["name"].map(clean_answer)
        ans = ans.rename({"index": "report_id", "name": "antimicrobial_id"}, axis=1).drop(
            ["value"], axis=1)
        return ans

    def transform(self, df: pd.DataFrame, file_id: int):
        report = pd.DataFrame()
        report["hn"] = df["hn"].map(str)
        report["date_of_submission"] = df["date_of_submission"]
        report["report_issued_date"] = df["report_issued_date"]
        report["species_id"] = self.tranform_species(df["species"])
        report["bacteria_genus_id"] = self.tranform_bacteria_genus(
            df["bacteria_genus"])
        report["submitted_sample_id"] = self.tranform_submitted_sample(
            df["submitted_sample"])
        report["vitek_id"] = self.vitek_id
        report["file_id"] = file_id
        report_sir = self.tranform_sir(df)
        report_ans = self.tranform_answer(df)
        return report, report_sir, report_ans

    def split_train_test_bycase(self, report: pd.DataFrame):
        train, test = train_test_split(
            report.index, stratify=report["species_id"], test_size=0.1, random_state=0)
        return train, test

    def split_train_test_byanti(self, report: pd.DataFrame, anti: pd.DataFrame):
        report_train = report[report["type"] == "train"]
        upload_data = pd.DataFrame()
        anti_id = [
            np.arange(1, 12),  # GN
            np.arange(12, 23),  # GP
        ]
        for i in anti_id[self.vitek_id - 1]:
            report_train_answer = report_train.join(
                anti[anti["antimicrobial_id"] == i].set_index("report_id"), how='left')["antimicrobial_id"]
            report_train_answer = report_train_answer.map(pd.notna)
            train, test = train_test_split(
                report_train["id"], stratify=report_train_answer, test_size=.2, random_state=0)
            upload_train = pd.DataFrame()
            upload_train["report_id"] = train
            upload_train["antimicrobial_id"] = i
            upload_train["sub_type"] = "train"

            upload_test = pd.DataFrame()
            upload_test["report_id"] = test
            upload_test["antimicrobial_id"] = i
            upload_test["sub_type"] = "test"
            upload_data = upload_data.append(upload_train)
            upload_data = upload_data.append(upload_test)
        return upload_data

    def upload(self, df: pd.DataFrame, file_id: int) -> int:
        report, report_sir, report_ans = self.transform(df, file_id)
        index_train, index_test = self.split_train_test_bycase(report)
        report.loc[index_train, "type"] = "train"
        report.loc[index_test, "type"] = "test"
        query = sqlalchemy.text("""
        INSERT INTO public.report(hn , date_of_submission, report_issued_date, species_id, bacteria_genus_id,submitted_sample_id,vitek_id,file_id) 
        VALUES (:hn , :date_of_submission, :report_issued_date, :species_id, :bacteria_genus_id,:submitted_sample_id,:vitek_id,:file_id) RETURNING id""")
        with self.conn.connect() as con:
            id_arr = []
            for i in report.index:
                new = {i: v for i, v in report.loc[i].items()}
                new["species_id"] = int(new["species_id"])
                new["bacteria_genus_id"] = int(
                    new["bacteria_genus_id"])
                new["submitted_sample_id"] = int(
                    new["submitted_sample_id"])
                new["vitek_id"] = int(new["vitek_id"])
                new["file_id"] = int(new["file_id"])
                rs = con.execute(
                    query, **new)
                for row in rs:
                    id_arr.append(row[0])
            report["id"] = id_arr

        def mapping_id(k):
            return {i: v for i,
                    v in report["id"].items()}.get(k)

        report_sir["report_id"] = report_sir["report_id"].map(mapping_id)
        report_ans["report_id"] = report_ans["report_id"].map(mapping_id)
        report_sir.to_sql('report_sir', schema='public',
                          con=self.conn, if_exists='append', index=False)
        report_ans.to_sql('report_answer', schema='public',
                          con=self.conn, if_exists='append', index=False)
        report_train = self.split_train_test_byanti(report, report_ans)
        report_train.to_sql('report_train', schema='public',
                            con=self.conn, if_exists='append', index=False)
        return len(report)

# %%
