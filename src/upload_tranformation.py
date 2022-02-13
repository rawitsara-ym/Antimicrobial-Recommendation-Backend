# %%
from matplotlib.pyplot import table
import pandas as pd
from sqlalchemy.engine import Engine
import sqlalchemy

# %%


class UploadTranformation:
    def __init__(self, conn: Engine) -> None:
        self.conn = conn

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


# %%
table = pd.read_csv("../upload_file/TestFile.csv")
# %%
