import pandas as pd
from sqlalchemy.engine import Engine
import sqlalchemy


class TableToCsv:

    def __init__(self, conn: Engine, vitek_id: int) -> None:
        self.conn = conn
        self.vitek_id = vitek_id
        self.startup()

    def startup(self):
        master_table = self.query_report_table()
        answer_table = self.query_answer_table()
        sir_table = self.query_sir_table()
        self.table = master_table.join(answer_table, how="left")
        self.table.loc[:, self.table.columns[self.table.columns.str.startswith(
            "ans_")]] = self.table.loc[:, self.table.columns[self.table.columns.str.startswith(
                "ans_")]].fillna(False)
        self.table = self.table.join(sir_table, how="left")
        self.table.loc[:, self.table.columns[self.table.columns.str.startswith(
            "S/I/R_")]] = self.table.loc[:, self.table.columns[self.table.columns.str.startswith(
                "S/I/R_")]].fillna("")

    def query_report_table(self):
        query = sqlalchemy.text("""
        SELECT report.id , report.hn , report.date_of_submission , species.name AS species, submitted_sample.name AS submitted_sample, vitek_id_card.name AS vitek_id, bacteria_genus.name AS bacteria_genus , report.report_issued_date, report.file_id
        FROM public.report AS report
        INNER JOIN public.species AS species ON species.id = report.species_id
        INNER JOIN public.bacteria_genus AS bacteria_genus ON bacteria_genus.id = report.bacteria_genus_id
        INNER JOIN public.submitted_sample AS submitted_sample ON submitted_sample.id = report.submitted_sample_id
        INNER JOIN public.vitek_id_card AS vitek_id_card ON vitek_id_card.id = report.vitek_id
        INNER JOIN public.file AS file ON file.id = report.file_id
        WHERE file.active AND report.vitek_id = :v_id
        """)
        table = pd.read_sql_query(query, self.conn, params={
                                  "v_id": self.vitek_id}).set_index('id')
        table['date_of_submission'] = pd.to_datetime(
            table['date_of_submission']).dt.normalize()
        table['report_issued_date'] = pd.to_datetime(
            table['report_issued_date']).dt.normalize()
        self.file_id = list(set(table['file_id']))
        # table.drop(columns=['file_id'], inplace=True)
        return table

    def query_answer_table(self):
        query = sqlalchemy.text("""
        SELECT report_answer.report_id , anti.name
        FROM public.report_answer AS report_answer
        INNER JOIN public.antimicrobial_answer AS anti ON anti.id = report_answer.antimicrobial_id
        WHERE anti.vitek_id = :v_id
        """)
        table = pd.read_sql_query(query, self.conn, params={
                                  "v_id": self.vitek_id})
        table["value"] = True
        table = table.pivot(index="report_id",
                            columns="name").droplevel(0, axis=1)
        table = table.rename(
            {ans: "ans_" + ans for ans in table.columns}, axis=1)
        return table

    def query_sir_table(self):
        query = sqlalchemy.text("""
        SELECT report_sir.report_id , anti.name , sir.symbol
        FROM public.report_sir AS report_sir
        INNER JOIN public.antimicrobial_sir AS anti ON anti.id = report_sir.antimicrobial_id
        INNER JOIN public.sir_sub_type AS sir ON sir.id = report_sir.sir_sub_id
        WHERE anti.vitek_id = :v_id
        """)
        table = pd.read_sql_query(query, self.conn, params={
                                  "v_id": self.vitek_id})
        table = table.applymap(lambda x: {'NEG': '-', 'POS': '+'}.get(x, x))
        table = table.pivot(index="report_id",
                            columns="name").droplevel(0, axis=1)
        table = table.rename(
            {ans: "S/I/R_" + ans for ans in table.columns}, axis=1)
        return table
