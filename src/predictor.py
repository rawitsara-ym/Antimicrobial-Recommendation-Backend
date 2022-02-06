import pandas as pd
import joblib
from sqlalchemy.engine import Engine
import sqlalchemy
from IPython.display import display


class Predictior:
    GN = 1
    GP = 2

    def __init__(self, conn: Engine) -> None:
        self.conn = conn
        self.startup()

    def startup(self):
        query = sqlalchemy.text("""SELECT model.id , antimicrobial_answer.name , model.schema AS model_schema, model.model_path , sub_binning.schema AS submitted_sample_binning
            FROM model 
            INNER JOIN antimicrobial_answer ON model.antimicrobial_id = antimicrobial_answer.id 
            INNER JOIN (
                SELECT model_group.id , model_group_model.model_id 
                FROM model_group 
				INNER JOIN model_group_model ON model_group.id = model_group_model.model_group_id 
				WHERE model_group.version > 0 ) AS m_group ON model.id = m_group.model_id 
			INNER JOIN submitted_sample_binning_model_group AS sub_binning ON sub_binning.model_group_id = m_group.id
            WHERE model.id IN (
                SELECT model_id FROM model_group_model WHERE model_group_id IN (
                    SELECT id FROM model_group WHERE version = 0 AND vitek_id = :v_id ))""")
        self.gn = pd.read_sql_query(query, self.conn, params={"v_id": self.GN})
        self.gp = pd.read_sql_query(query, self.conn, params={"v_id": self.GP})
        schema_gn_all = set()
        schema_gp_all = set()
        for schema in self.gn["model_schema"].values:
            schema_gn_all.update(set(schema))
        for schema in self.gp["model_schema"].values:
            schema_gp_all.update(set(schema))

        self.schema_gn_df = pd.DataFrame(
            data=[[0]*len(schema_gn_all)], columns=schema_gn_all)
        self.schema_gp_df = pd.DataFrame(
            data=[[0]*len(schema_gp_all)], columns=schema_gp_all)

        self.model_gn = {row[0]: joblib.load(row[1]) for row in self.gn[["name", "model_path"]].values}
        self.model_gp = {row[0]: joblib.load(row[1]) for row in self.gp[["name", "model_path"]].values}

    def prediction(self, data: pd.Series):
        result = []

        dummies_data_origin = pd.get_dummies(pd.DataFrame(data).T)
        