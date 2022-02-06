import pandas as pd
import joblib
from sqlalchemy.engine import Engine
import sqlalchemy
from IPython.display import display


class Predictior:
    GN = 0
    GP = 1

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

        database = [pd.read_sql_query(query, self.conn, params={
            "v_id": self.GN + 1}), pd.read_sql_query(query, self.conn, params={"v_id": self.GP + 1})]

        self.anti_names = [[anti for anti in database[self.GN]["name"].values],
                           [anti for anti in database[self.GP]["name"].values]]

        self.models_schema = [{row[0]: eval(row[1]) for row in database[self.GN][["name", "model_schema"]].values},
                              {row[0]: eval(row[1]) for row in database[self.GP][["name", "model_schema"]].values}]

        self.submitted_sample_binning = [{row[0]: eval(row[1]) for row in database[self.GN][["name", "submitted_sample_binning"]].values},
                                         {row[0]: eval(row[1]) for row in database[self.GP][["name", "submitted_sample_binning"]].values}]

        self.models = [{row[0]: joblib.load(row[1]) for row in database[self.GN][["name", "model_path"]].values},
                       {row[0]: joblib.load(row[1]) for row in database[self.GP][["name", "model_path"]].values}]

        schema_all = [set(), set()]

        for schema in self.models_schema[self.GN].values():
            schema_all[self.GN].update(schema)
        for schema in self.models_schema[self.GP].values():
            schema_all[self.GP].update(schema)

        self.schema_df = [pd.DataFrame(columns=schema_all[self.GN]),
                          pd.DataFrame(columns=schema_all[self.GP])]

    def predict(self, data: pd.Series, vitek_id):
        result = []
        for anti in self.anti_names[vitek_id]:
            model = self.models[vitek_id][anti]
            binning = self.submitted_sample_binning[vitek_id][anti]
            if not data['submitted_sample'] in binning:
                data['submitted_sample'] = "other"
            dummies_data_origin = pd.get_dummies(pd.DataFrame(data).T)
            dummies_df = self.get_dummies_dataframe_columns(
                self.schema_df[vitek_id], dummies_data_origin)
            dummies_data = dummies_df.filter(
                self.models_schema[vitek_id][anti])
            anti = anti.replace("_", '/')
            result_single = {
                "antimicrobial": anti,
                "score": model.predict_proba(dummies_data)[:, 1][0],
            }
            if result_single["score"] >= 0.5:
                result.append(result_single)
        list_sorted = sorted(
            result, key=lambda item: item['score'], reverse=True)
        return {item['antimicrobial']: round(float(item['score'])*100, 2) for item in list_sorted}

    def get_dummies_dataframe_columns(self, new_df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
        old_df = old_df.filter(new_df.columns)
        new_df = new_df.append(old_df)
        new_df.fillna(0, inplace=True)
        return new_df
