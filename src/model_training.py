import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.engine import Engine
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE, ADASYN, BorderlineSMOTE, SVMSMOTE
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
from typing import Dict
import joblib
import datetime
import os
from src.rsmote import RSmoteKClasses
from src.retraining_status import check_retraining_status

# SMOTE


class SMOTERounding:
    def __init__(self, smote) -> None:
        if hasattr(smote, "fit_resample"):
            self.smote = smote
        else:
            raise Exception("Method fit_resample not found.")

    def fit_resample(self, X, y):
        X_resample, y_resample = self.smote.fit_resample(
            X.astype(np.float64), y)
        X_resample = np.round(X_resample).astype(np.int32)
        return X_resample, y_resample

# Retraining


class ModelRetraining:
    def __init__(self, db, vitek_id: int, conn: Engine, model_location) -> None:
        self.db = db
        self.vitek_id = vitek_id
        self.conn = conn
        self.model_location = model_location

    def training(self, retraining_id: int):
        vitek = ["GN", "GP"][self.vitek_id - 1]
        last_ver = self.lastest_version()

        # Get current model
        current_model = self.get_model(0)

        # Create model directory
        dir_path = f"./ml_model/{vitek}/version_{last_ver+1}"
        if not os.path.exists(f'{self.model_location}/{dir_path}'):
            os.makedirs(f'{self.model_location}/{dir_path}')

        # Binning submitted sample
        self.db.table = self.binning_less_than(
            self.db.table, "submitted_sample", 10)
        submitted_sample_binning = list(
            self.db.table["submitted_sample"].unique())

        # Replace empty string with null & drop null columns
        self.db.table.replace(r'^\s*$', np.NaN, regex=True, inplace=True)
        self.db.table = self.db.table[self.db.table['file_id'].isin(
            self.db.file_id)].dropna(axis=1, how='all')

        rows_insert = []

        # Training
        for anti_id, anti_name in self.get_antimicrobial_ans().items():
            # Train & Test
            train, test = self.get_train_test(anti_id)
            X_train = train[["species", "submitted_sample", "bacteria_genus"] +
                            list(train.columns[train.columns.str.startswith("S/I/R_")])]
            X_test = test[["species", "submitted_sample", "bacteria_genus"] +
                          list(test.columns[test.columns.str.startswith("S/I/R_")])]
            y_train = train["ans_" + anti_name]
            y_test = test["ans_" + anti_name]

            # One-Hot
            X_train_dummies = pd.get_dummies(X_train)
            X_test_dummies = self.get_dummies_dataframe_columns(
                X_train_dummies, pd.get_dummies(X_test))

            # Get model config
            model, smote = self.get_model_configuration(anti_id)

            # SMOTE
            X_resampling, y_resampling = SMOTERounding(
                smote).fit_resample(X_train_dummies, y_train)

            # Fit model
            model = model.fit(X_resampling, y_resampling)

            # Evaluate model
            measure = self.evaluation(X_test_dummies, y_test, model)

            # Dump model
            model_path = dir_path + f"/{anti_name.replace('/','_')}.joblib"
            joblib.dump(model, f'{self.model_location}/{model_path}')

            # Compare new model with current model
            compare_result = self.compare_model_by_f1(X_test, y_test, current_model.loc[anti_id]["model_path"],
                                                      eval(current_model.loc[anti_id]["schema"]), f1_new=measure["f1"])

            # Data for insert
            row = {
                "antimicrobial_id": anti_id,
                "schema": str(list(X_test_dummies.columns)),
                "model_path": model_path,
                "create_at": datetime.datetime.now(),
                "performance": compare_result,
                "accuracy": measure["accuracy"],
                "precision": measure["precision"],
                "recall": measure["recall"],
                "f1": measure["f1"]
            }
            rows_insert.append(row)

            # if cancel
            if check_retraining_status(retraining_id, self.conn):
                return -1

        # INSERT model
        model_group_id = self.insert_into_db(
            rows_insert, submitted_sample_binning, last_ver+1)

        # Test by case
        test_by_case_measure = self.test_by_case(last_ver+1)

        # UPDATE model_group
        with self.conn.connect() as con:
            query = sqlalchemy.text(
                """
                UPDATE public.model_group
                SET accuracy=:accuracy, precision=:precision, recall=:recall, f1=:f1
                WHERE id = :id
                """)
            rs = con.execute(query, accuracy=test_by_case_measure["accuracy"],
                             precision=test_by_case_measure["precision"],
                             recall=test_by_case_measure["recall"],
                             f1=test_by_case_measure["f1"], id=model_group_id)

        # if cancel final check
        if check_retraining_status(retraining_id, self.conn):
            self.remove_model_group(model_group_id)
            return -1

        # UPDATE model current version
        self.update_model_current_version()

        return model_group_id

    def binning_less_than(self, df: pd.DataFrame, column, k: int, other="other") -> pd.DataFrame:
        newdf = df.copy()
        values = newdf[column].value_counts()
        index = values[values < k].index
        newdf.loc[newdf[column].isin(index), column] = other
        return newdf

    def get_dummies_dataframe_columns(self, df_dummies: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
        old_df = pd.get_dummies(old_df).filter(df_dummies.columns)
        new_df = pd.DataFrame(columns=list(df_dummies.columns)).append(old_df)
        new_df.fillna(0, inplace=True)
        return new_df

    def evaluation(self, X, y, model, measures: Dict = None, threshold: float = .5) -> pd.DataFrame:
        if measures is None:
            measures = {'accuracy': lambda true, pred: accuracy_score(true, [p >= threshold for p in pred]),
                        'precision': lambda true, pred: precision_score(true, [p >= threshold for p in pred]),
                        'recall': lambda true, pred: recall_score(true, [p >= threshold for p in pred]),
                        'f1': lambda true, pred: f1_score(true, [p >= threshold for p in pred]),
                        }

        return {key: value(y, [ctrue for _, ctrue in model.predict_proba(X)]) for key, value in measures.items()}

    def test_by_case(self, version: int):
        # antimicrobial answer startswith "ans_"
        anti_ans = [
            "ans_" + anti_name for anti_name in self.get_antimicrobial_ans().values()]
        test_bycase = self.get_test_bycase()  # get test by case report
        X_bycase = test_bycase[["species", "submitted_sample", "bacteria_genus"] +
                               list(test_bycase.columns[test_bycase.columns.str.startswith("S/I/R_")])]  # feature
        y_bycase = test_bycase[list(
            test_bycase.columns[test_bycase.columns.isin(anti_ans)])]  # answer

        models = [["ans_" + row[0], joblib.load(f"{self.model_location}/{row[1]}"), eval(row[2])]
                  for row in self.get_model(version).values]  # load model
        df_predict = pd.DataFrame()

        for model in models:
            df_schema = pd.DataFrame(columns=model[2])  # create schema
            X_dummies = self.get_dummies_dataframe_columns(
                df_schema, pd.get_dummies(X_bycase))  # one-hot
            df_predict[model[0]] = model[1].predict(X_dummies)  # predict

        return self.evaluate_by_case(y_bycase, df_predict)

    def evaluate_by_case(self, ans_df: pd.DataFrame, predict_df: pd.DataFrame):
        row = len(ans_df)
        acc_sum = 0
        prec_sum = 0
        rec_sum = 0
        f1_sum = 0
        for i in range(row):
            if ans_df.iloc[i].any() == False:  # No recommend case
                if predict_df.iloc[i].any() == False:
                    score = 1
                else:
                    score = 0
                acc_sum += accuracy_score(ans_df.iloc[i], predict_df.iloc[i])
                prec_sum += score
                rec_sum += score
                f1_sum += score
            else:  # Normal case
                if predict_df.iloc[i].any() == False:
                    prec_score = 0
                else:
                    prec_score = precision_score(
                        ans_df.iloc[i], predict_df.iloc[i])
                acc_sum += accuracy_score(ans_df.iloc[i], predict_df.iloc[i])
                prec_sum += prec_score
                rec_sum += recall_score(ans_df.iloc[i], predict_df.iloc[i])
                f1_sum += f1_score(ans_df.iloc[i], predict_df.iloc[i])

        return {
            "accuracy": acc_sum/row,
            "precision": prec_sum/row,
            "recall": rec_sum/row,
            "f1": f1_sum/row
        }

    def compare_model_by_f1(self, X_test, y_test, model_path_old: str, schema_old: list, f1_new: float):
        df_schema = pd.DataFrame(columns=schema_old)
        X_test_dummies = self.get_dummies_dataframe_columns(
            df_schema, pd.get_dummies(X_test))
        f1_old = self.evaluation(
            X_test_dummies, y_test, joblib.load(f'{self.model_location}/{model_path_old}'))["f1"]
        if f1_new > f1_old:
            performance = "better"
        elif f1_new < f1_old:
            performance = "worse"
        else:
            performance = "same"
        return performance

    def insert_into_db(self, rows: list, submitted_sample_binning: list, version: int):
        # INSERT model
        with self.conn.connect() as con:
            query = sqlalchemy.text(
                """
                INSERT INTO public.model(antimicrobial_id, schema, model_path, create_at, performance, accuracy, precision, recall, f1) 
                VALUES (:antimicrobial_id, :schema, :model_path, :create_at, :performance, :accuracy, :precision, :recall, :f1) 
                RETURNING id;
                """)

            model_id_list = []
            for new_row in rows:
                rs = con.execute(query, **new_row)
                for row in rs:
                    model_id_list.append(row[0])

        # INSERT model_group
        with self.conn.connect() as con:
            query = sqlalchemy.text(
                """
                INSERT INTO public.model_group(version, vitek_id) 
                VALUES (:version, :vitek_id) 
                RETURNING id;
                """)
            rs = con.execute(query, version=version, vitek_id=self.vitek_id)

            for row in rs:
                model_group_id = row[0]

        # INSERT model_group_model
        with self.conn.connect() as con:
            query = sqlalchemy.text(
                """
                INSERT INTO public.model_group_model(model_group_id, model_id) 
                VALUES (:model_group_id, :model_id) 
                """)
            for model_id in model_id_list:
                rs = con.execute(
                    query, model_group_id=model_group_id, model_id=model_id)

        # INSERT model_group_file
        with self.conn.connect() as con:
            query = sqlalchemy.text(
                """
                INSERT INTO public.model_group_file(file_id, model_group_id) 
                VALUES (:file_id, :model_group_id) 
                """)
            for file_id in self.db.file_id:
                rs = con.execute(query, file_id=file_id,
                                 model_group_id=model_group_id)

        # INSERT submitted_sample_binning_model_group
        with self.conn.connect() as con:
            query = sqlalchemy.text(
                """
                INSERT INTO public.submitted_sample_binning_model_group(model_group_id, schema) 
                VALUES (:model_group_id, :schema) 
                """)
            con.execute(query, model_group_id=model_group_id,
                        schema=str(submitted_sample_binning))

        return model_group_id

    def update_model_current_version(self):
        current_model_group_id = self.get_current_model_group_id(version=0)
        performance_model = self.get_performance_model(
            version=self.lastest_version())
        for perf in performance_model:
            if perf["performance"] == "better":
                mgm_id = self.get_model_group_model_id(
                    mg_id=current_model_group_id, anti_id=perf["anti_id"])
                with self.conn.connect() as con:
                    query = sqlalchemy.text(
                        """
                        UPDATE public.model_group_model
                        SET model_id = :model_id
                        WHERE id = :id
                        """)
                    rs = con.execute(
                        query, model_id=perf["model_id"], id=mgm_id)

    def remove_model_group(self, model_group_id):
        # DELETE model
        with self.conn.connect() as con:
            query_get_model = sqlalchemy.text(
                """
                SELECT m.id , m.model_path FROM public.model_group_model AS mgm
                INNER JOIN public.model AS m ON m.id = mgm.model_id
                WHERE model_group_id = :id;
                """)
            rs = con.execute(query_get_model, id=model_group_id)

            query_del_model = sqlalchemy.text(
                """
                DELETE FROM public.model
                WHERE id = :id;
                """)

            for row in rs:
                model_id = row[0]
                model_path = row[1]
                con.execute(query_del_model, id=model_id)
                os.remove(model_path)

            query_del_model_group = sqlalchemy.text(
                """
                DELETE FROM public.model_group
                WHERE id = :id;
                """)

            con.execute(query_del_model_group, id=model_group_id)

    ########### Query ##########

    def get_train_test(self, anti_id: int):
        query_train_id = sqlalchemy.text("""
                                         SELECT report_id 
                                         FROM public.report_train
                                         INNER JOIN public.report ON public.report.id = public.report_train.report_id
                                         WHERE sub_type = 'train' AND antimicrobial_id = :anti_id AND file_id = :file_id
                                         """)
        query_test_id = sqlalchemy.text("""
                                        SELECT report_id
                                        FROM public.report_train
                                        INNER JOIN public.report ON public.report.id = public.report_train.report_id
                                        WHERE sub_type = 'test' AND antimicrobial_id = :anti_id AND file_id = :file_id
                                        """)
        train_id = []
        test_id = []
        for file_id in self.db.file_id:
            train_id.extend(pd.read_sql_query(query_train_id, self.conn, params={
                            "anti_id": anti_id, "file_id": file_id})["report_id"])
            test_id.extend(pd.read_sql_query(query_test_id, self.conn, params={
                           "anti_id": anti_id, "file_id": file_id})["report_id"])
        df_train = self.db.table.loc[train_id]
        df_test = self.db.table.loc[test_id]
        return df_train, df_test

    def get_antimicrobial_ans(self) -> Dict:
        query_ans = sqlalchemy.text(
            "SELECT id, name FROM public.antimicrobial_answer WHERE vitek_id = :v_id ORDER BY id")
        ans_name = pd.read_sql_query(query_ans, self.conn, params={
                                     "v_id": self.vitek_id})
        anti_id_range = [
            np.arange(1, 12),  # GN
            np.arange(12, 23),  # GP
        ]
        anti_ans = {row[0]: row[1] for row in ans_name[ans_name['id'].isin(
            anti_id_range[self.vitek_id-1])].values}
        return anti_ans

    def get_test_bycase(self):
        query_test_id = sqlalchemy.text("""
                                        SELECT public.report.id 
                                        FROM public.report
                                        WHERE type = 'test' AND public.report.vitek_id = :v_id AND file_id = :file_id
                                        """)
        test_id = []
        for file_id in self.db.file_id:
            test_id.extend(pd.read_sql_query(query_test_id, self.conn, params={
                           "v_id": self.vitek_id, "file_id":  file_id})['id'])
        df_test = self.db.table.loc[test_id]
        return df_test

    def get_model(self, version: int):
        query_model = sqlalchemy.text("""SELECT ans.id, ans.name, model_path, schema
            FROM public.model_group as mg
            INNER JOIN public.model_group_model as mgm ON mgm.model_group_id = mg.id
            INNER JOIN public.model as m ON m.id = mgm.model_id
            INNER JOIN public.antimicrobial_answer as ans ON ans.id = m.antimicrobial_id
            WHERE mg.version = :version AND mg.vitek_id = :v_id
            ORDER BY ans.name""")
        model = pd.read_sql_query(query_model, self.conn, params={
                                  "version": version, "v_id": self.vitek_id})
        model.set_index("id", inplace=True)
        return model

    def lastest_version(self):
        query = sqlalchemy.text(
            """ SELECT MAX(version)
                FROM public.model_group
                WHERE vitek_id = :vitek_id
            """)
        lastest_version = pd.read_sql_query(
            query, self.conn, params={"vitek_id": self.vitek_id}).values[0][0]
        return int(lastest_version)

    def get_performance_model(self, version: int) -> Dict:
        query = sqlalchemy.text(
            """
            SELECT m.id, m.antimicrobial_id, m.performance
            FROM public.model_group as mg
            INNER JOIN public.model_group_model as mgm ON mgm.model_group_id = mg.id
            INNER JOIN public.model as m ON m.id = mgm.model_id
            WHERE vitek_id = :v_id AND version = :version
            """)
        performance = pd.read_sql_query(query, self.conn, params={
                                        "v_id": self.vitek_id, "version": version})
        return [{"model_id": row[0], "anti_id": row[1], "performance": row[2]} for row in performance.values]

    def get_current_model_group_id(self, version: int):
        query = sqlalchemy.text(
            """
            SELECT id
            FROM public.model_group
            WHERE vitek_id = :v_id AND version = :version
            """)
        model_group_id = pd.read_sql_query(query, self.conn, params={
                                           "v_id": self.vitek_id, "version": version}).values[0][0]
        return int(model_group_id)

    def get_model_group_model_id(self, mg_id: int, anti_id: int):
        query = sqlalchemy.text(
            """
            SELECT mgm.id
            FROM public.model_group_model as mgm
            INNER JOIN public.model as m ON m.id = mgm.model_id
            WHERE model_group_id = :mg_id AND antimicrobial_id = :anti_id
            """)
        mgm_id = pd.read_sql_query(query, self.conn, params={
                                   "mg_id": mg_id, "anti_id": anti_id}).values[0][0]
        return int(mgm_id)

    def get_model_configuration(self, anti_id: int):
        query = sqlalchemy.text(
            "SELECT * FROM public.model_configuration WHERE antimicrobial_id = :anti_id")
        config = pd.read_sql_query(query, self.conn, params={
                                   "anti_id": anti_id}).iloc[0]
        model = eval(config["algorithm"])(eval_metric=f1_score,
                                          verbosity=0,
                                          use_label_encoder=False,
                                          random_state=int(
                                              config["random_state"]),
                                          n_estimators=int(
                                              config["n_estimators"]),
                                          gamma=float(config["gamma"]),
                                          max_depth=int(config["max_depth"]),
                                          subsample=float(config["subsample"]),
                                          colsample_bytree=float(
                                              config["colsample_bytree"]),
                                          learning_rate=float(
                                              config["learning_rate"])
                                          )
        smote_algo = {
            "SMOTE": SMOTE,
            "R-SMOTE": RSmoteKClasses,
            "Borderline-SMOTE": BorderlineSMOTE,
            "SVM-SMOTE": SVMSMOTE,
            "ADASYN": ADASYN
        }
        smote = smote_algo[config["smote"]](
            random_state=config["smote_random_state"])
        return model, smote
