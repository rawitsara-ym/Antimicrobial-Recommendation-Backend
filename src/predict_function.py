import pandas as pd
import joblib


def predictor():
    with open("./ml_model/schema/schema.txt", 'r') as schema_file:
        schema = eval(schema_file.read())
    model_gp = {anti: joblib.load(f"./ml_model/GP/{anti}.joblib")
                for anti in schema["GP"].keys()}
    model_gn = {anti: joblib.load(f"./ml_model/GN/{anti}.joblib")
                for anti in schema["GN"].keys()}

    def prediction(data: pd.Series):
        dummies_data_origin = pd.get_dummies(pd.DataFrame(data).T)
        nonlocal schema
        if data.vitek_id == "GP":
            schema_gp = schema["GP"]
            result = []
            nonlocal model_gp
            for anti in schema_gp.keys():
                model = model_gp[anti]
                dummies_data = get_dummies_dataframe_columns(
                    schema_gp[anti], dummies_data_origin)
                anti = anti.replace("_", '/')
                result_single = {
                    "antimicrobial": anti,
                    "score": model.predict_proba(dummies_data)[:, 1][0],
                }
                if result_single["score"] >= 0.5 :
                    result.append(result_single)
            list_sorted = sorted(
                result, key=lambda item: item['score'], reverse=True)
            return {item['antimicrobial']: round(float(item['score'])*100, 2) for item in list_sorted}
        elif data.vitek_id == "GN":
            schema_gn = schema["GN"]
            result = []
            nonlocal model_gn
            for anti in schema_gn.keys():
                model = model_gn[anti]
                dummies_data = get_dummies_dataframe_columns(
                    schema_gn[anti], dummies_data_origin)
                anti = anti.replace("_", '/')
                result_single = {
                    "antimicrobial": anti,
                    "score": model.predict_proba(dummies_data)[:, 1][0],
                }
                if result_single["score"] >= 0.5 :
                    result.append(result_single)
            list_sorted = sorted(
                result, key=lambda item: item['score'], reverse=True)
            return {item['antimicrobial']: round(float(item['score'])*100, 2) for item in list_sorted}
        else:
            return "Not predictable."
    return prediction


def get_dummies_dataframe_columns(cols_name: list, old_df: pd.DataFrame) -> pd.DataFrame:
    old_df = old_df.filter(cols_name)
    new_df = pd.DataFrame(columns=cols_name).append(old_df)
    new_df.fillna(0, inplace=True)
    return new_df
