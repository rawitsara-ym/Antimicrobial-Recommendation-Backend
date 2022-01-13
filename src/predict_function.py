import pandas as pd
import joblib


def predictor():
    with open("./ml_model/schema/schema.txt", 'r') as schema_file:
        schema = eval(schema_file.read())
    model_gp = {anti: joblib.load(f"./ml_model/GP/{anti}.joblib")
                for anti in schema["GP"].keys()}
    model_gn = {anti: joblib.load(f"./ml_model/GN/{anti}.joblib")
                for anti in schema["GN"].keys()}
    # collect all schemas
    schema_gp_all = set()
    schema_gn_all = set()
    for anti in schema["GP"]:
        schema_gp_all.update(set(schema["GP"][anti]))
    for anti in schema["GN"]:
        schema_gn_all.update(set(schema["GN"][anti]))
    # create a DataFrame with all schemas
    schema_gp_df = pd.DataFrame(data=[[0]*len(schema_gp_all)], columns=schema_gp_all)
    schema_gn_df = pd.DataFrame(data=[[0]*len(schema_gn_all)], columns=schema_gn_all)

    def prediction(data: pd.Series):
        nonlocal schema
        result = []
        dummies_data_origin = pd.get_dummies(pd.DataFrame(data).T)
        if data.vitek_id == "GP":
            nonlocal model_gp, schema_gp_df
            dummies_df = get_dummies_dataframe_columns(schema_gp_df, dummies_data_origin)
            schema_data = schema["GP"]
            ml_model = model_gp
        elif data.vitek_id == "GN":
            nonlocal model_gn, schema_gn_df
            dummies_df = get_dummies_dataframe_columns(schema_gn_df, dummies_data_origin)
            schema_data = schema["GN"]
            ml_model = model_gn
        else:
            return "Not predictable."
        
        for anti in schema_data.keys():
            model = ml_model[anti]
            dummies_data = dummies_df.filter(schema_data[anti])
            anti = anti.replace("_", '/')
            result_single = {
                "antimicrobial": anti,
                "score": model.predict_proba(dummies_data)[:, 1][0],
            }
            if result_single["score"] >= 0.5 :
                result.append(result_single)
        list_sorted = sorted(result, key=lambda item: item['score'], reverse=True)
        return {item['antimicrobial']: round(float(item['score'])*100, 2) for item in list_sorted}
        
    return prediction


def get_dummies_dataframe_columns(new_df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
    old_df = old_df.filter(new_df.columns)
    new_df.update(old_df)
    return new_df
