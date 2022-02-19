from fastapi import Depends, FastAPI, File, UploadFile, BackgroundTasks
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
import os
import time
import datetime
import shutil
import asyncio
from src.model import PetDetail
from src.predictor import Predictior
from src.table_to_csv import TableToCsv
from src.upload_validator import UploadValidator
from src.upload_tranformation import UploadTranformation

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

app = FastAPI()
conn = sqlalchemy.create_engine(
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/antimicrobial_system")

predictor = Predictior(conn)


table_csv = {'GN': TableToCsv(conn, 1), 'GP': TableToCsv(conn, 2)}


@app.get("/api/species/")
async def species():
    species = pd.read_sql_query(
        "SELECT id , name FROM public.species", conn)
    return {
        "status": "success",
        "data": {
            "species": [{
                'id': row[0],
                'name': row[1]
            } for row in species.values]
        }
    }


@app.get("/api/vitek_id/")
async def vitek_id():
    vitek_id = pd.read_sql_query(
        "SELECT id , name FROM public.vitek_id_card", conn)
    return {
        "status": "success",
        "data": {
            "vitek_id": [
                {
                    'id': row[0],
                    'name': row[1]
                } for row in vitek_id.values
            ]
        }
    }


@app.get("/api/bacteria_genus/")
async def bacteria_genus():
    bacteria_genus = pd.read_sql_query(
        "SELECT id , name FROM public.bacteria_genus", conn)
    return {
        "status": "success",
        "data": {
            "bacteria_genus": [
                {
                    'id': row[0],
                    'name': row[1]
                } for row in bacteria_genus.values
            ]
        }
    }


@app.get("/api/submitted_sample/")
async def submitted_sample():
    submitted_sample_binning_latest = pd.read_sql_query(
        """
        SELECT public.submitted_sample_binning_model_group.schema
	    FROM public.submitted_sample_binning_model_group
	    INNER JOIN public.model_group ON public.model_group.id = public.submitted_sample_binning_model_group.model_group_id
	    WHERE (public.model_group.vitek_id , public.model_group.version) IN (
		    SELECT vitek_id , MAX(version)
		    FROM public.model_group
		    GROUP BY vitek_id);
        """, conn)

    submitted_all = set()
    for sub_sam in submitted_sample_binning_latest.values:
        submitted_all.update(set(eval(sub_sam[0])))

    query = sqlalchemy.text(
        "SELECT id , name FROM public.submitted_sample WHERE name IN :sub_all")

    submitted_sample = pd.read_sql_query(
        query, conn, params={'sub_all': tuple(submitted_all)})

    return {
        "status": "success",
        "data": {
            "submitted_sample": [
                {
                    'id': row[0],
                    'name': row[1]
                } for row in submitted_sample.values
            ]
        }
    }


@app.get("/api/antimicrobial_sir/")
async def antimicrobial_sir(v_id):
    query = sqlalchemy.text(
        "SELECT id , name , sir_type_id FROM public.antimicrobial_sir WHERE vitek_id = :v_id")
    antimicrobial_sir = pd.read_sql_query(query, conn, params={"v_id": v_id})

    query = sqlalchemy.text(
        "SELECT id , name FROM public.sir_type WHERE id IN :sir_type")
    sir_type = pd.read_sql_query(
        query, con=conn, params={"sir_type": tuple(int(i) for i in antimicrobial_sir["sir_type_id"].unique())})

    query = sqlalchemy.text(
        "SELECT id , sir_type_id ,  symbol FROM public.sir_sub_type WHERE sir_type_id IN :sir_sub_type")
    sir_sub_type = pd.read_sql_query(
        query, con=conn, params={"sir_sub_type": tuple(int(i) for i in antimicrobial_sir["sir_type_id"].unique())})

    return {
        "status": "success",
        "data": {
            "antimicrobial": [
                {
                    "id": row[0],
                    "name": row[1],
                    "sir_type": row[2]
                } for row in antimicrobial_sir.values],
            "sir_type": [
                {
                    "id": row[0],
                    "name": row[1],
                    "sub_type": [
                        {
                            "id": sub_row[0],
                            "name": sub_row[2]
                        }
                        for sub_row in sir_sub_type.values if sub_row[1] == row[0]
                    ]
                } for row in sir_type.values]
        }
    }


@app.post("/api/predict/")
async def predict(petDetail: PetDetail):
    species = petDetail.species.lower().strip()
    bact_genus = petDetail.bact_genus.lower().strip()
    submitted_sample = petDetail.submitted_sample.lower().strip()
    vitek_id = petDetail.vitek_id.upper().strip()

    data = dict()
    data['species'] = species
    data['bact_genus'] = bact_genus
    data['submitted_sample'] = submitted_sample
    data['vitek_id'] = vitek_id
    for key, value in petDetail.sir.items():
        value = value.upper()
        data[f'S/I/R_{key.lower().strip()}'] = {'POS': '+',
                                                'NEG': '-'}.get(value, value)

    v_id = {"GN": 0, "GP": 1}.get(vitek_id, -1)

    # predict answer
    if v_id != -1:
        result = predictor.predict(pd.Series(data), v_id)
        return {
            "status": "success",
            "data":
            {
                "answers": result
            }
        }
    else:
        return {
            "status": "failed",
            "message": "vitek_id must have GN or GP only."
        }


async def uploading(vitek_id: int, uploadfile: dict):
    # uploadfile = {"id": id_upload, "filename": in_file.filename,
    #               "filepath": filepath, "start_date": upload_date}
    def upload_result_func(detail: list, type: str, log_id: int):
        res = pd.DataFrame(detail, columns=["detail"])
        res["type"] = type
        res["upload_file_log_id"] = log_id
        res.to_sql('upload_file_result', schema='public',
                   con=conn, if_exists='append', index=False)

    file_upload = pd.read_csv(uploadfile["filepath"])
    # reset index
    file_upload.index = file_upload.index + 2  # start at 1 + header

    vitek = ['GN', 'GP'][vitek_id - 1]
    upload_validator = UploadValidator(table_csv[vitek])
    result = upload_validator.validate(file_upload)
    if result[0]:
        # File Result
        if result[1] == "warning":
            upload_result_func(result[2], result[1], uploadfile["id"])

        # File
        with conn.connect() as con:
            query = sqlalchemy.text(
                "INSERT INTO public.file(name, upload_at,active,vitek_id) VALUES (:name, :date, true , :v_id) RETURNING id;")
            rs = con.execute(query, name=uploadfile["filename"],
                             date=uploadfile["start_date"], v_id=vitek_id)
            for row in rs:
                file_id = row[0]

        # Report
        uploader = UploadTranformation(vitek_id, conn)
        row_count = uploader.upload(file_upload, file_id)

        # Reload Table
        table_csv[vitek].startup()

        status = "success"
    else:
        upload_result_func(result[2], result[1], uploadfile["id"])
        row_count = 0
        status = "fail"

    # Update Filelog
    finish_date = datetime.datetime.now()
    delta_time = (finish_date - uploadfile["start_date"]).seconds
    with conn.connect() as con:
        query = sqlalchemy.text(
            """
            UPDATE public.upload_file_log
            SET finish_date=:f_date, "time"=:time, amount_row=:count, status=:status
            WHERE id = :id""")
        rs = con.execute(query, id=uploadfile["id"], f_date=finish_date,
                         time=delta_time, count=row_count, status=status)

    # Delete Temp File
    os.remove(uploadfile["filepath"])
    # print("finish")


@app.post("/api/upload/")
async def upload(vitek_id: int, background_tasks: BackgroundTasks, in_file: UploadFile = File(...),):
    start_time = time.time()
    upload_date = datetime.datetime.now()
    filepath = f"./upload_file/{hash(start_time)}_{in_file.filename}"
    with open(filepath, mode="wb") as out_file:
        shutil.copyfileobj(in_file.file, out_file)
    with conn.connect() as con:
        query = sqlalchemy.text(
            "INSERT INTO public.upload_file_log(filename, start_date,status) VALUES (:filename, :date, 'pending') RETURNING id;")
        rs = con.execute(query, filename=in_file.filename,
                         date=upload_date)
        for row in rs:
            id_upload = row[0]
    uploadfile = {"id": id_upload, "filename": in_file.filename,
                  "filepath": filepath, "start_date": upload_date}
    background_tasks.add_task(
        uploading, vitek_id, uploadfile)
    return {
        "status": "success",
        "data":
        {
            "filename": in_file.filename,
            "start_date": upload_date,
        }
    }


@app.get("/api/upload_logs/")
async def upload_logs(page: int = 1):
    # show list
    AMOUNT_PER_PAGE = 10
    offset = (page - 1) * AMOUNT_PER_PAGE

    if offset < 0:
        return {
            "status": "failed",
        }

    # select count
    with conn.connect() as con:
        rs = con.execute("SELECT COUNT(*) FROM public.upload_file_log ")
        for row in rs:
            count = row[0]

    query = sqlalchemy.text("""
        SELECT *
        FROM public.upload_file_log
        ORDER BY start_date DESC
        LIMIT :app
        OFFSET :offset
        """)
    upload_file_logs = pd.read_sql(
        query, conn, params={"app": AMOUNT_PER_PAGE, "offset": offset})

    query = sqlalchemy.text("""
        SELECT *
        FROM public.upload_file_result
        WHERE upload_file_log_id IN :log_id
        """)

    upload_file_results = pd.read_sql(
        query, conn, params={"log_id": tuple(upload_file_logs["id"].tolist())})

    logs = []
    upload_file_logs = upload_file_logs.set_index("id")
    for _id in upload_file_logs.index:
        result_fetch = upload_file_results[upload_file_results["upload_file_log_id"] == _id]
        result = dict()
        if len(result_fetch) == 0:
            result["type"] = "success"
            result["detail"] = []
        else:
            result["type"] = result_fetch["type"].iloc[0]
            result["detail"] = result_fetch["detail"].tolist()
        logs.append({
            "id": int(_id),
            "filename": upload_file_logs.loc[_id, "filename"],
            "start_date": upload_file_logs.loc[_id, "start_date"],
            "finish_date": upload_file_logs.loc[_id, "finish_date"],
            "time": int(upload_file_logs.loc[_id, "time"]),
            "amount_row": int(upload_file_logs.loc[_id, "amount_row"]),
            "status": upload_file_logs.loc[_id, "status"],
            "result": result
        })

    return {
        "status": "success",
        "data":
        {
            "logs": logs,
            "total" : count
        }
    }

# ---------- DASHBOARD ----------

@app.get("/api/lastest_version/")
async def lastest_version(vitek_id):
    query = sqlalchemy.text(
        """ SELECT MAX(version)
            FROM public.model_group
            WHERE vitek_id = :vitek_id
        """)
    lastest_version = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id})

    return {
        "status": "success",
        "data": {
            "lastest_version": int(lastest_version.values[0][0])
        }
    }
    
@app.get("/api/antimicrobial_model/")
async def antimicrobial_model(vitek_id):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_answer.id, public.antimicrobial_answer.name
            FROM public.model
            INNER JOIN public.model_group_model ON model.id = model_group_model.model_id
            INNER JOIN public.model_group ON public.model_group_model.model_group_id = public.model_group.id
            INNER JOIN public.antimicrobial_answer ON public.model.antimicrobial_id = public.antimicrobial_answer.id
            WHERE public.model_group.vitek_id = :vitek_id
            GROUP BY public.antimicrobial_answer.id, public.antimicrobial_answer.name
            ORDER BY public.antimicrobial_answer.name
        """)
    antimicrobial = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id})

    return {
        "status": "success",
        "data": {
            "antimicrobial": [{
                'id': row[0],
                'name': row[1]
            } for row in antimicrobial.values]
        }
    }

# ---------- DATASET DASHBOARD ----------

@app.get("/api/dashboard_case/")
async def dashboard_case(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT to_char(public.report.report_issued_date, 'YYYY-MM'), COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY to_char(public.report.report_issued_date, 'YYYY-MM')
            ORDER BY 1
        """)
    case = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})
    
    return {
        "status": "success",
        "data": {
            "cases": [{
                'date': row[0],
                'count': row[1]
            } for row in case.values]
        }
    }

@app.get("/api/dashboard_species/")
async def dashboard_species(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.species.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id 
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.species ON public.report.species_id = public.species.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.species.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    species = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})
    
    return {
        "status": "success",
        "data": {
            "species": [{
                'name': row[0],
                'count': row[1]
            } for row in species.values]
        }
    }
    
@app.get("/api/dashboard_bacteria_genus/")
async def dashboard_bacteria_genus(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.bacteria_genus.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id 
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.bacteria_genus ON public.report.bacteria_genus_id = public.bacteria_genus.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.bacteria_genus.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    bacteria_genus = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})[:10]
    
    return {
        "status": "success",
        "data": {
            "bacteria_genus": [{
                'name': row[0],
                'count': row[1]
            } for row in bacteria_genus.values]
        }
    }
    
@app.get("/api/dashboard_submitted_sample/")
async def dashboard_submitted_sample(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.submitted_sample.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id 
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.submitted_sample ON public.report.submitted_sample_id = public.submitted_sample.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version 
            GROUP BY public.submitted_sample.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    submitted_sample = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})[:10]
    
    return {
        "status": "success",
        "data": {
            "submitted_sample": [{
                'name': row[0],
                'count': row[1]
            } for row in submitted_sample.values]
        }
    }
    
@app.get("/api/dashboard_antimicrobial_sir/")
async def dashboard_antimicrobial_answer(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_sir.name, 
	            COUNT(CASE WHEN public.sir_sub_type.id=1 THEN 1 END) as "POS",
	            COUNT(CASE WHEN public.sir_sub_type.id=2 THEN 1 END) as "NEG",
 	            COUNT(CASE WHEN public.sir_sub_type.id=3 THEN 1 END) as "S",
	            COUNT(CASE WHEN public.sir_sub_type.id=4 THEN 1 END) as "I",
	            COUNT(CASE WHEN public.sir_sub_type.id=5 THEN 1 END) as "R"
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.report_sir ON public.report.id = public.report_sir.report_id
            INNER JOIN public.antimicrobial_sir ON public.report_sir.antimicrobial_id = public.antimicrobial_sir.id
            INNER JOIN public.sir_sub_type ON public.report_sir.sir_sub_id = public.sir_sub_type.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.antimicrobial_sir.name
            ORDER BY public.antimicrobial_sir.name
        """)
    antimicrobial_sir = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})
    
    return {
        "status": "success",
        "data": {
            "antimicrobial_sir": [{
                'name': row[0],
                'count': {
                    'pos': row[1],
                    'neg': row[2],
                    's': row[3],
                    'i': row[4],
                    'r': row[5]
                }
            } for row in antimicrobial_sir.values]
        }
    }
    
@app.get("/api/dashboard_antimicrobial_answer/")
async def dashboard_antimicrobial_answer(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_answer.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.report_answer ON public.report.id = public.report_answer.report_id
            INNER JOIN public.antimicrobial_answer ON antimicrobial_answer.id = public.report_answer.antimicrobial_id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.antimicrobial_answer.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    antimicrobial_answer = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})[:11]
    
    return {
        "status": "success",
        "data": {
            "antimicrobial_answers": [{
                'name': row[0],
                'count': row[1]
            } for row in antimicrobial_answer.values]
        }
    }
    
# ---------- PERFORMANCE DASHBOARD ----------

@app.get("/api/dashboard_performance_by_antimicrobial/")
async def dashboard_performance_by_antimicrobial(antimicrobial_id):
    query = sqlalchemy.text(
        """ SELECT mg.version, m.accuracy, m.precision, m.recall, m.f1
            FROM public.model as m
            INNER JOIN public.model_group_model ON m.id = public.model_group_model.model_id
            INNER JOIN public.model_group AS mg ON public.model_group_model.model_group_id =  mg.id
            WHERE m.antimicrobial_id = :antimicrobial_id AND mg.version > 0
        """)
    performance = pd.read_sql_query(query, conn, params={"antimicrobial_id": antimicrobial_id})
    
    return {
        "status": "success",
        "data": {
            "performances": [{
                'version': row[0],
                'accuracy': row[1],
                'precision': row[2],
                'recall': row[3],
                'f1': row[4]
            } for row in performance.values]
        }
    }
    
@app.get("/api/dashboard_performance_by_version/")
async def dashboard_performance_by_version(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_answer.name, m_group.version, m.accuracy, m.precision, m.recall, m.f1, m.performance, m.id
            FROM public.model_group as mg
            INNER JOIN public.model_group_model as mgm ON mg.id = mgm.model_group_id
            INNER JOIN public.model as m ON mgm.model_id = m.id
            INNER JOIN public.antimicrobial_answer ON m.antimicrobial_id = public.antimicrobial_answer.id
            INNER JOIN (
            	SELECT *
            	FROM public.model_group_model
            	INNER JOIN public.model_group ON public.model_group_model.model_group_id = public.model_group.id
             	WHERE public.model_group.version > 0
            	) as m_group ON m.id = m_group.model_id
            WHERE mg.vitek_id = :vitek_id AND mg.version = :version
            ORDER BY public.antimicrobial_answer.name
        """)
    
    query_test_by_case = sqlalchemy.text(
        """ SELECT 'All Model (Test By Case)' as name, mg.version, mg.accuracy, mg.precision, mg.recall, mg.f1, '-' as performance, mg.id
            FROM public.model_group as mg
            WHERE mg.vitek_id = :vitek_id AND mg.version = :version
        """
    )
    
    params = {"vitek_id": vitek_id, "version": version}
    performance = pd.read_sql_query(query, conn, params=params)
    test_by_case = pd.read_sql_query(query_test_by_case, conn, params=params)
    performance = performance.append(test_by_case)

    return {
        "status": "success",
        "data": {
            "performances": [{
                'antimicrobial': row[0],
                'version': row[1],
                'accuracy': row[2],
                'precision': row[3],
                'recall': row[4],
                'f1': row[5],
                'performance': row[6],
                'model_group_id': row[7]
            } for row in performance.values]
        }
    }