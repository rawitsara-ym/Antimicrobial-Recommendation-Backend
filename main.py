from src.clean_function import *
from src.predict_function import *
from src.predictor import Predictior
from typing import Dict
from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from pydantic import BaseModel
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
import os
import time
import shutil


class PetDetail(BaseModel):
    species: str
    bact_genus: str
    vitek_id: str
    submitted_sample: str
    sir: Dict


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

app = FastAPI()
conn = sqlalchemy.create_engine(
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/antimicrobial_system")

# prediction = predictor()

predictor = Predictior(conn)


@app.get("/api/species/")
async def species():
    species = pd.read_sql_query("SELECT id , name FROM public.species", conn)
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
        data[f'S/I/R_{key.lower().strip()}'] = {"POS": '+',
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


@app.post("/api/logs_upload/")
async def logs_upload():
    upload_file_log = pd.read_sql(
        "SELECT id , name FROM public.upload_file_log", conn)
    return {
        "status": "success",
        "data":
        {
        }
    }


@app.post("/api/upload/")
async def upload(vitek_id, background_tasks: BackgroundTasks, in_file: UploadFile = File(...),):
    with open(f"./upload_file/{hash(time.time())}_{in_file.filename}", mode="wb") as out_file:
        shutil.copyfileobj(in_file.file, out_file)

    return {
        "status": "success",
        "data":
        {
            "filename": "",
            "start_date": ""
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
    cases = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})
    
    return {
        "status": "success",
        "data": {
            "cases": [{
                'date': row[0],
                'count': row[1]
            } for row in cases.values]
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
            INNER JOIN public.sir_sub_type ON public.report_sir.sir_id = public.sir_sub_type.id
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
    antimicrobial_answer = pd.read_sql_query(query, conn, params={"vitek_id": vitek_id, "version": version})[:15]
    
    return {
        "status": "success",
        "data": {
            "antimicrobial_answers": [{
                'name': row[0],
                'count': row[1]
            } for row in antimicrobial_answer.values]
        }
    }