# Antimicrobial-Recommendation-Backend
Backend (FastAPI) ของ Web
:)

## API [POST] -> /api/predict/
* **Request body (Example)**\
{\
  &nbsp;"species": "dog",\
  &nbsp;"bact_species": "pseudomonas aeruginosa",\
  &nbsp;"vitek_id": "GN",\
  &nbsp;"submitted_sample": "urine",\
  &nbsp;"sir": {\
  &emsp;"imipenem": "S",\
  &emsp;"amikacin": "I",\
  &emsp;"gentamicin": "I",\
  &emsp;"enrofloxacin": "S",\
  &emsp;"marbofloxacin": "S"\
  &nbsp;}\
}
	
* **Response body [ชื่อยา : %ความมั่นใจของโมเดล] (Example)**\
{\
  &nbsp;"answer": {\
    &emsp;"marbofloxacin": 96.49,\
    &emsp;"amoxicillin/clavulanic acid": 55.17,\
    &emsp;"enrofloxacin": 50.22\
  &nbsp;}\
}

## ชื่อยาต้านจุลชีพ
* amikacin
* amoxicillin/clavulanic acid
* ampicillin
* benzylpenicillin
* cefalexin
* cefalotin
* cefotaxime
* cefovecin
* cefoxitin screen
* cefpodoxime
* ceftiofur
* ceftriaxone
* chloramphenicol
* clindamycin
* doxycycline
* enrofloxacin
* erythromycin
* esbl
* florfenicol
* fusidic acid
* gentamicin
* imipenem
* inducible clindamycin resistance
* levofloxacin
* linezolid
* marbofloxacin
* minocycline
* moxifloxacin
* mupirocin
* neomycin
* nitrofurantoin
* oxacillin
* piperacillin
* polymyxin b
* pradofloxacin
* rifampicin
* teicoplanin
* tetracycline
* tigecycline
* tobramycin
* trimethoprim/sulfamethoxazole
* vancomycin

`*** หมายเหตุ โมเดลไม่รองรับสกุลแบคทีเรีย streptococcus`
