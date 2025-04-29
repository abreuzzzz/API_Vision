import requests
import time
import datetime
import pandas as pd
import os
import json
from dateutil.relativedelta import relativedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

# -------- CONFIGURAÇÕES --------
CLIENT_ID = "52imi455hqp3ue31jc8snfm0v3"
CLIENT_SECRET = "trch1gb3ks5eodn4206kcgf8mjpb8rh84cdi8pcd79skc6il3e1"
REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.cKu5Ug_eHOAtQgfti0bcjM_ykCbAF-Ar9UqNAclwCJsHsuwCt1Hrw5jcZC_bRA3kec6eq4A7GwWj6YnkSy8yGSh1vntKYQzbpG7-g3UFp2XfsMUNl_UcCwf_Szo7h6MZe13NdMFUoJcsUtZoZupvb2hCBpJMtyixhAzIodQsJ8HtIDDbugx7B3RshXjme84KsxfUueYlIPnhcRVDzkSlE42afUj8rdToZpCHhmJ8CoUOb0Z8YOPJrL3WE6Vhv3ZHdRw02EgYiX5rhtLPy_kCI3S0XfUcc7yscxFDW1twFYx6G3JI__uUd5gaQLHSYFywBI_ar8xeSAtQhKW3eY7Eyw.fPqB9sa--vd5v5lO.vdr7SeUfmItu5zxPs9Fu1wrKcgnlJcHoXW7z3RBEqJGYX6bWfiBjZ4d7ZlN1zOq3mF3cRAeM8FONhzoLXFoFgRsa9My707oVp5eVE31gmv9PpwWtXhyN2C0JA0h0bzFT2xxmi_vagQz1q3_MmPRZXPVtFhBVkEaBRgWDjUKPMYg4MuYiCvfuMU56SO5BzvT52yf-7Y5LQ4Te9pGof50HyLELWp-mVZgKff_SqKOpwpzPuWOQegOOvp5qg-m_5KHwc7_S7rW53VOxnURwF_8tr87MiPpUSEOFwmXTCpe2B5Ufm-Is9YzZWbYVPfF1bhVwwVU-YJttvqFbZ9DDpedZF38iNhO-nZCazN1hZQ6-qREBekz2t6ywY8F4doofGOAiN89YtP16wUk_qMSPiCbJf6MlaQS6WnFkntiYMt7wz6D50y9EORvyXaKlTg9DYOEXc4eqPsW7KdiXvI8tWLNIKfO6C1N06pA6iZJABdo5bQHxqYcPzAgu-p_nsd_0CKYYYofa3Nps7bcbyN0vl3wGcF3z8UIvWCID4AsgidsK1FfHLOn8pAjFaDT6lwdRaxVyS9XG435WkfaMiEVv4YwnnFV4wTsem5JXif2eTn-UumWsX2r5RAaRM0yT4lzcN39oVzZnTcoVD_Uu-deGOV4RmEroUa8PL6yG4XXKaRH2GOu42kyaDbRs2Ley5SZN73UV5XjXwFLB_hO6NS0lKr45iZLyBZzDcd408a0fANi7wx1JdO_arPJ3ZnQPqmU5CEZfJb9RkpgpLTtQUvqm3-y6gu6ri6zpgYCqlD_Ek-H3LU6qmWDvaL9bO7XD4TtyM5im_hOkuayMQ7cuzwF_yspbnYwZj-UQGYliid9zfWHTDnVPMP1RsRemqbtM65Eag9vTPlWV87PYbJuRQXmNFJPYZQNnnthP7zYEgJIv8Y9ywmyphkCK89vePz7AWp_9iozxhabIV9rZf67vAkZwU07YQ7gLX8RZmkL84XusdzYT9ia8OCWDl9qc3b-jWtw0tZ2UiB3ci9bCiMebhOthJu2NkuR2t-tUspqV5CnX41f6q5q3S6Di8WFGpNrh78xliDlfLMqnssK-8wWhefePNDrt-RiunX7Wn2efixYGVwdoSgqiSTOKAZHu20SbzKuloinsXUUp5tq3ssoNO-SpLR1eBy7g87yGQ3qL2eDGijiwsetbJFdSPlAY-k0-75zLrak3YIub35EGB8YQjer4AKUKmW-PCEWdUX7UPpoHQILn1JVagmc2pKQYcED8TDLKWXecGai-sGTK8oIM3nBsYM77YzMUTr6ppVkVfgTiCs2nlN99kHMiMF2AxRFb_fS3_zyr_StlNygDTMLPXhKpDV50lp814fooFw.rC3_vclt-bxTEhOHGwz11Q"  # ou coloque diretamente aqui
CSV_FILE_NAME = "contas_a_pagar_centro.csv"
PASTA_ID = "16prsjUYZj-fq6ORpQhnWxqMNGTMidKSj"

# -------- AUTENTICAÇÃO --------
def get_access_token():
    url = "https://auth.contaazul.com/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    body = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    response = requests.post(url, data=body, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]

# -------- GOOGLE DRIVE --------
def autenticar_drive_service_account(json_secret):
    gauth = GoogleAuth()
    credentials_dict = json.loads(json_secret)
    scope = ["https://www.googleapis.com/auth/drive"]

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gauth.credentials = credentials
    return GoogleDrive(gauth)

def baixar_csv_drive(drive, filename, folder_id):
    file_list = drive.ListFile({
        "q": f"'{folder_id}' in parents and title = '{filename}' and trashed=false"
    }).GetList()
    
    if not file_list:
        return pd.DataFrame()

    file = file_list[0]
    file.GetContentFile("temp.csv")
    return pd.read_csv("temp.csv")

def salvar_csv_drive(drive, df, filename, folder_id):
    df.to_csv("temp.csv", index=False)

    file_list = drive.ListFile({
        "q": f"'{folder_id}' in parents and title = '{filename}' and trashed=false"
    }).GetList()

    if file_list:
        file = file_list[0]
    else:
        file = drive.CreateFile({"title": filename, "parents": [{"id": folder_id}]})

    file.SetContentFile("temp.csv")
    file.Upload()

# -------- API --------
def buscar_centros_de_custo(token):
    url = "https://api-v2.contaazul.com/v1/centro-de-custo"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"pagina": 1, "tamanho_pagina": 1000}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    dados = resp.json()
    centros = dados.get("itens", dados)
    return {c["id"]: c["nome"] for c in centros}

def buscar_eventos(token, inicio, fim, centro_id=None, pagina=1):
    url = "https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "data_vencimento_de": inicio,
        "data_vencimento_ate": fim
    }
    if centro_id:
        payload["ids_centros_de_custo"] = [centro_id]

    params = {"pagina": pagina, "tamanho_pagina": 1000}
    resp = requests.post(url, headers=headers, params=params, json=payload)
    if resp.status_code == 400:
        return []
    resp.raise_for_status()
    return resp.json().get("itens", [])

# -------- SALVAR CSV COM UPDATE --------
def atualizar_csv(df_atual, novos_eventos):
    df_novos = pd.DataFrame(novos_eventos)
    df_novos = df_novos[["id", "status", "descricao", "total", "data_vencimento", "centro_custo_nome"]]

    if df_atual.empty:
        return df_novos

    df_atual = df_atual.astype(str)
    df_novos = df_novos.astype(str)

    # Remove duplicatas com mesmo ID e centro_custo_nome (atualiza se mudou)
    df_sem_novos = df_atual[~df_atual["id"].isin(df_novos["id"])]
    df_mesmos_id = df_atual[df_atual["id"].isin(df_novos["id"])]
    
    df_atualizada = pd.concat([df_sem_novos, df_novos], ignore_index=True)
    df_atualizada = df_atualizada.drop_duplicates(subset=["id", "centro_custo_nome"], keep="last")
    return df_atualizada

# -------- EXECUÇÃO --------
def main():
    token = get_access_token()
    centros = buscar_centros_de_custo(token)
    json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
    drive = autenticar_drive_service_account(json_secret)

    df_csv = baixar_csv_drive(drive, CSV_FILE_NAME, PASTA_ID)

    data_inicio = datetime.date(2025, 1, 1)
    data_fim = datetime.date(2025, 4, 1)

    while data_inicio < data_fim:
        inicio = data_inicio.strftime("%Y-%m-%d")
        fim = (data_inicio + relativedelta(months=1) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Buscando de {inicio} até {fim}")

        for centro_id, centro_nome in centros.items():
            pagina = 1
            while True:
                eventos = buscar_eventos(token, inicio, fim, centro_id=centro_id, pagina=pagina)
                if not eventos:
                    break

                for evento in eventos:
                    evento["centro_custo_nome"] = centro_nome

                df_csv = atualizar_csv(df_csv, eventos)
                pagina += 1
                 time.sleep(1)

        data_inicio += relativedelta(months=1)

    salvar_csv_drive(drive, df_csv, CSV_FILE_NAME, PASTA_ID)

if __name__ == "__main__":
    main()
