import os
import json
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account

# = Autenticação Google =
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# = Buscar arquivos no Drive =
folder_id = "16prsjUYZj-fq6ORpQhnWxqMNGTMidKSj"
sheet_input_name = "Financeiro_contas_a_pagar_Vision"
sheet_output_name = "Detalhe_centro_pagamento"

def get_file_id(name):
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Arquivo '{name}' não encontrado na pasta especificada.")
    return files[0]["id"]

input_sheet_id = get_file_id(sheet_input_name)
output_sheet_id = get_file_id(sheet_output_name)

# = Leitura do Google Sheets diretamente para o Pandas =
sheet_range = "A:Z"
result = sheets_service.spreadsheets().values().get(
    spreadsheetId=input_sheet_id,
    range=sheet_range
).execute()
values = result.get('values', [])
df_base = pd.DataFrame(values[1:], columns=values[0])

ids = df_base["financialEvent.id"].dropna().unique()
print(f"📥 Planilha carregada com {len(ids)} IDs únicos.")

# = Configuração da API Conta Azul =
headers = {
    'X-Authorization': '64057706-c700-4036-9cf0-c4b3ed44c594',
    'User-Agent': 'Mozilla/5.0'
}

# = Função para extrair todos os campos aninhados =
def extract_fields(item):
    resultado = []
    base_id = item.get("id")
    
    # Obter observation
    observation = item.get("observation", "")
    
    # Verificar se existem attachments
    attachments = item.get("attachments", [])
    tem_attachments_api = "Sim" if attachments and len(attachments) > 0 else "Não"
    
    # **NOVA CONDICIONAL**: Se observation contiver "desconsiderar anexo", definir como "Sim"
    if "desconsiderar anexo" in observation.lower():
        tem_attachments = "Sim"
    else:
        tem_attachments = tem_attachments_api
    
    categories = item.get("categoriesRatio", [])
    for cat in categories:
        linha = {"id": base_id}
        
        # Adicionar as informações sobre attachments e observation em cada linha
        linha["tem_attachments"] = tem_attachments
        linha["observation"] = observation
        
        for k, v in cat.items():
            if k == "costCentersRatio":
                for i, centro in enumerate(v):
                    for ck, cv in centro.items():
                        linha[f"categoriesRatio.costCentersRatio.{i}.{ck}"] = cv
            else:
                linha[f"categoriesRatio.{k}"] = v
        resultado.append(linha)
    
    # Se não houver categoriesRatio, ainda assim criar uma linha com o ID, status dos attachments e observation
    if not categories:
        linha = {"id": base_id, "tem_attachments": tem_attachments, "observation": observation}
        resultado.append(linha)
    
    return resultado

# = Coleta paralela dos detalhes via API =
def fetch_detail(fid):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{fid}/summary"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return extract_fields(response.json())
        else:
            print(f"❌ Erro no ID {fid}: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Falha no ID {fid}: {e}")
    return None

print("🚀 Iniciando requisições paralelas...")
todos_detalhes = []

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_detail, fid) for fid in ids]
    for f in as_completed(futures):
        resultado = f.result()
        if resultado:
            todos_detalhes.extend(resultado)

print(f"✅ Coleta finalizada com {len(todos_detalhes)} registros.")

# = Enviar dados ao Google Sheets =
df_detalhes = pd.DataFrame(todos_detalhes)

# Reorganizar as colunas para colocar 'observation' e 'tem_attachments' no final
colunas_especiais = ['tem_attachments', 'observation']
if any(col in df_detalhes.columns for col in colunas_especiais):
    colunas = [col for col in df_detalhes.columns if col not in colunas_especiais]
    # Adicionar as colunas especiais na ordem desejada (se existirem)
    for col in colunas_especiais:
        if col in df_detalhes.columns:
            colunas.append(col)
    df_detalhes = df_detalhes[colunas]

# Limpar conteúdo anterior da planilha
sheets_service.spreadsheets().values().clear(
    spreadsheetId=output_sheet_id,
    range="A:Z"
).execute()

# Enviar os dados
values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").astype(str).values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=output_sheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("📊 Dados atualizados na planilha com sucesso.")
