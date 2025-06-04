import os
import json
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# 🔐 Lê o segredo e salva como credentials.json
gdrive_credentials = os.getenv("GDRIVE_SERVICE_ACCOUNT")
with open("credentials.json", "w") as f:
    json.dump(json.loads(gdrive_credentials), f)

# 📌 Autenticação com Google
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# === IDs das planilhas ===
planilhas_ids = {
    "Financeiro_contas_a_receber_Vision": "1wB-f-ywlbYh0L9Mh0pd2XfRyUUyCuF_ba0mpQjWT4S8",
    "Financeiro_contas_a_pagar_Vision": "1zIhxmFzHYmNdKyawzhElFl4Vl1MO3bc6QOapO2ynYwk",
    "Detalhe_centro_pagamento": "1QJmlNhgjfiFo4_BScyW4l7A2Zkk2n5WQsWdQA_8uWU0",
    "Detalhe_centro_recebimento": "1F85E2_p3qlZu9vtkr3s_dCAb86DqT19IROsI_gH8gqQ",
    "Financeiro_Completo_Vision": "1EiJ_GSWLYAamMmmnIJKHDogP3MtMo7yJoZmwy-mjeHY"
}

# === Função para abrir e ler planilha por ID ===
def ler_planilha_por_id(nome_arquivo):
    planilha = client.open_by_key(planilhas_ids[nome_arquivo])
    aba = planilha.sheet1
    df = get_as_dataframe(aba).dropna(how="all")
    return df

# Lê os dados das planilhas
df_receber = ler_planilha_por_id("Financeiro_contas_a_receber_Vision")
df_pagar = ler_planilha_por_id("Financeiro_contas_a_pagar_Vision")
df_pagamento = ler_planilha_por_id("Detalhe_centro_pagamento")
df_recebimento = ler_planilha_por_id("Detalhe_centro_recebimento")

# Adiciona a coluna tipo
df_receber["tipo"] = "Receita"
df_pagar["tipo"] = "Despesa"

# Junta os dois dataframes
df_completo = pd.concat([df_receber, df_pagar], ignore_index=True)

# 1º join com Detalhe_centro_pagamento usando financialEvent.id
df_merge = df_completo.merge(
    df_pagamento,
    how="left",
    left_on="financialEvent.id",
    right_on="id",
    suffixes=('', '_detalhe_pagamento')
)

# Filtra os que ainda não foram encontrados (onde campos de detalhe estão nulos)
nao_encontrados = df_merge[df_merge['id_detalhe_pagamento'].isna()].copy()

# 2º join com Detalhe_centro_recebimento usando financialEvent.id
df_enriquecido = nao_encontrados.drop(columns=[col for col in df_pagamento.columns if col != 'id'])
df_enriquecido = df_enriquecido.merge(
    df_recebimento,
    how='left',
    left_on="financialEvent.id",
    right_on="id",
    suffixes=('', '_detalhe_recebimento')
)

# Atualiza as linhas originais com os detalhes de recebimento
df_merge.update(df_enriquecido)

# Remove linhas com competenceDate maior que hoje
if 'financialEvent.competenceDate' in df_merge.columns:
    df_merge['financialEvent.competenceDate'] = pd.to_datetime(df_merge['financialEvent.competenceDate'], errors='coerce')
    df_merge = df_merge[df_merge['financialEvent.competenceDate'] <= datetime.today()]

# Corrige valores da coluna categoriesRatio.value com base na condição
if 'categoriesRatio.value' in df_merge.columns and 'paid' in df_merge.columns:
    df_merge['categoriesRatio.value'] = df_merge.apply(
        lambda row: row['paid'] if pd.notna(row['categoriesRatio.value']) and pd.notna(row['paid']) and row['categoriesRatio.value'] > row['paid'] else row['categoriesRatio.value'],
        axis=1
    )

# 📄 Abrir a planilha de saída
planilha_saida = client.open_by_key(planilhas_ids["Financeiro_Completo_Vision"])
aba_saida = planilha_saida.sheet1

# Limpa a aba e sobrescreve
aba_saida.clear()
set_with_dataframe(aba_saida, df_merge)

print("✅ Planilha sobrescrita com sucesso.")
