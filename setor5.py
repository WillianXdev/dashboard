import streamlit as st
import sqlite3
import pandas as pd
import json
import os
from datetime import date
from calendar import monthrange
import re
from collections import Counter
import pydeck as pdk
import pandas as pd
import streamlit as st
from io import BytesIO
import xlsxwriter

DB_TAREFAS = "data/tarefas.sqlite3"
DB_USUARIOS = "data/usuarios.sqlite3"
DB_EQUIPAMENTOS = "data/db.sqlite3"
DB_CLIENTES = "data/clientes_por_grupo.sqlite3"

st.set_page_config(page_title="Painel Técnico – Todos os Setores", layout="wide")
st.title("🏷️ Painel Técnico – Setor 5")

if not all(
    [
        os.path.exists(DB_TAREFAS),
        os.path.exists(DB_USUARIOS),
        os.path.exists(DB_EQUIPAMENTOS),
        os.path.exists(DB_CLIENTES),
    ]
):
    st.error("❌ Um ou mais bancos de dados necessários não foram encontrados.")
    st.stop()


@st.cache_data
def carregar_tarefas():
    conn = sqlite3.connect(DB_TAREFAS)
    df = pd.read_sql("SELECT * FROM tarefas_raw", conn)
    df["json"] = df["json"].apply(json.loads)
    conn.close()
    return df


@st.cache_data
def carregar_usuarios():
    conn = sqlite3.connect(DB_USUARIOS)
    df = pd.read_sql("SELECT user_id, nome FROM usuarios", conn)
    conn.close()
    return df


@st.cache_data
def carregar_equipamentos():
    conn = sqlite3.connect(DB_EQUIPAMENTOS)
    df = pd.read_sql(
        "SELECT id, name, associated_customer_id FROM equipamentos WHERE ativo = 1",
        conn,
    )
    conn.close()
    return df


@st.cache_data
def carregar_clientes_por_setor():
    conn = sqlite3.connect(DB_CLIENTES)
    setores = {
        "Setor 1": pd.read_sql("SELECT id FROM clientes_grupo_156750", conn)[
            "id"
        ].tolist(),
        "Setor 2": pd.read_sql("SELECT id FROM clientes_grupo_156751", conn)[
            "id"
        ].tolist(),
        "Setor 3": pd.read_sql("SELECT id FROM clientes_grupo_156752", conn)[
            "id"
        ].tolist(),
        "Setor 4": pd.read_sql("SELECT id FROM clientes_grupo_156753", conn)[
            "id"
        ].tolist(),
        "Setor 5": pd.read_sql("SELECT id FROM clientes_grupo_156754", conn)[
            "id"
        ].tolist(),
    }
    conn.close()
    return setores


TIPOS_FIXOS = ["Preventiva Semestral", "Preventiva Mensal", "Corretiva"]

with st.spinner("🔄 Carregando dados..."):
    df_raw = carregar_tarefas()
    df_usuarios = carregar_usuarios()
    equipamentos_df = carregar_equipamentos()
    equipamentos_dict = dict(zip(equipamentos_df["id"], equipamentos_df["name"]))
    clientes_por_setor = carregar_clientes_por_setor()

    df = pd.DataFrame(
        [
            {
                "taskID": row["taskID"],
                "user_id": row["user_id"],
                "data": row["data_referencia"],
                "escola": row["json"].get("customerDescription"),
                "customer_id": row["json"].get("customerId"),
                "tipo": row["json"].get("taskTypeDescription"),
                "status_id": row["json"].get("taskStatus"),
                "checkin": row["json"].get("checkIn"),
                "checkout": row["json"].get("checkOut"),
                "assinatura": row["json"].get("signatureName"),
                "observacao": row["json"].get("report"),
                "equipamentos_id": row["json"].get("equipmentsId"),
                "questionarios": row["json"].get("questionnaires"),
                "taskUrl": row["json"].get("taskUrl"),
                "deliveredDate": row["json"].get("deliveredDate", ""),
                "deliveredOnSmarthPhone": row["json"].get(
                    "deliveredOnSmarthPhone", False
                ),
            }
            for _, row in df_raw.iterrows()
        ]
    )
    # ✅ Filtro oficial para ignorar tarefas inválidas ou canceladas
    df = df[
        ~(
            (df["deliveredOnSmarthPhone"] == True)
            & (df["deliveredDate"] == "0001-01-01T00:00:00")
        )
    ]

    df["status"] = (
        df["status_id"]
        .map(
            {
                1: "Aberta",
                2: "Em Deslocamento",
                3: "Check-in",
                4: "Check-out",
                5: "Finalizada",
                6: "Pausada",
            }
        )
        .fillna("Desconhecido")
    )

    df = pd.merge(df, df_usuarios, how="left", on="user_id")
    df["data"] = pd.to_datetime(df["data"]).dt.date

setor_escolhido = "Setor 5"
clientes_do_setor = clientes_por_setor[setor_escolhido]
df = df[df["customer_id"].isin(clientes_do_setor)]

col2, col3 = st.columns(2)
with col2:
    status_filtro = st.multiselect(
        "📌 Status",
        sorted(df["status"].dropna().unique()),
        default=sorted(df["status"].dropna().unique()),
    )
with col3:
    tipo_filtro = st.multiselect("🧰 Tipo da Tarefa", TIPOS_FIXOS, default=TIPOS_FIXOS)

hoje = date.today()
ano, mes = hoje.year, hoje.month
primeiro_dia_mes = date(ano, mes, 1)
ultimo_dia_mes = date(ano, mes, monthrange(ano, mes)[1])

col4, col5, _ = st.columns(3)
with col4:
    data_ini = st.date_input("📅 De", value=primeiro_dia_mes)
with col5:
    data_fim = st.date_input("📅 Até", value=ultimo_dia_mes)

st.markdown(
    f"🗓️ **Período Selecionado:** {data_ini.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}"
)

df_filt = df[
    df["status"].isin(status_filtro)
    & df["tipo"].apply(lambda x: any(t in x for t in tipo_filtro if isinstance(x, str)))
    & (df["data"] >= data_ini)
    & (df["data"] <= data_fim)
]


# Função para contar equipamentos respondidos
def contar_equipamentos_respondidos(df_status):
    return sum(
        len(
            {
                q.get("questionnaireEquipamentId")
                for q in row["questionarios"]
                if q.get("questionnaireEquipamentId")
            }
        )
        for _, row in df_status.iterrows()
    )


# Equipamentos finalizados (respondidos em tarefas finalizadas)
finalizadas = contar_equipamentos_respondidos(
    df_filt[df_filt["status"] == "Finalizada"]
)

# Equipamentos em pausa (respondidos em tarefas pausadas)
pausadas = contar_equipamentos_respondidos(df_filt[df_filt["status"] == "Pausada"])

# Equipamentos em aberto (esperados em tarefas nem finalizadas nem pausadas)
em_aberto = sum(
    len(row["equipamentos_id"] or [])
    for _, row in df_filt[~df_filt["status"].isin(["Finalizada", "Pausada"])].iterrows()
)

# Exibir no painel
st.info(
    f"📊 Equipamentos Finalizados: **{finalizadas}** | Em Pausa: **{pausadas}** | Em Aberto: **{em_aberto}**"
)


# Corrigir aqui: equipamentos_df precisa ser um DataFrame com a coluna associated_customer_id
esperado_prev_mensal = equipamentos_df[
    equipamentos_df["associated_customer_id"].isin(clientes_do_setor)
].shape[0]

mensal_realizados = 0
semestral_realizados = 0

st.subheader("🗓️ Acompanhamento da Preventiva Mensal")

# 🔍 Filtra tarefas mensais
df_mensal = df_filt[
    df_filt["tipo"].str.contains("Preventiva Mensal", case=False, na=False)
]

# ✅ Conta equipamentos respondidos nas tarefas mensais
mensal_realizados = sum(
    len(
        {
            q.get("questionnaireEquipamentId")
            for q in (row.get("questionarios") or [])
            if q.get("questionnaireEquipamentId")
        }
    )
    for _, row in df_mensal.iterrows()
)

# ✅ Conta esperados nas tarefas mensais (se quiser adicionar)
mensal_esperados = sum(
    len(row.get("equipamentos_id") or []) for _, row in df_mensal.iterrows()
)

# 🎯 Exibir na tela
col1, col2, col3 = st.columns(3)

col1.metric("📋 Total de Equipamentos", esperado_prev_mensal)
col2.metric("✅ Prev. Mensal", mensal_realizados)

faltam_prev_mensal = esperado_prev_mensal - mensal_realizados
col3.metric(
    "⚠️ Faltam Prev. Mensal (Só Tarefa Mensal FINALIZADAS)",
    faltam_prev_mensal if faltam_prev_mensal > 0 else 0,
)


# 🔻 Linha de baixo – Acompanhamento da Preventiva Semestral
st.markdown("---")
st.subheader("📅 Acompanhamento da Preventiva Semestral")

# Filtra somente tarefas do tipo Preventiva Semestral
df_semestral = df_filt[
    df_filt["tipo"].str.contains("Preventiva Semestral", case=False, na=False)
]

# Conta equipamentos respondidos nas tarefas semestrais
semestral_realizados = sum(
    len(
        {
            q.get("questionnaireEquipamentId")
            for q in (row.get("questionarios") or [])
            if q.get("questionnaireEquipamentId")
        }
    )
    for _, row in df_semestral.iterrows()
)

# Conta quantos equipamentos eram esperados nas tarefas semestrais
semestral_esperados = sum(
    len(row.get("equipamentos_id") or []) for _, row in df_semestral.iterrows()
)

# Exibe os indicadores
col4, col5, col6 = st.columns(3)

col4.metric("📋 Total de Equipamentos", esperado_prev_mensal)
col5.metric("✅ Prev. Semestral Realizada", semestral_realizados)

faltam_semestral = esperado_prev_mensal - semestral_realizados
col6.metric(
    "⚠️ Faltam Prev. Semestral",
    faltam_semestral if faltam_semestral > 0 else 0,
)


def obter_status_predominante(df_tarefas):
    contagem = Counter(df_tarefas["status"])
    if contagem:
        return contagem.most_common(1)[0][0]
    return "Desconhecido"


def definir_cor(status):
    if status == "Finalizada":
        return [0, 200, 0]  # Verde
    elif status == "Pausada":
        return [255, 165, 0]  # Laranja
    elif status == "Aberta":
        return [200, 0, 0]  # Vermelho
    else:
        return [150, 150, 150]  # Cinza para status desconhecidos


# Mapa interativo
with st.expander("🗺️ Visualizar Mapa de Escolas e Equipamentos"):
    conn = sqlite3.connect(DB_CLIENTES)
    escolas_df = pd.read_sql(
        """
        SELECT id, description AS nome, latitude, longitude FROM clientes_grupo_156750
        UNION ALL SELECT id, description AS nome, latitude, longitude FROM clientes_grupo_156751
        UNION ALL SELECT id, description AS nome, latitude, longitude FROM clientes_grupo_156752
        UNION ALL SELECT id, description AS nome, latitude, longitude FROM clientes_grupo_156753
        UNION ALL SELECT id, description AS nome, latitude, longitude FROM clientes_grupo_156754
        """,
        conn,
    )
    conn.close()

    escolas_filtradas = escolas_df[
        escolas_df["id"].isin(df_filt["customer_id"].unique())
    ]

    if not escolas_filtradas.empty:
        dados_mapa = []

        for _, row_escola in escolas_filtradas.iterrows():
            escola_id = row_escola["id"]
            nome_escola = row_escola["nome"]
            lat = row_escola["latitude"]
            lon = row_escola["longitude"]

            # Filtrar tarefas para esta escola
            tarefas_escola = df_filt[df_filt["customer_id"] == escola_id]

            # Contar equipamentos e pendências
            equipamentos_total = sum(
                [
                    (
                        len(t["equipamentos_id"])
                        if isinstance(t["equipamentos_id"], list)
                        else 0
                    )
                    for _, t in tarefas_escola.iterrows()
                ]
            )

            pendencias_total = 0
            pausadas_total = len(tarefas_escola[tarefas_escola["status"] == "Pausada"])
            equipamentos_lista = []

            for _, t in tarefas_escola.iterrows():
                eq_ids = t["equipamentos_id"] or []
                questionarios = t["questionarios"] or []
                eq_q = [
                    q.get("questionnaireEquipamentId")
                    for q in questionarios
                    if q.get("questionnaireEquipamentId")
                ]
                pendencias_total += len(set(eq_ids) - set(eq_q))
                equipamentos_lista.extend(
                    [equipamentos_dict.get(eq, f"ID {eq}") for eq in eq_ids]
                )

            # Criar tooltip HTML
            equipamentos_html = "<br>".join(
                [f"- {nome}" for nome in equipamentos_lista]
            )
            tooltip = (
                f"<b>{nome_escola}</b><br>"
                f"Total de equipamentos: {equipamentos_total}<br>"
                f"🔧 Pendências: {pendencias_total}<br>"
                f"⏸️ Em Pausa: {pausadas_total}<br>"
                f"📊 Finalizadas: {len(tarefas_escola[tarefas_escola['status'] == 'Finalizada'])} | "
                f"Em Pausa: {pausadas_total} | "
                f"Em Aberto: {len(tarefas_escola) - len(tarefas_escola[tarefas_escola['status'] == 'Finalizada']) - pausadas_total}"
            )

            if equipamentos_html:
                tooltip += f"<br><br>{equipamentos_html}"

            # Determinar status predominante
            status_predominante = obter_status_predominante(tarefas_escola)

            # Adicionar à lista de dados do mapa
            dados_mapa.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "tooltip": tooltip,
                    "status": status_predominante,
                }
            )

        # Criar DataFrame do mapa
        mapa_df = pd.DataFrame(dados_mapa)

        # Definir cores baseadas no status
        mapa_df["cor"] = mapa_df["status"].apply(definir_cor)

        # Criar layer para o mapa
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=mapa_df,
            get_position="[lon, lat]",
            get_color="cor",
            get_radius=100,
            pickable=True,
        )

        # Exibir o mapa
        st.pydeck_chart(
            pdk.Deck(
                map_style="mapbox://styles/mapbox/light-v9",
                initial_view_state=pdk.ViewState(
                    latitude=mapa_df["lat"].mean(),
                    longitude=mapa_df["lon"].mean(),
                    zoom=11,
                    pitch=0,
                ),
                layers=[layer],
                tooltip={"html": "{tooltip}"},
            )
        )
    else:
        st.warning("Nenhuma escola com dados disponíveis para exibir no mapa.")

# Exibir informações detalhadas das tarefas

for _, row in df_filt.iterrows():
    equipamentos_ids = list(set(row.get("equipamentos_id") or []))  # remove duplicatas
    qtd_equip = len(equipamentos_ids)
    questionarios = row.get("questionarios") or []

    equipamentos_q = list(
        {
            q.get("questionnaireEquipamentId")
            for q in questionarios
            if q.get("questionnaireEquipamentId")
        }
    )

    tipo_limpo = (
        re.sub(r"^# .*? - ", "", row["tipo"].strip())
        if isinstance(row["tipo"], str)
        else row["tipo"]
    )

    pendentes = list(set(equipamentos_ids) - set(equipamentos_q))
    pendentes_count = len(pendentes)
    pendente_icone = " ⚠️" if pendentes_count > 0 else " 🟢"

    data_formatada = pd.to_datetime(row["data"]).strftime("%d/%m/%Y")

    titulo = (
        f"🏫 Escola: {row['escola']} - {tipo_limpo} (por {row['nome']}) "
        f"- Equipamentos - {qtd_equip} ({pendentes_count} pendentes){pendente_icone}"
    )

    with st.expander(titulo):
        if row.get("taskUrl"):
            tarefa_numero = row.get("taskID", "Sem número")
            st.markdown(
                f"[🔗 Abrir tarefa na Auvo, #{tarefa_numero}]({row['taskUrl']})"
            )

        st.markdown(f"**📅 Data:** {data_formatada}")
        st.markdown(f"**📌 Status:** {row['status']}")
        st.markdown(f"**✅ Check-in:** {'Sim' if row['checkin'] else 'Não'}")
        st.markdown(f"**✅ Check-out:** {'Sim' if row['checkout'] else 'Não'}")
        st.markdown(f"**🔢 Equipamentos esperados:** {qtd_equip}")
        st.markdown(f"**📝 Observação:** {row['observacao'] or '-'}")
        st.markdown(f"**✍️ Assinatura:** {row['assinatura'] or 'Não assinado'}")

        col_eq1, col_eq2 = st.columns(2)

        with col_eq1:
            st.markdown("### 🛠 Equipamentos no Questionário")
            if equipamentos_q:
                for eq in equipamentos_q:
                    nome = equipamentos_dict.get(eq, f"ID {eq} ⚠️ Inativo na Auvo")
                    st.markdown(f"- {nome}")
            else:
                st.warning("Nenhum equipamento informado no questionário.")

        with col_eq2:
            st.markdown("### 🧩 Equipamentos do Local")
            if equipamentos_ids:
                for eq in equipamentos_ids:
                    nome = equipamentos_dict.get(eq, f"ID {eq} ⚠️ Inativo na Auvo")
                    icone = "🟢" if eq in equipamentos_q else "⚠️"
                    st.markdown(f"- {nome} {icone}")
            else:
                st.warning("Nenhum equipamento registrado no local.")

        st.markdown("### ⚠️ Equipamentos pendentes (não respondidos)")
        if pendentes:
            for p in pendentes:
                nome = equipamentos_dict.get(p, f"ID {p} ⚠️ Inativo na Auvo")
                st.error(f"- {nome}")
        else:
            st.success("Todos os equipamentos foram respondidos.")


# Exportar dados
df_export = df_filt[
    [
        "data",
        "nome",
        "escola",
        "tipo",
        "status",
        "observacao",
        "taskUrl",
        "equipamentos_id",
        "questionarios",
    ]
].copy()
df_export["Equipamentos Esperados"] = df_export["equipamentos_id"].apply(
    lambda x: len(x) if isinstance(x, list) else 0
)
df_export["Equipamentos Respondidos"] = df_export["questionarios"].apply(
    lambda qs: (
        len([q for q in qs if q.get("questionnaireEquipamentId")])
        if isinstance(qs, list)
        else 0
    )
)
df_export.rename(
    columns={
        "data": "Data",
        "nome": "Prestador",
        "escola": "Escola",
        "tipo": "Tipo da Tarefa",
        "status": "Status",
        "observacao": "Observação",
        "taskUrl": "Link da Tarefa",
        "equipamentos_id": "_equip",
        "questionarios": "_quest",
    },
    inplace=True,
)
df_export = df_export[
    [
        "Data",
        "Prestador",
        "Escola",
        "Tipo da Tarefa",
        "Status",
        "Observação",
        "Equipamentos Esperados",
        "Equipamentos Respondidos",
        "Link da Tarefa",
        "_equip",
    ]
]


# 🔍 Função para converter IDs de equipamentos em nomes
def listar_equipamentos(equip_ids):
    if not equip_ids:
        return ""
    return ", ".join([equipamentos_dict.get(e, f"ID {e}") for e in equip_ids])


total_equipamentos = sum(
    len(row.get("equipamentos_id") or []) for _, row in df_filt.iterrows()
)
total_escolas = df_filt["escola"].nunique()

with st.expander(
    f"📦 Ver lista de equipamentos ({total_escolas} escolas / {total_equipamentos} equipamentos)"
):
    equipamentos_lista = []

    for _, row in df_filt.iterrows():
        escola = row["escola"]
        equipamentos_ids = row.get("equipamentos_id") or []
        nomes = [equipamentos_dict.get(e, f"ID {e}") for e in equipamentos_ids]

        equipamentos_lista.append(
            f"🏫 {escola}: {', '.join(nomes) if nomes else 'Sem equipamentos cadastrados'}"
        )

    for linha in equipamentos_lista:
        st.markdown(f"- {linha}")

df_export["Lista de Equipamentos"] = df_export["_equip"].apply(listar_equipamentos)
df_export.drop(columns=["_equip"], inplace=True)
df_export["Data"] = pd.to_datetime(df_export["Data"]).dt.strftime("%d/%m/%Y")

# 🔥 Gerar DataFrame formatado com sublinhas de equipamentos

linhas = []

for _, row in df_export.iterrows():
    equipamentos = (
        row["Lista de Equipamentos"].split(", ")
        if row["Lista de Equipamentos"]
        else ["Sem equipamentos"]
    )

    primeira = True
    for equipamento in equipamentos:
        if primeira:
            linhas.append(
                {
                    "Data": row["Data"],
                    "Prestador": row["Prestador"],
                    "Escola": row["Escola"],
                    "Tipo da Tarefa": row["Tipo da Tarefa"],
                    "Status": row["Status"],
                    "Observação": row["Observação"],
                    "Equipamentos Esperados": row["Equipamentos Esperados"],
                    "Equipamentos Respondidos": row["Equipamentos Respondidos"],
                    "Link da Tarefa": row["Link da Tarefa"],
                    "Equipamento": equipamento,
                }
            )
            primeira = False
        else:
            linhas.append(
                {
                    "Data": "",
                    "Prestador": "",
                    "Escola": "",
                    "Tipo da Tarefa": "",
                    "Status": "",
                    "Observação": "",
                    "Equipamentos Esperados": "",
                    "Equipamentos Respondidos": "",
                    "Link da Tarefa": "",
                    "Equipamento": equipamento,
                }
            )

df_final_export = pd.DataFrame(linhas)

csv = df_final_export.to_csv(index=False, sep=";").encode("utf-8")

# 🔧 Dados de exemplo com equipamentos agrupados
dados = [
    [
        "06/05/2025",
        "Pako Ruhan",
        "Escola A",
        "Preventiva Mensal",
        "Finalizada",
        "Observação",
        5,
        5,
        "link",
        "Equipamento 1",
    ],
    ["", "", "", "", "", "", "", "", "", "Equipamento 2"],
    ["", "", "", "", "", "", "", "", "", "Equipamento 3"],
    [
        "09/05/2025",
        "Pako Ruhan",
        "Escola B",
        "Preventiva Semestral",
        "Finalizada",
        "Observação",
        3,
        3,
        "link",
        "Equipamento 1",
    ],
    ["", "", "", "", "", "", "", "", "", "Equipamento 2"],
]

colunas = [
    "Data",
    "Prestador",
    "Escola",
    "Tipo da Tarefa",
    "Status",
    "Observação",
    "Equipamentos Esperados",
    "Equipamentos Respondidos",
    "Link da Tarefa",
    "Equipamento",
]

# 🔥 -> Usa o DataFrame que já está filtrado na tela:
df_base = df_filt.copy()

# Prepara lista de linhas para exportação
linhas = []

for equipamento in equipamentos if equipamentos else ["Sem equipamentos"]:
    nome_equipamento = (
        equipamentos_dict.get(equipamento, f"ID {equipamento}")
        if equipamento
        else "Sem equipamentos"
    )

    if primeira:
        linhas.append(
            {
                "Data": row["data"].strftime("%d/%m/%Y"),
                "Prestador": row["nome"],
                "Escola": row["escola"],
                "Tipo da Tarefa": row["tipo"],
                "Status": row["status"],
                "Observação": row.get("observacao", ""),
                "Link da Tarefa": row.get("taskUrl", ""),
                "Equipamento": nome_equipamento,
                "Nível": 0,
            }
        )
        primeira = False
    else:
        linhas.append(
            {
                "Data": "",
                "Prestador": "",
                "Escola": "",
                "Tipo da Tarefa": "",
                "Status": "",
                "Observação": "",
                "Link da Tarefa": "",
                "Equipamento": nome_equipamento,
                "Nível": 1,
            }
        )

df_final = pd.DataFrame(linhas)

# 🎯 Gerar Excel com expansores
output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    df_final.to_excel(writer, sheet_name="Tarefas", index=False, startrow=1)

    workbook = writer.book
    worksheet = writer.sheets["Tarefas"]

    header_format = workbook.add_format({"bold": True, "bg_color": "#D9D9D9"})
    for col_num, value in enumerate(df_final.columns[:-1]):  # Exclui coluna 'Nível'
        worksheet.write(0, col_num, value, header_format)

    # 🔗 Definir agrupamento de linhas no Excel
    row_num = 1
    while row_num < len(df_final) + 1:
        nivel = df_final.iloc[row_num - 1]["Nível"]

        if nivel == 0:
            start = row_num
            end = row_num
            # Conta quantas linhas filhas tem
            for next_row in range(row_num + 1, len(df_final) + 1):
                if df_final.iloc[next_row - 1]["Nível"] == 1:
                    end = next_row
                else:
                    break

            if end > start:
                worksheet.set_row(start - 1, None, None, {"level": 0})
                for r in range(start, end + 1):
                    worksheet.set_row(r - 1, None, None, {"level": 1})
            else:
                worksheet.set_row(start - 1, None, None, {"level": 0})

            row_num = end + 1
        else:
            row_num += 1

    worksheet.outline_settings(True, False, False, False)

# 🔽 Botão de download no Streamlit
st.download_button(
    label="📥 Baixar Excel com expansores",
    data=output.getvalue(),
    file_name="tarefas_com_expansores.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
