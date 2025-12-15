import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- CONFIGURAÇÃO DE ACESSO AO MINIO (DOCKER) ---
# Como o Streamlit roda no Windows, acessamos via localhost:9000
os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'password'
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

st.set_page_config(page_title="Data Shield | Analytics", layout="wide", page_icon="🛡️")

# --- CSS PARA ESTILIZAR ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÃO DE CARGA DE DADOS ---
# O cache evita ler do disco toda hora, recarrega a cada 30s
@st.cache_data(ttl=30)
def load_data_from_lakehouse(bucket, folder):
    try:
        path = f"s3://{bucket}/{folder}/"
        storage_options = {
            "client_kwargs": {"endpoint_url": "http://localhost:9000"},
            "key": "admin",
            "secret": "password"
        }
        # Lê usando pyarrow (engine do pandas para parquet)
        df = pd.read_parquet(path, storage_options=storage_options)
        return df
    except Exception as e:
        # Se der erro (ex: tabela não existe ainda), retorna DataFrame vazio
        return pd.DataFrame()

# --- CABEÇALHO ---
st.title("🛡️ Data Shield Enterprise")
st.markdown("### 📊 Painel de Controle: Vendas & Segurança")
st.markdown("---")

# --- CARREGANDO DADOS ---
with st.spinner('Sincronizando com o Data Lake...'):
    df_vendas = load_data_from_lakehouse("gold", "vendas_por_loja")
    df_fraude = load_data_from_lakehouse("gold", "ml_fraud_detection")

# --- LAYOUT DO DASHBOARD ---

# Seção 1: KPIs Gerais
if not df_vendas.empty:
    col1, col2, col3 = st.columns(3)
    
    total_faturamento = df_vendas["total_vendas"].sum()
    total_transacoes = df_vendas["qtd_transacoes"].sum()
    top_loja = df_vendas.sort_values("total_vendas", ascending=False).iloc[0]["store_name"]

    col1.metric("💰 Faturamento Total", f"R$ {total_faturamento:,.2f}")
    col2.metric("📦 Total de Transações", f"{total_transacoes}")
    col3.metric("🏆 Top Loja", top_loja)

st.markdown("---")

col_left, col_right = st.columns([2, 1])

# Seção 2: Gráfico de Vendas
with col_left:
    if not df_vendas.empty:
        st.subheader("Desempenho por Loja")
        fig_bar = px.bar(
            df_vendas.sort_values("total_vendas", ascending=True).tail(10), # Top 10
            x="total_vendas",
            y="store_name",
            orientation='h',
            text_auto='.2s',
            title="Top 10 Lojas por Faturamento",
            color="total_vendas",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.warning("⚠️ Tabela Gold de Vendas não encontrada ou vazia.")

# Seção 3: Monitor de Fraudes (ML)
with col_right:
    st.subheader("🤖 Monitor de Fraudes (IA)")
    
    if not df_fraude.empty:
        # Gráfico de Pizza
        contagem = df_fraude["is_anomaly"].value_counts().reset_index()
        contagem.columns = ["Tipo", "Qtd"]
        
        fig_pie = px.pie(
            contagem, 
            values="Qtd", 
            names="Tipo", 
            color="Tipo",
            color_discrete_map={"NORMAL": "#2ecc71", "SUSPEITA": "#e74c3c"},
            hole=0.4
        )
        st.plotly_chart(fig_pie, use_container_width=True)
        
        # Alerta de últimas fraudes
        suspeitas = df_fraude[df_fraude["is_anomaly"] == "SUSPEITA"]
        qtd_suspeitas = len(suspeitas)
        
        if qtd_suspeitas > 0:
            st.error(f"⚠️ {qtd_suspeitas} transações suspeitas detectadas!")
            st.dataframe(
                suspeitas[["client_name", "amount", "store_name"]].sort_values("amount", ascending=False).head(5),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("Nenhuma atividade suspeita recente.")
            
    else:
        st.info("Aguardando processamento do modelo de ML...")

# Rodapé
st.markdown("---")
st.caption("🚀 Pipeline: Kafka → Spark Streaming (Bronze) → Spark ETL (Silver) → Spark Aggregation (Gold) → Streamlit")