from urllib.request import urlopen
import json
import streamlit as st
from influxdb_client.client import influxdb_client
import config

bucket = config.bucket
org = config.org
token = config.token
url = config.url

queryDiaria = f'from(bucket:"{bucket}")\
    |> range(start: -1d)\
    |> window(every: 1h)\
    |> mean()\
    |> filter(fn:(r) => r._measurement == "Aforo")\
    |> duplicate(column: "_stop", as: "_time")\
    |> window(every: inf)\
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") \
    |> keep(columns: ["cantidad","_time"])\
    |> timeShift(duration: 1h, columns: ["_time"])'

query5minutos = f'from(bucket: "{bucket}")\
    |> range(start: -1d)\
    |> filter(fn: (r) => r._measurement == "Aforo") \
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\
    |> keep(columns: ["cantidad", "_time"]) \
    |> timeShift(duration: 2h, columns: ["_time"])'


def getData(query):
    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    query_api = client.query_api()

    result_df = query_api.query_data_frame(org=org, query=query)

    client.close()
    return result_df


st.set_page_config(page_title="Aforo UJI", page_icon="descarga.png")

st.title("Gente actual en el gimnasio:")

# Gente actual en el gimnasio
response = urlopen("https://uji-data-ocupacion-se.s3.eu-west-1.amazonaws.com/status.json")
data_json = json.loads(response.read())
cant = int(data_json['status']['ocupation'])
if cant < 30:
    color = "green"
elif cant < 45:
    color = "orange"
else:
    color = "red"

st.markdown(f'<h1 style="text-align: center; color:{color};">{cant} / 50</h1>', unsafe_allow_html=True)

# Media por hora
data = getData(queryDiaria)
data['hour'] = data['_time'].dt.hour
data.rename(columns={"_time": "Horas", "cantidad": "Cantidad"}, inplace=True)
data.to_csv("prueba.csv", index=False)

st.title("Media de cada hora durante el dia")
st.bar_chart(data, x="Horas", y="Cantidad")

# Gente cada 5 minutos
data5minutos = getData(query5minutos)
data5minutos = data5minutos.rename(columns={"_time": "Date-Time", "cantidad": "Cantidad"})

st.title("Evolucion cada 5 min")
element = st.line_chart(data5minutos, x="Date-Time", y="Cantidad")


# Sidebar
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')


csv = convert_df(data)
json = data.to_json()

with st.sidebar:
    st.text("Descargar datos en formato CSV:")
    st.download_button(
        "DESCARGAR CSV",
        csv,
        "datos.csv"
    )
    st.text("Descargar datos en formato JSON:")
    st.download_button(
        "DESCARGAR JSON",
        json,
        "datos.json"
    )
