from influxdb_client.client.write_api import SYNCHRONOUS
from urllib.request import urlopen
from datetime import datetime
import influxdb_client
import streamlit as st

import json
import time

bucket = st.secrets["BUCKET"]
org = st.secrets["ORG"]
token = st.secrets["TOKEN"]
url = st.secrets["URL"]
json_url = st.secrets["JSON_URL"]


class Programa:

    def __init__(self):
        self.url = json_url # JSON con los datos del aforo
        self.bucket = bucket  # Bucket donde se almacenan los datos
        self.org = org
        self.token = token
        self.urlDB = url
        self.cliente = influxdb_client.InfluxDBClient(
            url=self.urlDB,
            org=self.org,
            token=self.token
        )
        self.write_api = self.cliente.write_api(write_options=SYNCHRONOUS)

    def introducirDatos(self):
        currentDateAndTime = datetime.now()
        currentHour = currentDateAndTime.strftime("%H")
        currentMonth = currentDateAndTime.month
        currentDay = currentDateAndTime.day

        if (int(currentMonth) == 6 or int(currentMonth) == 7) and int(currentDay) <= 21:
            endHour = 21
        else:
            endHour = 13

        while int(currentHour) != endHour:
            response = urlopen(self.url)
            try:
                data_json = json.loads(response.read())
                cant = int(data_json['status']['ocupation'])

                self._ejecutarQuery(cant)
                time.sleep(300)
                currentDateAndTime = datetime.now()
                currentHour = currentDateAndTime.strftime("%H")
            except:
                pass

    def _ejecutarQuery(self, cant):
        p = influxdb_client.Point("Aforo").field("cantidad", cant)  # Query para introducir datos
        self.write_api.write(bucket=self.bucket, org=self.org, record=p)  # Escribo en la BBDD


if __name__ == "__main__":
    programa = Programa()
    programa.introducirDatos()

# mqtt
