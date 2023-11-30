import airflow.models.variable as af_variable
import airflow.utils.dates as af_dates
import airflow.decorators as af_decorators
from airflow import DAG
from airflow.operators.python import PythonOperator as PyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator as HttpOp
from airflow.providers.postgres.operators.postgres import PostgresOperator as PostgresOp
from datetime import datetime
import json
import logging


def prepare_call_for_api(city, execution_dt):
    exec_timestamp = int(datetime.fromisoformat(execution_dt).timestamp())
    logging.info(f"City: {city}, Execution Date: {execution_dt}, Timestamp: {exec_timestamp}")

    latitude, longitude = coordinates_of_city[city]
    return latitude, longitude, exec_timestamp


def group_for_city(city_name):
    @af_decorators.task_group(group_id=f"group_{city_name}")
    def city_related_group():
        api_call_task = PyOperator(
            task_id=f"api_call_{city_name}",
            python_callable=prepare_call_for_api,
            op_kwargs={"city": city_name, "execution_dt": "{{ ds }}"},
        )

        data_extraction_task = HttpOp(
            task_id=f"data_extraction_{city_name}",
            http_conn_id="weather_conn_id",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "lat": "{{ ti.xcom_pull(task_ids='group_" + city_name + ".api_call_" + city_name +  "')[0] }}",
                "lon": "{{ ti.xcom_pull(task_ids='group_" + city_name + ".api_call_" + city_name + "')[1] }}",
                "dt": "{{ ti.xcom_pull(task_ids='group_" + city_name + ".api_call_" + city_name + "')[2] }}",
                "appid": af_variable.Variable.get("WEATHER_API_KEY"),
            },
            method="GET",
            response_filter=lambda response: json.loads(response.text),
            log_response=True,
        )

        api_call_task >> data_extraction_task

    return city_related_group


def process_weather_data(task_instance, city_name):
    weather_data = task_instance.xcom_pull(f"group_{city_name}.data_extraction_{city_name}")
    weather_timestamp = datetime.utcfromtimestamp(int(weather_data["data"][0]["dt"])).strftime('%Y-%m-%d')
    weather_temp = weather_data["data"][0]["temp"]
    weather_humidity = weather_data["data"][0]["humidity"]
    weather_clouds = weather_data["data"][0]["clouds"]
    weather_wind_speed = weather_data["data"][0]["wind_speed"]

    return weather_timestamp, weather_temp, weather_humidity, weather_clouds, weather_wind_speed



def create_transform_and_store_group(city):
    @af_decorators.task_group(group_id=f"process_and_store_{city}")
    def transform_and_store_group():
        transform_weather_data = PyOperator(
            task_id=f"transform_{city}",
            python_callable=process_weather_data,
            op_kwargs={"city_name": city},
        )

        store_weather_data = PostgresOp(
            task_id=f"store_{city}",
            postgres_conn_id="db_conn_hw1",
            sql="""
                INSERT INTO weather_metrics (city, date, temperature, humidity, cloudiness, wind_speed) 
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
            parameters=[
                city,
                "{{ ti.xcom_pull(task_ids='process_and_store_" + city + ".transform_" + city + "')[0] }}",
                "{{ ti.xcom_pull(task_ids='process_and_store_" + city + ".transform_" + city + "')[1] }}",
                "{{ ti.xcom_pull(task_ids='process_and_store_" + city + ".transform_" + city + "')[2] }}",
                "{{ ti.xcom_pull(task_ids='process_and_store_" + city + ".transform_" + city + "')[3] }}",
                "{{ ti.xcom_pull(task_ids='process_and_store_" + city + ".transform_" + city + "')[4] }}",
            ],
        )

        transform_weather_data >> store_weather_data

    return transform_and_store_group

with DAG("modified_weather_scrapper", start_date=af_dates.days_ago(3), schedule_interval="@daily", catchup=False) as dag:
    coordinates_of_city = {
        "Lviv": ("49.8397", "24.0297"),
        "Kyiv": ("50.4501", "30.5234"),
        "Kharkiv": ("49.9935", "36.2304"),
        "Odesa": ("46.4825", "30.7233"),
        "Zhmerynka": ("49.0384", "28.1056"),
    }

    create_weather_table = PostgresOp(
        task_id="create_weather_table",
        postgres_conn_id="db_conn_hw1",
        sql="""
            CREATE TABLE IF NOT EXISTS weather_metrics (
                city VARCHAR(255),
                date TIMESTAMP,
                temperature FLOAT,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT
            );
            """,
    )

    city_groups = [group_for_city(city)() for city in coordinates_of_city]
    for group in city_groups:
        create_weather_table >> group

    transform_and_store_groups = [
        create_transform_and_store_group(city)() for city in coordinates_of_city
    ]
    for city_group, transform_store_group in zip(city_groups, transform_and_store_groups):
        city_group >> transform_store_group