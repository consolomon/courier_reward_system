import logging
import pendulum

from airflow.decorators import dag, task
from stg.pg_saver import PgSaver
from stg.collection_loader import CollectionLoader
from stg.collection_reader import CollectionReader
from lib import ConnectionBuilder
from lib.api_connect import APIConnection 

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_origin_to_stg_dag():

    # Задаём константы ссылок и заголовков
    HEADERS = {
        "X-Nickname": "consolomon",
        "X-Cohort": "10",
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
    }
    API_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"

    # Задаём имена коллекций из API
    RESTAURANTS_COLLECTION = "restaurants"
    COURIERS_COLLECTION = "couriers"
    DELIVERIES_COLLECTION = "deliveries"

    # Инициализируем подключение к базе dwh.

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Инициализируем подключение через API
    api_connect = APIConnection(API_URL, HEADERS)
    
    # Инициализируем класс, в котором реализована логика сохранения.
    pg_saver = PgSaver()

    @task()
    def load_restaurants():

        # Инициализируем класс, реализующий чтение данных из источника.
        restaurants_reader = CollectionReader(api_connect, RESTAURANTS_COLLECTION)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CollectionLoader(restaurants_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.load_collection()

    restaurants_etl = load_restaurants()

    @task()
    def load_couriers():

        # Инициализируем класс, реализующий чтение данных из источника.
        couriers_reader = CollectionReader(api_connect, COURIERS_COLLECTION)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CollectionLoader(couriers_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.load_collection()

    users_etl = load_couriers()

    @task()
    def load_deliveries():

        # Инициализируем класс, реализующий чтение данных из источника.
        deliveries_reader = CollectionReader(api_connect, DELIVERIES_COLLECTION)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CollectionLoader(deliveries_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.load_collection()

    orders_etl = load_deliveries() 

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurants_etl >> users_etl >> orders_etl  # type: ignore


origin_to_stg_dag = sprint5_origin_to_stg_dag()  # noqa
