import logging

import pendulum
from airflow.decorators import dag, task
from dds.couriers_loader import CourierLoader
from dds.restaurants_loader import RestaurantLoader
from dds.orders_loader import OrderLoader
from dds.deliveries_loader import DeliveriesLoader
from dds.reports_loader import ReportsLoader
from lib import ConnectionBuilder


log = logging.getLogger(__name__)

@dag(
    schedule_interval='15,25,35,45,55 * * * *',  # Задаем расписание выполнения дага - каждую 15-ю, 25-ю,..., 55-ю минуту каждого часа.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_to_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        loader = RestaurantLoader(dwh_pg_connect, log)
        loader.load_restaurants() # Вызываем функцию, которая перельет данные.

    
    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        loader = CourierLoader(dwh_pg_connect, log)
        loader.load_couriers() # Вызываем функцию, которая перельет данные.

    
    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        loader = OrderLoader(dwh_pg_connect, log)
        loader.load_orders() # Вызываем функцию, которая перельет данные.


    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        loader = DeliveriesLoader(dwh_pg_connect, log)
        loader.load_deliveries() # Вызываем функцию, которая перельет данные.


    @task(task_id="reports_load")
    def load_reports():
        # создаем экземпляр класса, в котором реализована логика.
        loader = ReportsLoader(dwh_pg_connect, log)
        loader.load_reports() # Вызываем функцию, которая перельет данные.                                   


    # Инициализируем объявленные таски.

    restaurants_etl = load_restaurants()

    couriers_etl = load_couriers()

    orders_etl = load_orders()

    deliveries_etl = load_deliveries()

    reports_etl = load_reports()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    restaurants_etl >> [couriers_etl, orders_etl, deliveries_etl] >> reports_etl  # type: ignore


stg_to_dds_dag = sprint5_stg_to_dds_dag()
