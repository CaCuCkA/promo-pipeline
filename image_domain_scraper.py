from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from bs4 import BeautifulSoup
from google.cloud import vision
from google.oauth2 import service_account

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

OPENAI_KEY = "openai-gcp-key"
TARGET_URL = "https://www.whosmailingwhat.com/blog/best-direct-mail-marketing-examples/"
GOOGLE_API_KEY = {
    "api_key": "<KEY>"
}


def scrape_and_process_image_urls(**kwargs):
    response = requests.get(TARGET_URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    image_urls = [img['src'] for p in soup.find_all('p') for img in p.find_all('img') if 'src' in img.attrs]

    processed_data_list = []

    for image_url in image_urls:
        processed_data = process_image_url(image_url)
        processed_data_list.append(processed_data)

    return processed_data_list


def get_openai_response(query):
    openai_url = "https://api.openai.com/v1/chat/completions"
    openai_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_KEY}"
    }

    openai_data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "user", "content": query},
        ],
    }

    openai_response = requests.post(openai_url, headers=openai_headers, json=openai_data)
    return openai_response.json()


def process_image_url(image_url):
    credentials = service_account.Credentials.from_service_account_info(GOOGLE_API_KEY)
    client = vision.ImageAnnotatorClient(credentials=credentials)

    image = vision.Image()
    image.source.image_uri = image_url
    response = client.text_detection(image=image)

    try:
        text_description = response.text_annotations[0].description
    except IndexError:
        text_description = None

    domain_query = (
        "Extract from this text only domain names in JSON list format. If there is no domain return "
        f"empty list {text_description}"
    )
    description_query = f"Thank you! Can you get a small description from this? It can be like a summary: {text_description}"

    openai_data = get_openai_response(domain_query)
    choices = openai_data.get('choices', [])
    domain_name = choices[0]['message']['content'] if choices and len(choices) > 0 else None

    openai_data = get_openai_response(description_query)
    choices = openai_data.get('choices', [])
    description = choices[0]['message']['content'] if len(choices) > 0 else None

    print("Domain: " + str(domain_name))
    print(str(description))

    return {
        'image_url': image_url,
        'domain_name': domain_name,
        'additional_info': description,
    }


def insert_data_into_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='scrape_and_process_image_urls_task')

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    insert_sql = """
        INSERT INTO market_urls (image_url, domain, description, created_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (image_url) DO UPDATE
        SET domain = COALESCE(EXCLUDED.domain, market_urls.domain),
            description = COALESCE(EXCLUDED.description, market_urls.description),
            created_at = COALESCE(EXCLUDED.created_at, market_urls.created_at)
    """

    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for record in data:
        cursor.execute(insert_sql, (record['image_url'], record['domain_name'], record['additional_info']))

    conn.commit()
    cursor.close()
    conn.close()


dag_id = 'scraping_image_dag'
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=datetime.today(),
)

scrape_and_process_image_urls_task = PythonOperator(
    task_id='scrape_and_process_image_urls_task',
    python_callable=scrape_and_process_image_urls,
    provide_context=True,
    dag=dag,
)

insert_data_into_db_task = PythonOperator(
    task_id='insert_data_into_db_task',
    python_callable=insert_data_into_db,
    provide_context=True,
    dag=dag,
)

create_postgres_table_task = PostgresOperator(
    task_id='create_postgres_table',
    postgres_conn_id='postgres_localhost',
    sql="""
        CREATE TABLE IF NOT EXISTS market_urls (
            image_url VARCHAR(255) PRIMARY KEY,
            domain VARCHAR(255) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    dag=dag,
)

create_postgres_table_task >> scrape_and_process_image_urls_task >> insert_data_into_db_task
