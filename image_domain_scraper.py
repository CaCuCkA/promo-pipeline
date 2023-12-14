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

OPENAI_KEY = "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"
TARGET_URL = "https://www.whosmailingwhat.com/blog/best-direct-mail-marketing-examples/"
GOOGLE_API_KEY = {
    "type": "service_account",
    "project_id": "apache-airflow-407819",
    "private_key_id": "ebe4ee45a2cf8bc3af2c2d5606ce63bdeec45dc0",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDBr4gW2tVfGeR0\nl/wb8FrPmWRatc6ikBaQJaTKls9TV5/aOM2CZNHWlrmuwmILkC50UAYibvV1fbAS\nW/IErx9QiogfUH7bmANegIRjzcUF92uy09lSCWtGycWpQa4ry8lQU//c4Nv8DEi7\n1kQmlDb01cp+XimhIRTA7NBZbDW+Ii0E4ub5Fj0h9pAv1yTRQqhQgPelm2FUlld6\nAHc00NF8W0UqtMhWpoe+HYdTx4hmE/uATWt2h1k0tboMcy7H+rx/wVkv7ieOhXqV\nCaIf8AHp1Mvn0FWNl5dXdYDzY93IoJkzVwIxm1JpXg7zsZ1Gp1sGCpO3ad0DcBxy\nPsAuNgaFAgMBAAECggEACUbiCs4yeBqiYZ9B+QibIORk9R9OJWUnX4IeEYIFriU9\nR9N+rGct0cxoWmF/FrSyNOn8UTvlT9pUS5zRy72jj1UDThSp7tSqHKWlMa6SaEXA\nh9Ez0IUY+UwCxqeZswlQvCM51xeCEeF4vl515y8kLrfqugDCl7Ag8VimZhITxOaC\numIvSSxEqXtuZ28ciIdRotbG8GWpgiLUcfckzTkguHX5BSDdEDAMtsi6+0aXhxUb\n8GN6m4xgaRjkia22dtwqaSzBMYfjmNOxUqtf0Pzotleu3rTiL6GROxqbYS4mk9hL\ns/R8rFBBCSXzxekzuBANcTjDA7wypEB6eg58ikbz5QKBgQD0vWUpwKBMLnXQC24O\nnQI88ka7O0xjP04GElJQe8dq8FQ/ssdPo5w7psk0dlPH0tHd7le2Nv26hIpu1kvZ\nhdKKMzI5JR1G6OoqrbLuLA+8q5/eOUFSRY8h/x4Hq/gF8ALwYy3RCRmCKGKSpe57\n/9CbsaiGm84zIrBZqxnRSOvIqwKBgQDKmM7puF/vVyezwcTWnu/pW2vB/NAuyadd\nAKD1WjHGU185aRgb4tJwuQQX0alZhEgmF8HCDM/XfjuibawCwo3ffks93jLlUZTw\nSsdKT1I6DKu8eP9SgrDhRMgkj6Kq6O/nQ3azAm1Qloi+nmv6b2PglOBHTB2hhc5M\nuYcTiHDNjwKBgQCmOIgfzmtQsnFjxo+OL0cY27f8bC7abWFxsDnl3du4HtgplyPV\nTBrNTN702bTXT7EoGTvLTxgO+PwIgVVsvH4dTN48f5+dzI1WRTj0mEpr6uiehqZ9\n2S54eYwMy9idN8DXQZUZlyOTChjA3x/Vag5l4EjEe8eVZWb+z72uLeeuFwKBgD5G\nZGvhWdPnNXQC3u+d4V8Y0/HINXH8WG61D8T7WYt9+ypZjKidu1Qc+w4bS3QRvl+/\nM7bjW0wBKHGQRqx+gz+swZId6AUnY6HNSp8j7MGdPXjstIb3V0mXa/IZGMZyXbP3\nv9fcqh94dkYpykUr0kapXJtt4TnSOIzLBqNRcGMvAoGBAIE2XzS81fquRI4Wo9Ll\nfqaU5Rb1H1dHKYWRGu8PACfD32MMZRSwfeDK8SRHkNTO6S4O3V5DW14L2jlAYywY\nchCKh67q1bobZ3eJdc2zFdQ0xQKaT27PoVQMyKcR4jXFFbEZr2NtU75taHw/6vFL\nN07OGUvyOa5gZj5MZVacERWW\n-----END PRIVATE KEY-----\n",
    "client_email": "apache-airflow@apache-airflow-407819.iam.gserviceaccount.com",
    "client_id": "114236664258855094875",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/apache-airflow%40apache-airflow-407819.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
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
