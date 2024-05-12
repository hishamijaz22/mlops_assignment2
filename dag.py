import re
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 15),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Function to extract articles from Dawn.com
def extract_dawn_articles():
    dawn_url = "https://www.dawn.com"
    dawn_response = requests.get(dawn_url)
    dawn_soup = BeautifulSoup(dawn_response.text, 'html.parser')
    dawn_articles = dawn_soup.find_all('article', class_='story')
    return dawn_articles

# Function to extract articles from BBC.com
def extract_bbc_articles():
    bbc_url = "https://www.bbc.com"
    bbc_response = requests.get(bbc_url)
    bbc_soup = BeautifulSoup(bbc_response.text, 'html.parser')
    bbc_articles = bbc_soup.find_all('div', class_='gs-c-promo-body')
    return bbc_articles

# Function for text preprocessing
def preprocess_text(text):
    # Remove special characters and multiple whitespaces
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    # Convert text to lowercase
    text = text.lower()
    return text.strip()

# Function to process articles
def process_articles():
    dawn_articles = extract_dawn_articles()
    bbc_articles = extract_bbc_articles()
    data = []
    for article in dawn_articles:
        title_element = article.find('h2', class_='story__title')
        if title_element:
            title = preprocess_text(title_element.text)
        else:
            title = "Title not found"
        
        desc_element = article.find('div', class_='story__excerpt')
        if desc_element:
            desc = preprocess_text(desc_element.text)
        else:
            desc = "Description not found"
        
        data.append({'Source': 'Dawn.com', 'Title': title, 'Description': desc})

    for article in bbc_articles:
        # Try to find title
        title_element = article.find('h3', class_='gs-c-promo-heading__title')
        if title_element:
            title = preprocess_text(title_element.get_text(strip=True))
        else:
            title = "Title not found"

        # Try to find description
        desc_element = article.find('p', class_='gs-c-promo-summary')
        if desc_element:
            desc = preprocess_text(desc_element.get_text(strip=True))
        else:
            desc = "Description not found"

        data.append({'Source': 'BBC.com', 'Title': title, 'Description': desc})
    
    df = pd.DataFrame(data)
    df.to_csv('/path/to/your/google_drive_folder/articles.csv', index=False)

dag = DAG('data_extraction', default_args=default_args, schedule_interval='@daily')

# Define Python operators to execute the scraping and preprocessing functions
scrape_and_process_task = PythonOperator(
    task_id='scrape_and_process_articles',
    python_callable=process_articles,
    dag=dag
)
