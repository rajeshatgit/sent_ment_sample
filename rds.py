import psycopg2
import logging
from datetime import datetime
import requests
import base64
from newspaper import Article
from bs4 import BeautifulSoup
import json
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API Configuration
API_KEY = "PB-Token gPD3qd122Pv218lXdxLVMQrGU0NnLeVW"
BASE_URL_ENTITIES = "https://api-v2.pitchbook.com/sandbox-entities?entityType=COMPANIES"
BASE_URL_NEWS = "https://api.pitchbook.com/entities/{}/news?trailingRange=30"
HEADERS = {
    "Authorization": API_KEY
}

def connect_to_db():
    """
    Establish a connection to the PostgreSQL database.
    Update credentials with your AWS RDS connection details.
    """
    try:
        conn = psycopg2.connect(
            host="your-rds-endpoint.amazonaws.com",
            port="5432",
            dbname="your_database_name",
            user="your_username",
            password="your_password"
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None

def create_tables(conn):
    """
    Create the necessary tables if they don't already exist.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS CompanyTable (
                Company VARCHAR(255) NOT NULL,
                Ticker VARCHAR(50) NOT NULL,
                PB_ID VARCHAR(50) PRIMARY KEY
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ArticleAnalysisTable (
                Ticker VARCHAR(50) NOT NULL,
                PB_ID VARCHAR(50) NOT NULL REFERENCES CompanyTable(PB_ID),
                Execution_Timestamp TIMESTAMP NOT NULL,
                Url TEXT NOT NULL,
                Raw_Article_Text TEXT,
                Article_Summary TEXT,
                Sentiment_Score NUMERIC,
                Analysis JSONB,
                Company_Name VARCHAR(255),
                Article_Title TEXT,
                Published_TS TIMESTAMP,
                Modified_TS TIMESTAMP,
                Sentiment_Score_Reasoning TEXT,
                Company_Valuation_Significance TEXT,
                Company_Valuation_Significance_Reasoning TEXT,
                Explicit_Company_Impacts TEXT,
                Implicit_Industry_Impacts TEXT,
                Implicit_Impact_Peer_Companies TEXT,
                PRIMARY KEY (PB_ID, Url)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Logs (
                Log_ID SERIAL PRIMARY KEY,
                Error_Type TEXT,
                Error_Details TEXT,
                Related_Item TEXT,
                Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()
        logging.info("Tables created or already exist.")
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to create tables: {e}")

def fetch_company_data():
    """
    Fetch company data from PitchBook API.
    """
    try:
        response = requests.get(BASE_URL_ENTITIES, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("entities", [])
    except Exception as e:
        logging.error(f"Error fetching company data: {e}")
        return []

def fetch_articles_for_company(pb_id):
    """
    Fetch article URLs for a given company PB_ID.
    """
    try:
        url = BASE_URL_NEWS.format(pb_id)
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("articles", [])
    except Exception as e:
        logging.error(f"Error fetching articles for PB_ID {pb_id}: {e}")
        return []

def fetch_and_clean_article(url):
    """
    Fetch and clean article content using newspaper3k and BeautifulSoup.
    """
    try:
        article = Article(url)
        article.download()
        article.parse()
        title = article.title if article.title else "Article"
        article_html = article.html
        article_text = article.text

        soup = BeautifulSoup(article_html, 'html.parser')
        for tag in soup(['script', 'style', 'header', 'footer', 'aside', 'nav']):
            tag.decompose()

        cleaned_text = ''
        for para in soup.find_all('p'):
            para_text = para.get_text().strip()
            if para_text:
                cleaned_text += para_text + '\n\n'

        final_text = cleaned_text.strip() if cleaned_text else article_text
        formatted_text = re.sub(r'\n{3,}', '\n\n', final_text)

        return {
            "title": title,
            "text": formatted_text
        }
    except Exception as e:
        logging.error(f"Error fetching article: {e}")
        return None

def get_token():
    """
    Get an authentication token using Okta.
    """
    try:
        client_id = "your_client_id"
        client_secret = "your_client_secret"
        auth_url = "your_okta_auth_url"
        authorization = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        body = {"grant_type": "client_credentials"}
        response = requests.post(f"{auth_url}/v1/token", data=body, headers=headers)
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logging.error(f"Error fetching token: {e}")
        return None

def analyze_article(token, article_content):
    """
    Analyze article content using Azure OpenAI.
    """
    try:
        endpoint_url = "your_openai_api_endpoint"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        prompt = {
            "Company Name": "[Company Name]",
            "Article Title": "[Article Title]",
            "Article Published Timestamp in PT": "[MM/dd/yyyy hh:mm:ss a]",
            "Article Modified Timestamp in PT": "[MM/dd/yyyy hh:mm:ss a]",
            "Article Summary": "[Article Summary]",
            "Sentiment Score": "[Sentiment Score (based on -10 to 10)]",
            "Sentiment Score Reasoning": "[Sentiment Score Reasoning]",
            "Company Valuation Significance": "[Company Valuation Significance]",
            "Company Valuation Significance Reasoning": "[Company Valuation Significance Reasoning]",
            "Explicit Company Impacts": "[Explicit Company Impacts]",
            "Implicit Industry Impacts": "[Implicit Industry Impacts]",
            "Implicit Impact Peer Companies": "[Implicit Impact Peer Companies]"
        }
        payload = json.dumps({
            "messages": [
                {"role": "system", "content": "Assistant is a large language model trained by OpenAI for investment analysis."},
                {"role": "user", "content": json.dumps(prompt)}
            ],
            "max_tokens": 2000
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        response.raise_for_status()
        json_res = response.json()

        if 'choices' in json_res:
            return json.loads(json_res['choices'][0]['message']['content'])
        else:
            raise ValueError("The 'choices' key is missing from the API response.")
    except Exception as e:
        logging.error(f"Error analyzing article: {e}")
        return None

def insert_into_company_table(conn, company, ticker, pb_id):
    """
    Insert company-level information into CompanyTable.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO CompanyTable (Company, Ticker, PB_ID) VALUES (%s, %s, %s)
            ON CONFLICT (PB_ID) DO NOTHING;
            """,
            (company, ticker, pb_id)
        )
        conn.commit()
        cursor.close()
        logging.info(f"Inserted company {company} into CompanyTable.")
    except Exception as e:
        log_error(conn, "Database Insertion Error", str(e), pb_id)

def insert_into_article_table(conn, data):
    """
    Insert article-level information into ArticleAnalysisTable.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ArticleAnalysisTable (
                Ticker, PB_ID, Execution_Timestamp, Url, Raw_Article_Text,
                Article_Summary, Sentiment_Score, Analysis, Company_Name, Article_Title,
                Published_TS, Modified_TS, Sentiment_Score_Reasoning,
                Company_Valuation_Significance, Company_Valuation_Significance_Reasoning,
                Explicit_Company_Impacts, Implicit_Industry_Impacts, Implicit_Impact_Peer_Companies
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            data
        )
        conn.commit()
        cursor.close()
        logging.info(f"Inserted article data into ArticleAnalysisTable for PB_ID {data[1]}.")
    except Exception as e:
        log_error(conn, "Database Insertion Error", str(e), data[3])

def log_error(conn, error_type, error_details, related_item):
    """
    Log errors into the Logs table.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO Logs (Error_Type, Error_Details, Related_Item) VALUES (%s, %s, %s);
            """,
            (error_type, error_details, related_item)
        )
        conn.commit()
        cursor.close()
        logging.info(f"Logged error: {error_type} for {related_item}.")
    except Exception as e:
        logging.error(f"Failed to log error: {e}")

def main():
    """
    Main function to handle the entire workflow.
    """
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    create_tables(conn)

    companies = fetch_company_data()
    for company in companies:
        company_name = company.get("name")
        ticker = company.get("ticker", "Unknown")
        pb_id = company.get("id")

        insert_into_company_table(conn, company_name, ticker, pb_id)

        articles = fetch_articles_for_company(pb_id)
        for article in articles:
            url = article.get("url")
            article_data = fetch_and_clean_article(url)
            if not article_data:
                log_error(conn, "Scraping Error", "Failed to fetch article text", url)
                continue

            token = get_token()
            if not token:
                log_error(conn, "Authentication Error", "Failed to fetch token", url)
                continue

            analysis = analyze_article(token, article_data["text"])
            if not analysis:
                log_error(conn, "Analysis Error", "Failed to analyze article", url)
                continue

            data = (
                ticker, pb_id, datetime.now(), url, article_data["text"],
                analysis.get("Article Summary"), analysis.get("Sentiment Score"),
                json.dumps(analysis), company_name, article_data["title"],
                datetime.now(), None, analysis.get("Sentiment Score Reasoning"),
                analysis.get("Company Valuation Significance"), analysis.get("Company Valuation Significance Reasoning"),
                analysis.get("Explicit Company Impacts"), analysis.get("Implicit Industry Impacts"), analysis.get("Implicit Impact Peer Companies")
            )

            insert_into_article_table(conn, data)

    conn.close()

if __name__ == "__main__":
    main()
