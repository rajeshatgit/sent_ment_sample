import psycopg2
import logging
from datetime import datetime
import requests
import base64
from newspaper import Article
from bs4 import BeautifulSoup
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def fetch_and_clean_article(url):
    """
    Fetch and clean article content using newspaper3k and BeautifulSoup.
    """
    try:
        article = Article(url)
        article.download()
        article.parse()
        soup = BeautifulSoup(article.html, 'html.parser')
        for tag in soup(['script', 'style', 'header', 'footer', 'aside', 'nav']):
            tag.decompose()
        cleaned_text = '\n\n'.join([para.get_text().strip() for para in soup.find_all('p') if para.get_text().strip()])
        return cleaned_text if cleaned_text else article.text
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

def analyze_article(token, text):
    """
    Analyze article content using Azure OpenAI.
    """
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {"input": text}
        response = requests.post("your_openai_api_endpoint", headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error analyzing article: {e}")
        return None

def main():
    """
    Main function to handle scraping, analysis, and database saving with sample URLs.
    """
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    create_tables(conn)

    # Sample data for testing
    companies = [
        {"company": "Company A", "ticker": "CA", "pb_id": "PB001", "articles": [
            {"url": "https://example.com/article1"},
            {"url": "https://example.com/article2"}
        ]},
        {"company": "Company B", "ticker": "CB", "pb_id": "PB002", "articles": [
            {"url": "https://example.com/article3"}
        ]}
    ]

    for company in companies:
        insert_into_company_table(conn, company["company"], company["ticker"], company["pb_id"])

        for article in company["articles"]:
            raw_text = fetch_and_clean_article(article["url"])
            if not raw_text:
                log_error(conn, "Scraping Error", "Failed to fetch article text", article["url"])
                continue

            token = get_token()
            if not token:
                log_error(conn, "Authentication Error", "Failed to fetch token", article["url"])
                continue

            analysis = analyze_article(token, raw_text)
            if not analysis:
                log_error(conn, "Analysis Error", "Failed to analyze article", article["url"])
                continue

            data = (
                company["ticker"], company["pb_id"], datetime.now(), article["url"],
                raw_text, analysis.get("summary"), analysis.get("sentiment_score"),
                json.dumps(analysis), company["company"], analysis.get("title"),
                datetime.now(), None, analysis.get("reasoning"),
                analysis.get("valuation_significance"), analysis.get("reasoning_details"),
                analysis.get("explicit_impacts"), analysis.get("implicit_impacts"), analysis.get("peer_impacts")
            )

            insert_into_article_table(conn, data)

    conn.close()

if __name__ == "__main__":
    main()
