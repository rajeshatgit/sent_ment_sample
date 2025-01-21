import requests
import psycopg2
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright
import json
import base64
from bs4 import BeautifulSoup

# Database connection details
DB_HOST = "<DB_HOST>"
DB_PORT = "5432"
DB_NAME = "<DB_NAME>"
DB_USER = "<DB_USER>"
DB_PASSWORD = "<DB_PASSWORD>"

# API Sandbox Key:
API_KEY = "PB-Token gPD3qd122Pv218lXdxLVMQrGU0NnLeVW"

# Base URLs
BASE_URL_ENTITIES = "https://api-v2.pitchbook.com/sandbox-entities?entityType=COMPANIES"
BASE_URL_NEWS = "https://api.pitchbook.com/entities/{}/news?trailingRange=30"

# Headers for API requests
HEADERS = {
    "Authorization": API_KEY,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None

def create_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS CompanyTable (
                Company VARCHAR(255) NOT NULL,
                Ticker VARCHAR(50) NOT NULL,
                PB_ID VARCHAR(50) PRIMARY KEY
            );

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
    try:
        response = requests.get(BASE_URL_ENTITIES, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("entities", [])
    except Exception as e:
        logging.error(f"Error fetching company data: {e}")
        return []

def fetch_articles_for_company(pb_id):
    try:
        url = BASE_URL_NEWS.format(pb_id)
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("articles", [])
    except Exception as e:
        logging.error(f"Error fetching articles for PB_ID {pb_id}: {e}")
        return []

def scrape_with_playwright(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)  # Wait for up to 60 seconds
            content = page.content()
            browser.close()

            # Use BeautifulSoup to parse and extract meaningful content
            soup = BeautifulSoup(content, 'html.parser')
            article_text = "\n\n".join([p.get_text() for p in soup.find_all('p')])
            return {
                "title": soup.title.string if soup.title else "Article",
                "text": article_text
            }
    except Exception as e:
        logging.error(f"Playwright scraping failed for URL {url}: {e}")
        return None

def get_token():
    try:
        client_id = "<client_id>"
        client_secret = "<client_secret>"
        auth_url = "<auth_url>"
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
    try:
        endpoint_url = "<azure_openai_endpoint>"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        content = f"""
        I own an investment in the company in the news article.
        Based on the news article provided below, please provide me an analysis in a dictionary format (exactly as given) with the following fields filled out: 
        {{
            "Company Name": "[Company Name]",
            "Article Title": "[Article Title]",
            "Article Published Timestamp in PT": "[MM/dd/yyyy hh:mm:ss a] (convert timestamp to PT if it's posted as a different time zone)",
            "Article Modified Timestamp in PT": "[MM/dd/yyyy hh:mm:ss a] (only include if there is an article updated or modified timestamp, convert timestamp to Pacific Time if it's posted as a different time zone)",
            "Article News Source": "[Article News Source]",
            "Article Summary": "[Article Summary]",
            "Sentiment Score": "[Sentiment Score (based on -10 to 10)]",
            "Sentiment Score Reasoning": "[Sentiment Score Reasoning]",
            "Company Valuation Significance": "[Company Valuation Significance]",
            "Company Valuation Significance Reasoning": "[Company Valuation Significance Reasoning]",
            "Explicit Company Impacts": "[Explicit Company Impacts] (that summarize the direct impact to the valuation of the company the news article is primarily about)",
            "Implicit Industry Impacts": "[Implicit Industry Impacts (if any) that explain potential impacts beyond what’s in the article itself such as impacts to other companies or the industry]",
            "Implicit Impact Peer Companies": "[Implicit Impact Peer Companies (these are companies in the same industry or companies that may be affected by the news. List them in this field separated by a comma)]"
        }}
        Based on this news article content: {{article_content}}
        """
        payload = json.dumps({
            "messages": [
                {"role": "system", "content": "Assistant is a large language model trained by OpenAI for investment analysis."},
                {"role": "user", "content": content}
            ],
            "max_tokens": 3500
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

def log_error(conn, error_type, error_details, related_item):
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
        logging.info(f"Logged error: {error_type} for {related_item}. Details: {error_details}")
    except Exception as e:
        logging.error(f"Failed to log error: {e}")

def insert_into_company_table(conn, company_name, ticker, pb_id):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO CompanyTable (Company, Ticker, PB_ID) VALUES (%s, %s, %s)
            ON CONFLICT (PB_ID) DO NOTHING;
            """,
            (company_name, ticker, pb_id)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to insert into CompanyTable: {e}")

def insert_into_article_table(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ArticleAnalysisTable (
                Ticker, PB_ID, Execution_Timestamp, Url, Raw_Article_Text, 
                Article_Summary, Sentiment_Score, Analysis, Company_Name, Article_Title, 
                Published_TS, Modified_TS, Sentiment_Score_Reasoning, Company_Valuation_Significance, 
                Company_Valuation_Significance_Reasoning, Explicit_Company_Impacts, Implicit_Industry_Impacts, Implicit_Impact_Peer_Companies
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            data
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to insert into ArticleAnalysisTable: {e}")

def main():
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
            article_data = scrape_with_playwright(url)
            if not article_data:
                log_error(conn, "Scraping Error", f"Failed to scrape article from URL {url}", url)
                continue

            token = get_token()
            if not token:
                log_error(conn, "Authentication Error", "Failed to fetch token", url)
                continue

            analysis = analyze_article(token, article_data["text"])
            if not analysis:
                log_error(conn, "Analysis Error", "Failed to analyze article content", url)
                continue

            data = (
                ticker, pb_id, datetime.now(), url, article_data["text"],
                analysis.get("Article Summary"), analysis.get("Sentiment Score"),
                json.dumps(analysis), company_name, article_data["title"],
                datetime.now(), None, analysis.get("Sentiment Score Reasoning"),
                analysis.get("Company Valuation Significance"), analysis.get("Company Valuation Significance Reasoning"),
                analysis.get("Explicit Company Impacts"), analysis.get("Implicit Industry Impacts"), analysis.get("Implicit Impact Peer Companies")
            )

            try:
                insert_into_article_table(conn, data)
            except Exception as e:
                log_error(conn, "Database Insertion Error", f"Database insertion failed for URL {url}. Error: {e}", url)

    conn.close()

if __name__ == "__main__":
    main()
