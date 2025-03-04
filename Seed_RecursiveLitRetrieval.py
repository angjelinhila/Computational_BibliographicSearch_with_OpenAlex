#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install requests


# In[2]:


pip install pandas


# In[3]:


pip install psycopg2


# In[24]:


import requests
import psycopg2
import time
from psycopg2 import sql

# OpenAlex API endpoint
OPENALEX_WORKS_URL = "https://api.openalex.org/works"

# PostgreSQL connection details
DB_NAME = "#"
DB_USER = "#"
DB_PASSWORD = "#"
DB_HOST = "#"
DB_PORT = "#"

# Max depth for recursive reference retrieval
MAX_DEPTH = 2
reference_count = 0

def fetch_data(url, params=None):
    print(f"Fetching data from: {url}")  # Debug print
    response = requests.get(url, params=params)
    if response.status_code == 200:
        try:
            # Print rate limit information
            print(f"Rate limit: {response.headers.get('X-RateLimit-Limit')}")
            print(f"Remaining requests: {response.headers.get('X-RateLimit-Remaining')}")
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"Failed to decode JSON. Raw response: {response.text}")  # Debug print
            return None
    else:
        print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        print(f"Response content: {response.text}")  # Debug print
        return None

def insert_data(conn, table, data):
    print(f"Inserting data into {table}: {data}")  # Debug print
    with conn.cursor() as cursor:
        columns = data.keys()
        values = [data[col] for col in columns]
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        )
        cursor.execute(query, values)
    conn.commit()

def fetch_references(conn, paper_id, depth=0):
    global reference_count
    if depth >= MAX_DEPTH:
        return
    
    print(f"Fetching references for paper ID: {paper_id} (depth: {depth})")  # Debug print
    url = f"{OPENALEX_WORKS_URL}/{paper_id}"
    data = fetch_data(url)
    if not data:
        return
    
    references = data.get("referenced_works", [])
    print(f"References found: {references}")  # Debug print
    reference_count += len(references)

    for ref in references:
        # Construct the correct API URL for the reference
        ref_url = f"{OPENALEX_WORKS_URL}/{ref.split('/')[-1]}"  # Extract the ID from the full URL
        ref_data = fetch_data(ref_url)
        if ref_data and ref_data.get("title"):  # Ensure the reference has a title
            doi = ref_data.get("doi")
            if not doi:
                print(f"Skipping reference with missing DOI: {ref_data.get('title')}")
                continue  # Skip this reference

            paper_data = {
                "title": ref_data.get("title", "No title available"),
                "citations": ref_data.get("cited_by_count", 0),
                "year": ref_data.get("publication_year", "N/A"),
                "doi": doi,
                "abstract": ref_data.get("abstract", "N/A")
            }
            insert_data(conn, "papers", paper_data)
            
            # Insert reference link
            if not data.get("doi") or not ref_data.get("doi"):
                print(f"Skipping reference link due to missing DOI: {data.get('doi')} -> {ref_data.get('doi')}")
                continue

            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO paper_references (paper_id, reference_id)
                    VALUES (
                        (SELECT id FROM papers WHERE doi = %s),
                        (SELECT id FROM papers WHERE doi = %s)
                    ) ON CONFLICT DO NOTHING
                    """,
                    (data.get("doi"), ref_data.get("doi"))
                )
            conn.commit()
            
            # Add a delay to avoid hitting rate limits
            time.sleep(1)  # 1-second delay between requests
            
            fetch_references(conn, ref_data["id"], depth + 1)

def main():
    seed_DOI = "10.1109/TCSS.2022.3160677"  # Replace with your desired DOI
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    
    # Fetch and insert seed paper
    seed_paper = fetch_data(f"{OPENALEX_WORKS_URL}/doi:{seed_DOI}")
    if seed_paper:
        paper_data = {
            "title": seed_paper.get("title", "No title available"),
            "citations": seed_paper.get("cited_by_count", 0),
            "year": seed_paper.get("publication_year", "N/A"),
            "doi": seed_paper.get("doi", "N/A"),
            "abstract": seed_paper.get("abstract", "N/A")
        }
        insert_data(conn, "papers", paper_data)
        
        # Fetch references recursively
        fetch_references(conn, seed_paper["id"], depth=0)

        print(f"Total number of references fetched: {reference_count}")
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    main()

