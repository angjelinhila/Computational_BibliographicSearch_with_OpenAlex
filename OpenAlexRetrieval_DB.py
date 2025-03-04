#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install requests


# In[2]:


pip install pandas


# In[3]:


pip install psycopg2


# In[31]:


import requests
import psycopg2
import json
from psycopg2 import sql

# OpenAlex API endpoints
OPENALEX_WORKS_URL = "https://api.openalex.org/works"
OPENALEX_AUTHORS_URL = "https://api.openalex.org/authors"
OPENALEX_SOURCES_URL = "https://api.openalex.org/sources"
OPENALEX_INSTITUTIONS_URL = "https://api.openalex.org/institutions"

# PostgreSQL connection details
DB_NAME = "#"
DB_USER = "#"
DB_PASSWORD = "#"
DB_HOST = "#"
DB_PORT = "#"

# Define your search parameters
params = {
    "search": 'Reddit data analysis',  # Exact phrase matching
    "filter": "is_paratext:false,cited_by_count:>10,publication_year:2011-2025",  # Exclude non-peer-reviewed works, papers with >10 citations, and filter by date range
    "per_page": 50,  # Number of results per page (max allowed by API)
    "sort": "cited_by_count:desc",  # Sort by citation count in descending order
}

# Initialize variables
papers = []
page = 1
max_papers = 1000  # Maximum number of papers to retrieve

# Function to fetch data from OpenAlex API
def fetch_data(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        try:
            data = response.json()
            print(f"DEBUG: Fetched data: {data}")
            return data
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON response from {url}: {e}")
            print(f"Response text: {response.text}") # Print the raw response
            return None # Return None to indicate failure
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch data from {url}: {e}")
        if response is not None:
          print(f"Response status code: {response.status_code}") # Print status code
          print(f"Response text: {response.text}") # Print raw response
        return None  # Return None to indicate failure


# Function to insert data into PostgreSQL
def insert_data(conn, table, data):
    with conn.cursor() as cursor:
        if not isinstance(data, dict):
            print(f"ERROR: Expected dict, got {type(data)}. Content: {data}")
            return

        columns = list(data.keys())
        values = []
        placeholders = []  # Separate list for placeholders

        for key in columns:
            value = data[key]
            if isinstance(value, list):
                json_value = json.dumps(value)  # Convert list to JSON string
                values.append(json_value)
                placeholders.append(sql.Placeholder())  # Use placeholder for JSON
            else:
                values.append(value)
                placeholders.append(sql.Placeholder())  # Use placeholder for other values

        print(f"DEBUG: Columns: {columns}")
        print(f"DEBUG: Values: {values}")

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(placeholders)  # Use the placeholders list
        )

        print(f"DEBUG: Query: {query.as_string(conn)}")
        try:
            cursor.execute(query, values)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting data: {e}")


# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Loop through pages until we have enough papers or run out of results
while len(papers) < max_papers:
    # Set the current page
    params["page"] = page
    
    # Fetch papers
    data = fetch_data(OPENALEX_WORKS_URL, params)
    if not data:
        break
    
    results = data.get("results", [])
    papers.extend(results)
    
    # Stop if there are no more results
    if not results:
        print("No more results found.")
        break
    
    print(f"Retrieved {len(results)} papers from page {page}. Total so far: {len(papers)}")
    
    # Process each paper
    for paper in results:
        print(f"DEBUG: Paper object: {paper}")  # Inspect the paper object
        # Insert paper data
        paper_data = {
            "title": paper.get("title", "No title available"),
            "citations": paper.get("cited_by_count", 0),
            "year": paper.get("publication_year", "N/A"),
            "doi": paper.get("doi", "N/A"),
            "abstract": paper.get("abstract", "N/A")
        }
        insert_data(conn, "papers", paper_data)
        
        # Fetch and insert authors
        for authorship in paper.get("authorships", []):
            author = authorship.get("author", {})
            author_id = author.get("id")
            if author_id:
                print(f"DEBUG: Author ID: {author_id}")  # Inspect the author ID
                author_data = fetch_data(author_id)
                if author_data:
                    print(f"DEBUG: Author data: {author_data}")  # Inspect the author data
                    insert_data(conn, "authors", {
                        "name": author_data.get("display_name", "Unknown"),
                        "openalex_id": author_id
                    })
                    # Insert paper-author relationship
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO paper_author (paper_id, author_id) VALUES ((SELECT id FROM papers WHERE doi = %s), (SELECT id FROM authors WHERE openalex_id = %s)) ON CONFLICT DO NOTHING",
                            (paper_data["doi"], author_id)
                        )
                    conn.commit()
        
        # Fetch and insert source
        primary_location = paper.get("primary_location")  # Get primary_location first
        if primary_location:  # Check if primary_location exists
            source = primary_location.get("source")  # Then get source
            if source:  # Check if source exists
                source_id = source.get("id")
                if source_id:
                    source_data = fetch_data(source_id)
                    if source_data:
                        insert_data(conn, "source", {
                            "name": source_data.get("display_name", "N/A"),
                            "openalex_id": source_id
                        })
            else:
                print("Warning: No 'source' found in primary_location") # Helpful debugging
        else:
            print("Warning: No 'primary_location' found in paper") # Helpful debugging


            # Fetch and insert locations (institutions) - similar fix needed here:
            for authorship in paper.get("authorships", []):
                for institution in authorship.get("institutions", []):
                    institution_id = institution.get("id")
                    if institution_id:
                        institution_data = fetch_data(institution_id)
                        if institution_data:
                            insert_data(conn, "location", {
                                "name": institution_data.get("display_name", "N/A"),
                                "openalex_id": institution_id
                            })
    
    # Move to the next page
    page += 1

# Close the database connection
conn.close()

