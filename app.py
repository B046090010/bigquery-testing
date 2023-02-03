from google.cloud import bigquery
from time import process_time 

start = process_time()
client = bigquery.Client()

query = """
    SELECT subject AS subject, COUNT(*) AS num_duplicates
    FROM bigquery-public-data.github_repos.commits
    GROUP BY subject
    ORDER BY num_duplicates
    DESC LIMIT 100000
"""
job_config = bigquery.job.QueryJobConfig(use_query_cache=True)
results = client.query(query, job_config=job_config)

for row in results:
    subject = row['subject']
    num_duplicates = row['num_duplicates']
    # print(f'{subject:<20} | {num_duplicates:>9,}')
end = process_time()

print('-'*60)
print(f'Created: {results.created}')
print(f'Ended:   {results.ended}')
print(f'Bytes:   {results.total_bytes_processed:,}')
print(f'Process Time:   {end-start}')
