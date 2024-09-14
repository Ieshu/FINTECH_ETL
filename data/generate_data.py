import csv
import json
import pandas as pd
import random
from datetime import datetime
import os
import logging

# Set up logging
log_file_path = os.path.join(os.path.dirname(__file__), 'data_generation.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_data(num_rows):
    """Generates dummy transaction data."""
    categories = ['A', 'B', 'C']
    statuses = ['completed', 'failed', 'pending']
    
    data = []
    for i in range(num_rows):
        amount = round(random.uniform(100.0, 500.0), 2)
        discount = round(random.uniform(0.1, 0.3), 2)
        discounted_amount = round(amount * (1 - discount), 2)
        timestamp = f'2023-09-{random.randint(1, 30)}'
        year = datetime.strptime(timestamp, '%Y-%m-%d').year
        month = datetime.strptime(timestamp, '%Y-%m-%d').month
        row = {
            'transaction_id': i + 1,
            'amount': amount,
            'timestamp': timestamp,
            'category': random.choice(categories),
            'status': random.choice(statuses),
            'discount': discount,
            'discounted_amount': discounted_amount,
            'year': year,
            'month': month,
            'rank': i + 1
        }
        data.append(row)
    
    return data

def save_to_csv(data, file_path='data/transactions.csv'):
    """Save data to CSV file."""
    headers = ['transaction_id', 'amount', 'timestamp', 'category', 'status', 'discount', 'discounted_amount', 'year', 'month', 'rank']
    try:
        with open(file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        logging.info(f"{file_path} created with {len(data)} rows.")
    except Exception as e:
        logging.error(f"Failed to save CSV data: {e}")

def save_to_json(data, file_path='data/transactions.json'):
    """Save data to JSON file."""
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        logging.info(f"{file_path} created with {len(data)} rows.")
    except Exception as e:
        logging.error(f"Failed to save JSON data: {e}")

def save_to_txt(data, file_path='data/transactions.txt'):
    """Save data to TXT file."""
    try:
        with open(file_path, 'w') as file:
            headers = ', '.join(data[0].keys())
            file.write(f"{headers}\n")
            
            for row in data:
                row_data = ', '.join([str(value) for value in row.values()])
                file.write(f"{row_data}\n")
        logging.info(f"{file_path} created with {len(data)} rows.")
    except Exception as e:
        logging.error(f"Failed to save TXT data: {e}")

def save_to_parquet(data, file_path='data/transactions.parquet'):
    """Save data to Parquet file."""
    try:
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False)
        logging.info(f"{file_path} created with {len(data)} rows.")
    except Exception as e:
        logging.error(f"Failed to save Parquet data: {e}")

def save_to_html(data, file_path='data/sample_page.html'):
    """Save data to HTML file."""
    try:
        html_content = """
        <html>
            <head>
                <title>Transactions</title>
            </head>
            <body>
                <table border="1">
                    <tr>
                        <th>Transaction_ID</th>
                        <th>Amount</th>
                        <th>Timestamp</th>
                        <th>Category</th>
                        <th>Status</th>
                        <th>Discount</th>
                        <th>Discounted_Amount</th>
                        <th>Year</th>
                        <th>Month</th>
                        <th>Rank</th>
                    </tr>
        """
        
        for row in data:
            html_row = f"""
                <tr>
                    <td>{row['transaction_id']}</td>
                    <td>{row['amount']}</td>
                    <td>{row['timestamp']}</td>
                    <td>{row['category']}</td>
                    <td>{row['status']}</td>
                    <td>{row['discount']}</td>
                    <td>{row['discounted_amount']}</td>
                    <td>{row['year']}</td>
                    <td>{row['month']}</td>
                    <td>{row['rank']}</td>
                </tr>
            """
            html_content += html_row

        html_content += """
                </table>
            </body>
        </html>
        """

        with open(file_path, 'w') as file:
            file.write(html_content)
        logging.info(f"{file_path} created with {len(data)} rows.")
    except Exception as e:
        logging.error(f"Failed to save HTML data: {e}")

def generate_and_save_all_files(num_rows):
    """Generate data and save to all formats."""
    data = generate_data(num_rows)
    save_to_csv(data)
    save_to_json(data)
    save_to_txt(data)
    save_to_parquet(data)
    save_to_html(data)

if __name__ == "__main__":
    generate_and_save_all_files(1000)
