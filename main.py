import sys
import os
import time
import pandas as pd
from datetime import datetime


def process_csv(csv_file):
    # Load the csv file as a dataframe
    df = pd.read_csv(csv_file)

    # Filter only the online miners
    df['Hash Rate Numeric'] = df['Hash Rate RT'].str.extract(r'(\d+(\.\d+)?)', expand=False)[0].astype(float)

    # Sort by IP
    sorted_df = df.sort_values(by='IP')

    # Grab the online miner count per container
    online_count_df = pd.DataFrame(columns=['Container', 'Online'])
    for i in range(39):
        container_number = i + 1
        ip_pattern = (f"10.{'1' if (container_number < 22) else '2'}.{(container_number - 1) * 2}.",
                      f"10.{'1' if (container_number < 22) else '2'}.{(container_number * 2) - 1}.")
        container_ips = sorted_df[(sorted_df['IP'].str.startswith(ip_pattern[0])) |
                                  (sorted_df['IP'].str.startswith(ip_pattern[1]))]
        online_ips = container_ips[container_ips['Hash Rate Numeric'] > 0]
        online_miners = online_ips.shape[0]
        new_row = {'Container': container_number, 'Online': online_miners}
        online_count_df.loc[container_number] = new_row

    # Grab the issue miners
    blank_pool_df = df[df['Pool 1'].isnull()]
    zero_hashrate_df = df[df['Hash Rate Numeric'] <= 0]

    # Combine all issues into one dataframe
    issues_df = pd.concat([zero_hashrate_df, blank_pool_df]).drop_duplicates()

    # Populate the issue miner sheet
    issue_miner_df = pd.DataFrame(columns=['IP', 'Issue'])
    issue_miner_df['IP'] = issues_df['IP']
    issue_miner_df['Issue'] = issues_df.apply(lambda row:
                                              'Blank Pool' if pd.isnull(row['Pool 1']) else
                                              'Zero Hashrate', axis=1)

    # Get the total online count
    online_count = online_count_df['Online'].sum()
    online_row = {'Container': 'Online', 'Online': online_count}
    online_count_df.loc[40] = online_row

    # Get the total issue count
    issue_count = issue_miner_df.shape[0]
    issue_row = {'Container': 'Issue', 'Online': issue_count}
    online_count_df.loc[41] = issue_row

    total_count = online_count + issue_count
    total_row = {'Container': 'Total', 'Online': total_count}
    online_count_df.loc[42] = total_row

    # Save the Excel spreadsheet
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    #output_folder = "C:/Users/Admin/Desktop/E/exports"
    output_folder = "exports"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    output_file = f"{output_folder}/miners_{timestamp}.xlsx"

    with pd.ExcelWriter(output_file) as writer:
        online_count_df.to_excel(writer, sheet_name='Sheet1', index=False)
        issue_miner_df.to_excel(writer, sheet_name='Sheet2', index=False)

    print(f"Output saved to {output_file}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python3 main.py <csv_file>')
    else:
        print(f'Processing {sys.argv[1]}')
        process_csv(sys.argv[1])
    print('Done! Closing in 5 seconds...')
    time.sleep(5)
