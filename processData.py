import re
import socket
import plistlib
from datetime import datetime, timedelta
import sqlite3
import argparse
import os
import select
import time

# Precompiled regular expression pattern for filtering relevant lines from the DX Cluster data stream
compiled_pattern = re.compile(r'(\d+\.\d{1,2})\s+([A-Z0-9/]+)\s+([+-]?\s?\d{1,2})\s*dB\s+\d+\s+(?:FT8|FT4|CW)')

# old regex for testing purposes. comment out when done using
# compiled_pattern = re.compile(r'(\d+\.\d{2})\s+([A-Z0-9/]+)\s+(?:FT8|FT4|CW)\s+([+-]?\s?\d{1,2})\s*dB')

# SQLite database file name
db_file = 'callsigns.db'


def setup_database():
    """
    Sets up the SQLite database and creates the necessary table if it doesn't exist.
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create table to store callsign information if it doesn't already exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS callsigns (
            zone INTEGER,
            band INTEGER,
            snr INTEGER,
            timestamp REAL,
            spotter TEXT
        )
    ''')
    conn.commit()
    return conn, cursor


def insert_batch(cursor, buffer_list):
    """
    Inserts a batch of data into the SQLite database using executemany.
    """
    cursor.executemany('''
        INSERT INTO callsigns (zone, band, snr, timestamp, spotter)
        VALUES (?, ?, ?, ?, ?)
    ''', buffer_list)


def delete_old_entries(cursor):
    """
    Deletes entries older than 15 minutes from the SQLite database to keep the data current.
    """
    time_ago = datetime.now().timestamp() - timedelta(minutes=15).total_seconds()
    cursor.execute('DELETE FROM callsigns WHERE timestamp <= ?', (time_ago,))


def search_list(call_sign, cty_list):
    """
    Search through the cty.plist file to find CQZone information for the provided callsign.
    """
    original_call_sign = call_sign  # Keep the original callsign for reference
    while len(call_sign) >= 1 and call_sign not in cty_list:
        call_sign = call_sign[:-1]
    if len(call_sign) == 0:
        return None
    else:
        return cty_list[call_sign]["CQZone"]


def calculate_band(freq):
    """
    Calculate the radio band category based on the frequency.
    """
    if 1800 <= freq <= 2000:
        return 160
    elif 3500 <= freq <= 4000:
        return 80
    elif 7000 <= freq <= 7300:
        return 40
    elif 10100 <= freq <= 10150:
        return 30
    elif 14000 <= freq <= 14350:
        return 20
    elif 18068 <= freq <= 18168:
        return 17
    elif 21000 <= freq <= 21450:
        return 15
    elif 24890 <= freq <= 24990:
        return 12
    elif 28000 <= freq <= 29700:
        return 10
    elif 50000 <= freq <= 54000:
        return 6
    return None


def reconnect(host, port, max_retries=10):
    """
    Attempt to reconnect to the DX Cluster server with an exponential backoff strategy.
    """
    retries = 0
    backoff_time = 5  # Start with 5 seconds of wait time, then double for each retry

    while retries < max_retries:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))  # Try reconnecting
            s.setblocking(0)  # Set socket to non-blocking mode
            print(f"Reconnected to {host}:{port}")
            return s
        except (socket.error, socket.timeout) as e:
            retries += 1
            print(f"Reconnection attempt {retries} failed. Error: {e}")

            if retries >= max_retries:
                print("Max retries reached. Exiting.")
                raise Exception("Unable to reconnect after multiple attempts.")

            print(f"Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)
            backoff_time *= 2  # Exponential backoff: double the wait time

    return None  # In case the loop exits without success


def run(host, port, spotter):
    """
    Main function to connect to the DX Cluster, receive and process data, and store it in the SQLite database.
    Handles connection timeouts, data processing, and reconnection attempts.
    """
    last_update_time = datetime.now().timestamp()

    # Load the cty.plist file with callsign information
    try:
        with open("cty.plist", 'rb') as infile:
            cty_list = plistlib.load(infile, dict_type=dict)
    except FileNotFoundError:
        print(f"Error: cty.plist not found.")
        return

    # Establish the initial socket connection
    s = reconnect(host, port)

    # for testing purposes -- comment out or delete when not in use
    # s.sendall(b'LZ3NY\n')
    # s.sendall(b'SET/SKIMMER\nSET/NORTTY\nSET/FT4\nSET/FT8\nSET/CW\n')

    # Set up the SQLite database
    conn, cursor = setup_database()

    buffer = ""  # Buffer to store incoming data
    buffer_entry_count = 0  # Track how many valid entries are processed between updates
    buffer_list = []  # List to hold data entries before inserting into the database

    while True:
        now = datetime.now()  # Cache the current time
        ready_to_read, _, _ = select.select([s], [], [], 1)  # Wait up to 1 second for data

        if ready_to_read:
            try:
                data = s.recv(1024).decode()  # Non-blocking read, only if data is available

                if not data:
                    print("Connection closed by server.")
                    s = reconnect(host, port)  # Reconnect if the connection is closed
                    continue

                buffer += data  # Append the received data to the buffer

            except UnicodeDecodeError as e:
                print(f"Decoding error: {e}")
                continue
            except socket.error as e:
                print(f"Socket error: {e}. Reconnecting...")
                s = reconnect(host, port)
                continue

            # Split the buffer by newlines; the last part may be incomplete
            lines = buffer.split('\n')
            buffer = lines[-1]  # Save the incomplete line back to the buffer

            for line in lines[:-1]:  # Process all complete lines
                spotter_string = spotter + "-#:"
                if ((" FT4 " in line) or (" FT8 " in line)) and spotter_string in line:
                    current_timestamp = now.timestamp()  # Use cached timestamp

                    match = compiled_pattern.search(line)  # Use the precompiled regex

                    if match:
                        frequency = match.group(1)
                        call_sign = match.group(2)
                        snr = match.group(3).replace(" ", "")
                        cq_zone = search_list(call_sign, cty_list)
                        band = calculate_band(float(frequency))

                        if band and cq_zone and snr:  # Skip invalid entries
                            # Add the enhanced callsign info to the buffer list
                            buffer_list.append((cq_zone, band, snr, current_timestamp, spotter))

                            buffer_entry_count += 1  # Increment the count of valid entries processed

        # Every 500 lines or 30 seconds, update the database with the new info
        if ((buffer_entry_count >= 500) or (now.timestamp() - last_update_time > 30)) and buffer_list:
            insert_batch(cursor, buffer_list)
            delete_old_entries(cursor)  # Keep the database size manageable
            conn.commit()  # Commit the changes

            last_update_time = now.timestamp()

            # Get the current time and print it along with the update message
            current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"Database updated on {current_time}. Processed {buffer_entry_count} total entries from the buffer.")

            # Reset buffer entry count and list after each update
            buffer_entry_count = 0
            buffer_list = []


if __name__ == '__main__':
    # Argument parser for command-line options
    parser = argparse.ArgumentParser(
        description="Connect to a DX Cluster, collect spotted callsigns, and store them in an SQLite database.")
    parser.add_argument("-a", "--address", help="Specify hostname/address of the DX Cluster",
                        default=os.getenv("DX_CLUSTER_HOST", "100.68.66.71"))
    parser.add_argument("-p", "--port", help="Specify port for the DX Cluster", type=int,
                        default=int(os.getenv("DX_CLUSTER_PORT", 7550)))
    parser.add_argument("-s", "--spotter", help="Specify the spotter name to track",
                        default=os.getenv("SPOTTER_NAME", "VE3EID"))

    args = parser.parse_args()

    # Run the main function with provided arguments
    run(args.address, args.port, args.spotter)
