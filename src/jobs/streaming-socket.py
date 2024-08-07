import json  # to serialize the data
import socket
import time
import pandas as pd


def send_data_via_socket(file_path, host="127.0.0.1", port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # initiate the socket
    s.bind((host, port))  # bind the socket to address
    s.listen(1)  # listen to socket
    print(f"Listening to socket for connections on {host}:{port}")
    conn, addr = s.accept()  # accept connection, and assign connection and address
    print(f"Connection from {addr}")
    last_sent_index = 0
    try:
        with open(file_path, 'r') as f:
            # skip lines that were already sent
            for _ in range(last_sent_index):  # iterate through message from the last accessed line
                next(f)
            records = []  # to store chunks of the message
            for line in f:
                records.append(json.loads(line))  # parse the ine and append it to records
                if len(records) == chunk_size:  # check is we got the right size
                    messages = pd.DataFrame(records)  # transform records into dataframe
                    print(messages)
                    for message in messages.to_dict(orient="records"):  # [{column:value}, ..., {column:value}]
                        serialized_data = json.dumps(message).encode('utf-8')  # convert to json
                        conn.send(serialized_data + b'\n')  # send message
                        time.sleep(2)
                        last_sent_index += 1
                    records = []
    except (BrokenPipeError, ConnectionResetError):
        print("Client Disconnected.")
    finally:
        conn.close()  # close connection
        print("Connection closed.")


if __name__ == "__main__":
    send_data_via_socket("../datasets/yelp_academic_dataset_review.json")

