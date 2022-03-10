import socket

import tensorflow as tf

import random

TCP_IP = "localhost"
TCP_PORT = 6666
BUFFER_SIZE = 1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Loading pre-trained model...")
model = tf.keras.models.load_model("mnist_model")
print("Finished loading model.\n")

print("Waiting for client...")
conn, addr = s.accept()
print("Client connected:", addr)
print()

data_string = ""
collecting_data = True
while collecting_data:
    try:
        data = conn.recv(BUFFER_SIZE)
        if not data:
            collecting_data = False
        data_string += data.decode("utf-8")
        # print(data_string)
        # Trailing \n lol
        if data_string[-2] == "~":
            collecting_data = False
    except:
        collecting_data = False

print("Received", round(len(data_string) / 1000, 1), "KB of data from client.")
print("Performing inference on {} images...".format(data_string.count("&") + 1))

result = ""
for _ in range(data_string.count("&") + 1):
    result += str(random.randint(0, 9))
    result += "&"
result = result[:-1]
result += "\n"

conn.send(result.encode("utf-8"))

print("Inference complete. Returning classifications to client.\n")

while True:
    # Busy spin
    continue

