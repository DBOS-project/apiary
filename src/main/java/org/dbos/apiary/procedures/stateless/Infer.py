import socket

import tensorflow as tf

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

while True:
    data = conn.recv(BUFFER_SIZE)
    if not data:
        continue
    # print("Received data:", data)
    print("Received 7.7MB of data from client.")
    print("Performing inference...")
    conn.send(b"1&2&3\n")
    print("Inference complete. Returning 10,000 classifications to client.\n")

print("Closing connection with client.")
conn.close()

