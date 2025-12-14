import socket
import json
import uuid
import os
import math
import base64
import hashlib
def file_to_bytes(path, chunk_size=1024, offset=0):
    with open(path, "rb") as f:
        while True:
            f.seek(offset*chunk_size)
            offset += 1
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk
class Client:
    def __init__(self, server_ip="127.0.0.1", server_port=9000):
        self.server_addr = (server_ip, server_port)
        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.bind(("0.0.0.0", 0))
        self.client.settimeout(2)
    def send_message(self, dict):
        message = json.dumps(dict).encode()
        self.client.sendto(message, self.server_addr)
    def receive_response(self):
        try:
            data, addr = self.client.recvfrom(4096)
            return data, addr
        except socket.timeout:
            return None, None
    def close(self):
        self.client.close()
client = Client()

file_path = "duck.png"
chunk_size = 1024
file_id = str(uuid.uuid4())
file_size = os.path.getsize(file_path)
total_chunks = (file_size % chunk_size) + file_size - (file_size % chunk_size)


for i, byte_chunk in enumerate(file_to_bytes(file_path, chunk_size)):
    dict = {"type": "DATA",
            "file_id": file_id,
            "file_name": file_path,
            "chunk_index": i,
            "total_chunks": total_chunks,
            "chunk_size": len(byte_chunk),
            "data": base64.b64encode(byte_chunk).decode("ascii"),
            "checksum": base64.b64encode(
            hashlib.sha256(byte_chunk).digest()
        ).decode("ascii")}
    print(f"Gửi chunk {i+1}/{dict}")
    client.send_message(dict)
    data, addr = client.receive_response()
    if data:
        print("Server trả:", data.decode())
    else:
        print("Timeout – chưa nhận ACK")
    
dict = {"type": "END",
        "file_id": str(uuid.uuid4()),
        "file_checksum": "def456",
        "status": "finished"}


# client.close()


