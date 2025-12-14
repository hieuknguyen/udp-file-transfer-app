import socket
import json

# Tạo socket UDP
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind IP + port
server.bind(("0.0.0.0", 9000))

print("UDP Server đang lắng nghe tại cổng 9000...")

while True:
    try:
        data, addr = server.recvfrom(4096)
        data = data.decode()          # bytes → string
        packet = json.loads(data)   # string → dict
        if packet["type"] == "DATA":
            dict = {"type": "ACK",
                    "file_id": packet["file_id"],
                    "chunk_index" : packet["chunk_index"],
                    "status": "RECEIVED"}
            server.sendto(json.dumps(dict).encode(), addr)
        print(data)
    except ConnectionResetError:
        print("Client đã đóng kết nối hoặc port không còn tồn tại")
    continue  # nhận dữ liệu
    