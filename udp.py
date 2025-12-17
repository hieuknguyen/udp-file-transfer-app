import socket
import json
import base64
import hashlib
import os
# Tạo socket UDP
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind IP + port
server.bind(("0.0.0.0", 9000))

print("UDP Server đang lắng nghe tại cổng 9000...")

OUTPUT_DIR = "received"
CHUNK_STRIDE = 1024  # phải trùng với `chunk_size` cố định bên client
os.makedirs(OUTPUT_DIR, exist_ok=True)

file_states = {}  # file_id -> {"path": str, "fh": file}


def sha256_b64_file(path, block_size=1024 * 1024):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            hasher.update(block)
    return base64.b64encode(hasher.digest()).decode("ascii")

while True:
    try:
        data, addr = server.recvfrom(65535)
        data = data.decode()          
        packet = json.loads(data)   
        if packet["type"] == "DATA":
            file_id = packet["file_id"]
            chunk_index = int(packet["chunk_index"])
            chunk_bytes = base64.b64decode(packet["data"].encode("ascii"))

            if base64.b64encode(hashlib.sha256(chunk_bytes).digest()) == packet["checksum"].encode("ascii"):
                state = file_states.get(file_id)
                if state is None:
                    original_name = os.path.basename(packet.get("file_name") or "received.bin")
                    out_path = os.path.join(OUTPUT_DIR, f"{file_id}_{original_name}")
                    file_states[file_id] = {"path": out_path, "fh": open(out_path, "wb+")}
                    state = file_states[file_id]

                state["fh"].seek(chunk_index * CHUNK_STRIDE)
                state["fh"].write(chunk_bytes)

                dict = {"type": "ACK",
                        "file_id": file_id,
                        "chunk_index": chunk_index,
                        "status": "RECEIVED"}
                server.sendto(json.dumps(dict).encode(), addr)
            else:
                dict = {"type": "ERROR",
                        "file_id": file_id,
                        "chunk_index": chunk_index,
                        "error": "CHECKSUM_MISMATCH"}
                server.sendto(json.dumps(dict).encode(), addr)
            print(f"Nhận chunk {chunk_index}, {len(chunk_bytes)} bytes")
            
        elif packet["type"] == "END":
            file_id = packet["file_id"]
            state = file_states.pop(file_id, None)
            if state is not None:
                state["fh"].close()
                expected_file_checksum = packet.get("file_checksum")
                if expected_file_checksum is not None:
                    actual_file_checksum = sha256_b64_file(state["path"])
                    status = "OK" if actual_file_checksum == expected_file_checksum else "BAD_CHECKSUM"
                else:
                    status = "FINISHED"
                end_ack = {"type": "END_ACK", "file_id": file_id, "status": status}
                server.sendto(json.dumps(end_ack).encode(), addr)
                print(f"Đã lưu file: {state['path']} ({status})")

        # print(data)
    except ConnectionResetError:
        print("Client đã đóng kết nối hoặc port không còn tồn tại")
    continue  # nhận dữ liệu
    
