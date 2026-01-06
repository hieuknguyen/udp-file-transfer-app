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

# def file_to_bytes1(path):
#     with open(path, "rb") as f:
#         chunk = f.read()
#         return chunk
            
# data = file_to_bytes1("duck.png")

class Client:
    def __init__(self, server_ip="127.0.0.1", server_port=9000):
        # Lưu địa chỉ Server (IP, port) để dùng cho sendto()
        self.server_addr = (server_ip, server_port)
         # Tạo socket UDP (SOCK_DGRAM)
        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
         # Bind client vào một cổng nguồn bất kỳ do OS cấp phát giúp cố định port trong suốt phiên chạy
        self.client.bind(("0.0.0.0", 0))
        # Thiết lập timeout cho recvfrom()
        # Dùng để phát hiện mất ACK → nền tảng cho retransmission
        self.client.settimeout(2)
        
    def send_message(self, dict):
         # Chuyển packet dạng dict → JSON string → bytes
        message = json.dumps(dict).encode()
         # Gửi packet UDP đến Server
        self.client.sendto(message, self.server_addr)
        
    def receive_response(self):
        try:
            # Nhận phản hồi từ Server (ACK hoặc ERROR)
            data, addr = self.client.recvfrom(4096)
            return data, addr
        except socket.timeout:
            # Nếu timeout xảy ra:
            # - Có thể gói DATA bị mất
            # - Hoặc ACK từ Server bị mất
            # Trong cả hai trường hợp, Client cần retransmit ở phiên bản nâng cao.
            return None, None
            
    def close(self):
        # Đóng socket UDP
        self.client.close()

# Tạo đối tượng Client
client = Client()
# Đường dẫn file cần gửi
file_path = "a.zip"
# chunk_size xác định kích thước mỗi packet dữ liệu.
# Chia nhỏ giúp tránh packet quá lớn và dễ retransmit
chunk_size = 1024
# Tạo file_id duy nhất cho phiên truyền
# Giúp Server phân biệt nhiều file / nhiều client
file_id = str(uuid.uuid4())
# Lấy kích thước file để tính tổng chunk
file_size = os.path.getsize(file_path)
# Tổng số chunk của file
# (Mục đích: theo dõi tiến độ và hỗ trợ ráp file phía Server)
total_chunks = (file_size % chunk_size) + file_size - (file_size % chunk_size)
#tạo đối tựong băm SHA256(chunk1 + chunk2 + chunk3)
file_hasher = hashlib.sha256()

    # Mỗi vòng lặp tương ứng với một DATA packet.
    # i đóng vai trò là chunk_index – vị trí của mảnh dữ liệu trong file gốc.
for i, byte_chunk in enumerate(file_to_bytes(file_path, chunk_size)):
     # Cập nhật hash tổng file 
    file_hasher.update(byte_chunk)
    
    # Đóng gói DATA packet dưới dạng JSON
    dict =  {"type": "DATA", 
            # file_id giúp Server biết chunk này thuộc về file nào
            "file_id": file_id,
            # Tên file (để Server đặt tên file output)
            "file_name": file_path,
            # chunk_index là chìa khóa để Server ráp file đúng vị trí.
            # Điều này đặc biệt quan trọng vì UDP không đảm bảo thứ tự gói tin.
            "chunk_index": i,
            # Tổng số chunk của file
            # Giúp Server theo dõi tiến độ và kiểm tra thiếu chunk
            "total_chunks": total_chunks,
            # Kích thước thực tế của chunk (chunk cuối có thể nhỏ hơn)
            "chunk_size": len(byte_chunk),
            # data chứa nội dung chunk đã được mã hóa base64.
            # Việc encode là bắt buộc vì JSON không hỗ trợ dữ liệu nhị phân.
            "data": base64.b64encode(byte_chunk).decode("ascii"),
            # checksum là mã băm SHA-256 của chunk.
            # Server sẽ tính lại checksum để phát hiện lỗi dữ liệu.
            "checksum": base64.b64encode(
            hashlib.sha256(byte_chunk).digest()
        ).decode("ascii")}
    # print(f"Gửi chunk {i+1}/{dict}")
    
    # Gửi DATA packet tới Server
    client.send_message(dict)
    # Chờ phản hồi từ Server (ACK / ERROR)
    data, addr = client.receive_response()
    if data:
        # Nếu nhận được phản hồi
        print("Server trả:", data.decode())
    else:
        # Nếu timeout (chưa nhận ACK)
        print("Timeout – chưa nhận ACK")
    
dict = {
        # Gói END báo hiệu đã gửi xong toàn bộ chunk
        "type": "END",
        # Gắn với file_id của phiên truyền
        "file_id": file_id,
        # Checksum tổng của toàn bộ file
        # Server dùng để kiểm tra file sau khi ráp xon
        "file_checksum": base64.b64encode(file_hasher.digest()).decode("ascii"),
        # Trạng thái kết thúc
        "status": "finished"}
# print(file_hasher.digest())
# print(base64.b64encode(file_hasher.digest()).decode("ascii"))
# print(base64.b64encode(hashlib.sha256(data).digest()).decode("ascii"))
# Gửi gói END tới Server
client.send_message(dict)

# client.close()




