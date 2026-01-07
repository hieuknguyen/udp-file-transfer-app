import socket 
import json
import base64
import hashlib
import os

# --- KHỞI TẠO SERVER & CẤU HÌNH MÔI TRƯỜNG ---


# Khởi tạo socket với giao thức UDP.
# Lý do chọn UDP: Tối ưu tốc độ truyền tải cho file lớn.
# Tuy nhiên, ta phải tự xử lý việc mất gói tin ở tầng ứng dụng.
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind vào 0.0.0.0 để lắng nghe trên tất cả các card mạng của máy này.
# Port 9000 là cổng đích mà client phải gửi dữ liệu tới.
server.bind(("0.0.0.0", 9000))

print("UDP Server đang lắng nghe tại cổng 9000...")
# Khởi tạo socket với giao thức UDP (SOCK_DGRAM).
# Lý do chọn UDP: Tối ưu tốc độ truyền tải cho file lớn, giảm độ trễ bắt tay (handshake) của TCP.
# Tuy nhiên, ta phải tự xử lý việc mất gói tin ở tầng ứng dụng.
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind vào 0.0.0.0 để lắng nghe trên tất cả các card mạng (network interfaces) của máy này.
# Port 9000 là cổng đích mà client phải gửi dữ liệu tới.
server.bind(("0.0.0.0", 9000))
server.settimeout(1)  # Giúp vòng lặp thoát nhanh khi không có dữ liệu (hữu ích để Ctrl+C)

print("UDP Server đang lắng nghe tại cổng 9000...")

OUTPUT_DIR = "received"
CHUNK_STRIDE = 1024  # Kích thước bước nhảy khi ghi file, PHẢI khớp với chunk_size của Client.
os.makedirs(OUTPUT_DIR, exist_ok=True) # Đảm bảo thư mục tồn tại, tránh lỗi FileNotFoundError.

# State Management Dictionary
# Vì UDP không giữ kết nối (stateless), server không biết gói tin nào thuộc về client nào.
# Biến này đóng vai trò như một "Session Manager" trong bộ nhớ RAM.
# Cấu trúc: Key là file_id (UUID), Value là dict chứa đường dẫn file và file handle đang mở.
# Giúp server xử lý đồng thời (concurrently) nhiều file từ nhiều client khác nhau.
file_states = {}  # file_id -> {"path": str, "fh": file_object}

# --- [B3E] ---

def sha256_b64_file(path, block_size=1024 * 1024):
    hasher = hashlib.sha256() #Tạo đối tượng băm
    with open(path, "rb") as f: #Mở file ở chế độ đọc nhị phân (rb)
        for block in iter(lambda: f.read(block_size), b""): #Đọc theo từng khối mặc định 1MB để tránh đọc hết vào Ram
            hasher.update(block) #Với mỗi khối, cập nhật giá trị băm sau khi đọc từng khối
    return base64.b64encode(hasher.digest()).decode("ascii") #Sau khi đọc hết file lấy digest nhị phân->mã hóa Base64
     # rồi giải mã sang chuỗi ascii để dùng trong JSON

# --- [B3S] VÒNG LẶP CHÍNH & XỬ LÝ IO ---
# IO chính là nhận gói tin UDP từ client, giải mã, ghi file, và gửi ACK và gửi lỗi NACK.

while True:
    try:
        # Nhận gói tin thô. 65535 là kích thước tối đa lý thuyết của 1 gói UDP packet.
        # hàm recvfrom để nhận bắt gói tin đi từ bất kỳ client nào. 
        # với data là nội dung gói tin, addr là địa chỉ (IP, port) của client gửi đến.
        # 65535 bytes là kích thước tối đa của gói UDP (2^16)
        data, addr = server.recvfrom(65535)
        
        # bắt gói tin ở dạng bytes nên ta cần giải mã nó.
        # giải mã bytes sang str ở dạng Json sau đó parse sang dict
        data = data.decode() 
        #parse JSON         
        packet = json.loads(data)   
        
        if packet["type"] == "DATA":
            file_id = packet["file_id"]
            # ép kiểu chunk_index về int để dễ xử lý. phòng trường hợp chunk_index là str
            chunk_index = int(packet["chunk_index"])
            # thò vao packet lấy value của key data sau đó parse sang bytes(ascii-quy định các con số với ký tự riêng)
            # mã hóa lại thành dạng byte chuỗi sau đó giãi mã base64 để lấy dữ liệu nhị phân gốc
            chunk_bytes = base64.b64decode(packet["data"].encode("ascii"))
            # Vỏ ngoài cùng (JSON String): IyB1... (Dữ liệu đóng hộp để gửi qua mạng).
            # Lớp vỏ đệm (ASCII Bytes): b'IyB1...' (Chuyển dạng để máy giải mã hiểu).
            # Cốt lõi (Decoded File): b'# udp...' (Nội dung file thực sự để ghi vào ổ cứng).


            # packet["checksum"] phải biến lại thành bytes để so sánh với hàm băm (vì bên client checksum đã được mã hóa base64)
            if base64.b64encode(hashlib.sha256(chunk_bytes).digest()) == packet["checksum"].encode("ascii"):
                
                # [STATE MANAGEMENT] Kiểm tra xem file này đã được mở chưa?
                state = file_states.get(file_id)
                
                if state is None:
                    # Nếu chưa có trong bộ nhớ -> Đây là chunk đầu tiên nhận được của file này.
                    # Lưu ý: Chunk đầu tiên nhận được KHÔNG NHẤT THIẾT là chunk 0 (do UDP lộn xộn).
                    original_name = os.path.basename(packet.get("file_name") or "received.bin")
                    out_path = os.path.join(OUTPUT_DIR, f"{file_id}_{original_name}")
                    
                    # Mở file chế độ "wb+" (Write Binary + Update).
                    # Cần mode '+' để có thể seek (nhảy) đến vị trí bất kỳ mà không xóa nội dung cũ.
                    # Lưu file handle vào RAM để tái sử dụng cho các chunk sau -> Tối ưu I/O.
                    file_states[file_id] = {"path": out_path, "fh": open(out_path, "wb+")}
                    state = file_states[file_id]

                # [CORE LOGIC - RANDOM ACCESS WRITE]
                # UDP có thể gửi chunk 2 đến trước chunk 1.
                # Ta dùng seek() để dời con trỏ ghi đến đúng vị trí offset của chunk đó.
                # Công thức: Vị trí = Số thứ tự chunk * Kích thước 1 chunk.
                state["fh"].seek(chunk_index * CHUNK_STRIDE)
                
                # Ghi dữ liệu nhị phân xuống đĩa cứng tại vị trí vừa seek.
                state["fh"].write(chunk_bytes)

                dict = {"type": "ACK", "file_id": file_id, "chunk_index": chunk_index, "status": "RECEIVED"}
                server.sendto(json.dumps(dict).encode(), addr)
            else:
                # ... (Xử lý lỗi checksum) ...
                dict = {"type": "ERROR", "file_id": file_id, "chunk_index": chunk_index, "error": "CHECKSUM_MISMATCH"}
                server.sendto(json.dumps(dict).encode(), addr)
            print(f"Nhận chunk {chunk_index}, {len(chunk_bytes)} bytes")
            
        elif packet["type"] == "END":
            # [RESOURCE CLEANUP] Xử lý khi Client báo đã gửi xong
            file_id = packet["file_id"]
            
            # Lấy trạng thái ra và XÓA luôn khỏi dictionary (pop) để giải phóng RAM.
            # Tránh memory leak nếu server chạy lâu dài.
            state = file_states.pop(file_id, None)
            
            if state is not None:
                # [QUAN TRỌNG] Đóng file handle để hệ điều hành ghi tất cả buffer xuống đĩa (flush).
                # Nếu không close, file có thể bị lỗi hoặc thiếu dữ liệu cuối cùng.
                state["fh"].close()
                
                # ... (Đoạn kiểm tra toàn vẹn file ) ...
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
    continue 

