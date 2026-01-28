# 🚀 Render Deployment Manifest

Tất cả code đã được chuẩn bị sẵn sàng để deploy lên Render. Do giới hạn về quyền truy cập Git trong môi trường này (Lỗi 403), bạn cần thực hiện bước cuối cùng sau đây:

### 1. Push Code lên GitHub
Mở terminal tại thư mục dự án và chạy lệnh sau:
```bash
git push origin master
```

### 2. Cấu hình trên Render.com
Khi tạo **Web Service** mới trên Render, hãy sử dụng các thông tin sau:

| Field | Value |
|-------|-------|
| **Runtime** | Python 3 |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `gunicorn api_server:app --bind 0.0.0.0:$PORT` |

### 3. Biến môi trường (Environment Variables)
**BẮT BUỘC** thêm các biến này trong phần **Environment** trên Render:

```env
PYTHON_VERSION = 3.12.0
SUPABASE_URL = https://nthbhmefjdqxwlmtmgry.supabase.co
SUPABASE_KEY = [Lấy từ file .env hiện tại của bạn]
AIMS_ENABLED = false
```

### 4. Kiểm tra
Sau khi deploy xong, dashboard của bạn sẽ có URL dạng: `https://crew-dashboard.onrender.com`
