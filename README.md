## txt → JSONL rút gọn

Công cụ này duyệt qua các thư mục chứa file `.txt` (có thể lồng nhau), và gom lại thành 1 hoặc nhiều file `.jsonl` với mỗi dòng là 1 object:

**Chế độ single (mặc định):**
- **title**: ~20 ký tự đầu tiên của tiêu đề (mặc định lấy theo tên file, không gồm phần mở rộng)
- **category**: tên thư mục con trực tiếp của thư mục input 
- **summary**: 2048 ký tự đầu tiên của nội dung file (mặc định)

**Chế độ chunked (chia đoạn):**
- **title**: ~20 ký tự đầu tiên của tiêu đề
- **category**: tên thư mục con trực tiếp của thư mục input
- **summary**: một đoạn nội dung (mặc định 2048 ký tự, có thể điều chỉnh)
- **chunk_index**: số thứ tự đoạn (1, 2, 3...)
- **total_chunks**: tổng số đoạn của file

### Cài đặt
- Yêu cầu: Python 3.8+
- Không cần cài thêm thư viện ngoài

### Cách chạy nhanh

**Chế độ single (1 file = 1 object):**
```powershell
python -m txt_to_jsonl --input-dirs data --output-dir out --prefix dataset --split-mode single --summary-chars 2048
```

**Chế độ chunked (1 file = nhiều object theo đoạn):**
```powershell
python -m txt_to_jsonl --input-dirs data --output-dir out --prefix dataset --split-mode chunked --summary-chars 2048
```

Hoặc dùng line continuation với backtick:
```powershell
python -m txt_to_jsonl `
  --input-dirs data `
  --output-dir out `
  --prefix dataset `
  --split-mode chunked `
  --summary-chars 2048 `
  --title-source filename `
  --title-max-chars 20
```

- Kết quả sẽ là các file: `out\dataset_00001.jsonl`, `out\dataset_00002.jsonl`, ...
- Mỗi dòng là một JSON object với các trường tương ứng theo chế độ đã chọn

### Tham số CLI
- **--input-dirs**: 1 hoặc nhiều thư mục đầu vào. Công cụ sẽ duyệt đệ quy các thư mục con và lấy tất cả `.txt`.
- **--output-dir**: thư mục để ghi file `.jsonl`.
- **--prefix**: tiền tố tên file output. Mặc định: `dataset`.
- **--max-records-per-file**: số dòng tối đa mỗi file `.jsonl` trước khi tách sang file tiếp theo. Mặc định: `100000`.
- **--summary-chars**: số ký tự tối đa cho mỗi đoạn summary (ảnh hưởng đến kích thước chunk khi dùng `split-mode=chunked`). Mặc định: `2048`.
- **--title-source**: `filename` (mặc định) hoặc `firstline`.
  - `filename`: dùng tên file (bỏ `.txt`) làm tiêu đề, sau đó cắt còn `--title-max-chars` ký tự.
  - `firstline`: dùng dòng đầu tiên trong file làm tiêu đề, sau đó cắt còn `--title-max-chars` ký tự.
- **--title-max-chars**: số ký tự tối đa cho `title`. Mặc định: `20`.
- **--no-subdirs**: chỉ quét thư mục cấp hiện tại, không quét thư mục con.
- **--split-mode**: `single` (mặc định) hoặc `chunked`.
  - `single`: 1 file .txt = 1 object JSONL
  - `chunked`: 1 file .txt = nhiều object JSONL theo đoạn
- **--chunk-overlap**:  Số dòng chồng lấp giữa các đoạn khi dùng `split-mode=chunked`.  Tính năng này hiện không được sử dụng.

### Ghi chú
- Mặc định mở file với mã hóa `utf-8` và `errors=replace` để an toàn với các trường hợp mã hóa không chuẩn.
- Nếu bạn muốn thay đổi logic lấy `title` (ví dụ: ưu tiên dòng đầu không rỗng trong file), có thể chỉnh trong `txt_to_jsonl/cli.py`.

### Ví dụ thêm
Chỉ quét thư mục hiện tại, không quét thư mục con, lấy `title` từ dòng đầu tiên của file:
```powershell
python -m txt_to_jsonl --input-dirs . --output-dir out --no-subdirs --title-source firstline
```

**Ví dụ chia đoạn:**
```powershell
python -m txt_to_jsonl --input-dirs data --output-dir out --split-mode chunked --summary-chars 2048
```

**Test trước với dry-run:**
```powershell
python -m txt_to_jsonl --input-dirs data --output-dir out --split-mode chunked --dry-run
```

**Khi chạy lệnh:**
```powershell
python -m txt_to_jsonl --input-dirs data --output-dir out --prefix dataset
```

### Cấu trúc thư mục hiện tại
```
└── cleandata-finetunexml\
    ├── data\           # Thư mục chứa các file .txt cần xử lý
    ├── out\            # Thư mục chứa kết quả .jsonl
    ├── txt_to_jsonl\   # Package CLI
    └── README.md       # Hướng dẫn sử dụng
```

## Trích xuất theo khối <doc>...</doc>

Dùng cho file .txt chứa nhiều khối bắt đầu bằng `<doc ...>` và kết thúc bằng `</doc>`.
Mỗi khối sẽ thành 1 object JSONL riêng.

```powershell
python -m txt_to_jsonl.cli_doctxt --input-file data\multi_docs.txt --output-dir out --prefix doctxt --summary-chars 1024 --title-source filename
```

- Trường sinh ra:
  - `title`: theo tên file (hoặc cố định nếu chọn `--title-source fixed`)
  - `category`: tên thư mục chứa file đầu vào
  - `summary`: 1024 ký tự đầu của nội dung bên trong mỗi `<doc>...</doc>`
  - `doc_index`: số thứ tự doc trong file (1, 2, ...)

Có thể thử trước với `--dry-run` để in mẫu 1-2 object.
