import argparse
import json
import os
from pathlib import Path
from typing import Generator, Iterable, List, Optional
import re


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quét thư mục chứa file .txt và xuất ra 1 hoặc nhiều file .jsonl "
            "với các trường: title, category, summary."
        )
    )
    parser.add_argument(
        "--input-dirs",
        nargs="+",
        required=True,
        help="Một hoặc nhiều thư mục đầu vào để quét .txt (hỗ trợ đệ quy).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Thư mục để ghi các file .jsonl đầu ra.",
    )
    parser.add_argument(
        "--prefix",
        default="dataset",
        help="Tiền tố tên file .jsonl (mặc định: dataset).",
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=100000,
        help="Số bản ghi tối đa mỗi file .jsonl trước khi tách file mới (mặc định: 50000).",
    )
    parser.add_argument(
        "--summary-chars",
        type=int,
        default=2048,
        help="Số ký tự cho mỗi đoạn summary (mặc định: 1024).",
    )
    parser.add_argument(
        "--title-source",
        choices=["filename", "firstline"],
        default="filename",
        help="Nguồn lấy title: từ tên file (filename) hoặc dòng đầu của file (firstline).",
    )
    parser.add_argument(
        "--title-max-chars",
        type=int,
        default=20,
        help="Giới hạn ký tự cho title (mặc định: 20).",
    )
    parser.add_argument(
        "--no-subdirs",
        action="store_true",
        help="Không quét thư mục con (mặc định có quét đệ quy).",
    )
    parser.add_argument(
        "--split-mode",
        choices=["single", "chunked"],
        default="single",
        help="Chế độ chia file: single (1 file = 1 object) hoặc chunked (1 file = nhiều object theo đoạn).",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=50,
        help="Số dòng chồng lấp giữa các đoạn khi dùng split-mode=chunked (mặc định: 50).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ đếm file và hiển thị ví dụ 1-2 bản ghi, không ghi file.",
    )
    return parser.parse_args(argv)


def collect_txt_files(input_dirs: Iterable[str], recursive: bool = True) -> List[Path]:
    collected: List[Path] = []
    for directory in input_dirs:
        base = Path(directory)
        if not base.exists() or not base.is_dir():
            continue
        if recursive:
            for root, _dirs, files in os.walk(base):
                root_path = Path(root)
                for name in files:
                    if name.lower().endswith(".txt"):
                        collected.append(root_path / name)
        else:
            for path in base.iterdir():
                if path.is_file() and path.name.lower().endswith(".txt"):
                    collected.append(path)
    collected = sorted(set(p.resolve() for p in collected))
    return collected


def read_first_n_chars(file_path: Path, limit: int) -> str:
    if limit <= 0:
        return ""
    chars_remaining = limit
    parts: List[str] = []
    with file_path.open("r", encoding="utf-8", errors="replace") as f:
        while chars_remaining > 0:
            chunk = f.read(min(8192, chars_remaining))
            if not chunk:
                break
            parts.append(chunk)
            chars_remaining -= len(chunk)
    return "".join(parts)


def read_file_chunks(file_path: Path, chunk_size: int, overlap: int) -> List[str]:
    """Đọc file và chia thành các đoạn văn bản có nghĩa với khoảng chồng lấp, đảm bảo không cắt câu."""
    if chunk_size <= 0:
        return []
    
    with file_path.open("r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    
    if not content:
        return []
    
    # Lọc bỏ các ký tự không phải tiếng Việt và các ký tự đặc biệt không mong muốn
    content = re.sub(r'[^\w\s.,?!:;()"\'\u00C0-\u1FFF]+', '', content)
    
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|!)\s', content)
    
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        if len(current_chunk) + len(sentence) + 1 <= chunk_size:
            current_chunk += sentence + " "  # Thêm câu vào chunk hiện tại
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())  # Lưu chunk hiện tại
            current_chunk = sentence + " "  # Bắt đầu một chunk mới
    
    if current_chunk:
        chunks.append(current_chunk.strip())  # Lưu chunk cuối cùng
    
    return chunks


def read_first_line(file_path: Path) -> str:
    with file_path.open("r", encoding="utf-8", errors="replace") as f:
        line = f.readline()
        return line.strip("\r\n")


def build_title(file_path: Path, title_source: str, title_max_chars: int) -> str:
    if title_source == "firstline":
        raw = read_first_line(file_path)
        if not raw:
            raw = file_path.stem
    else:
        raw = file_path.stem
    if title_max_chars > 0:
        return raw[:title_max_chars]
    return raw


def generate_records(
    files: Iterable[Path],
    input_dirs: List[str],
    summary_chars: int,
    title_source: str,
    title_max_chars: int,
    split_mode: str = "single",
    chunk_overlap: int = 50,
) -> Generator[dict, None, None]:
    # Chuyển đổi input_dirs thành Path objects
    input_paths = [Path(d).resolve() for d in input_dirs]
    
    def get_category_from_input_dir(file_path: Path) -> str:
        """Lấy category từ thư mục con trực tiếp của thư mục input"""
        file_path_resolved = file_path.resolve()
        
        # Tìm thư mục input chứa file này
        for input_path in input_paths:
            try:
                # Kiểm tra xem file có nằm trong thư mục input này không
                if file_path_resolved.is_relative_to(input_path):
                    # Lấy phần tương đối từ thư mục input
                    relative_path = file_path_resolved.relative_to(input_path)
                    # Lấy thư mục con đầu tiên (nếu có)
                    if len(relative_path.parts) > 1:
                        return relative_path.parts[0]
                    else:
                        # Nếu file nằm trực tiếp trong thư mục input
                        return input_path.name
            except ValueError:
                # File không nằm trong thư mục input này, tiếp tục với thư mục khác
                continue
        
        # Fallback: trả về tên thư mục cha trực tiếp của file
        return file_path.parent.name
    
    for file_path in files:
        try:
            title = build_title(file_path, title_source, title_max_chars)
            category = get_category_from_input_dir(file_path)
            
            if split_mode == "single":
                # Chế độ cũ: 1 file = 1 object
                summary = read_first_n_chars(file_path, summary_chars)
                yield {
                    "title": title,
                    "category": category,
                    "summary": summary,
                }
            else:
                # Chế độ mới: 1 file = nhiều object theo đoạn
                chunks = read_file_chunks(file_path, summary_chars, chunk_overlap)
                for i, chunk in enumerate(chunks):
                    yield {
                        "title": title,
                        "category": category,
                        "summary": chunk,
                        "chunk_index": i + 1,
                        "total_chunks": len(chunks),
                    }
        except Exception as exc:  # noqa: BLE001
            # Bỏ qua file lỗi, có thể in cảnh báo nếu cần
            print(f"[WARN] Lỗi khi xử lý {file_path}: {exc}")
            continue


def ensure_output_dir(path: str) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def write_sharded_jsonl(
    records: Iterable[dict],
    output_dir: Path,
    prefix: str,
    max_records_per_file: int,
) -> int:
    shard_index = 1
    record_in_shard = 0
    file_handle = None
    written_total = 0

    def open_new_shard(idx: int):
        filename = f"{prefix}_{idx:05d}.jsonl"
        return (output_dir / filename).open("w", encoding="utf-8", errors="replace")

    try:
        file_handle = open_new_shard(shard_index)
        for rec in records:
            if max_records_per_file > 0 and record_in_shard >= max_records_per_file:
                file_handle.close()
                shard_index += 1
                record_in_shard = 0
                file_handle = open_new_shard(shard_index)

            json_line = json.dumps(rec, ensure_ascii=False)
            file_handle.write(json_line + "\n")
            record_in_shard += 1
            written_total += 1
    finally:
        if file_handle is not None and not file_handle.closed:
            file_handle.close()

    return written_total


def run(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    files = collect_txt_files(args.input_dirs, recursive=not args.no_subdirs)
    print(f"Tìm thấy {len(files)} file .txt")

    if args.dry_run:
        # In thử 2 bản ghi ví dụ
        preview_files = files[:2]
        for rec in generate_records(
            preview_files,
            input_dirs=args.input_dirs,
            summary_chars=args.summary_chars,
            title_source=args.title_source,
            title_max_chars=args.title_max_chars,
            split_mode=args.split_mode,
            chunk_overlap=args.chunk_overlap,
        ):
            print(json.dumps(rec, ensure_ascii=False))
        return

    output_dir = ensure_output_dir(args.output_dir)
    total = write_sharded_jsonl(
        generate_records(
            files,
            input_dirs=args.input_dirs,
            summary_chars=args.summary_chars,
            title_source=args.title_source,
            title_max_chars=args.title_max_chars,
            split_mode=args.split_mode,
            chunk_overlap=args.chunk_overlap,
        ),
        output_dir=output_dir,
        prefix=args.prefix,
        max_records_per_file=args.max_records_per_file,
    )

    mode_info = f"split-mode={args.split_mode}"
    if args.split_mode == "chunked":
        mode_info += f", overlap={args.chunk_overlap} dòng"
    
    print(
        f"Đã ghi {total} bản ghi vào thư mục '{output_dir}'. "
        f"Tiền tố: '{args.prefix}', dung lượng mỗi file: {args.max_records_per_file} bản ghi. "
        f"Chế độ: {mode_info}."
    )
