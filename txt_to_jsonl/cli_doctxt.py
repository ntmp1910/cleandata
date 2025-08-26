import argparse
import json
import os
import re
import fnmatch
from pathlib import Path
from typing import Generator, Iterable, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Trích xuất các khối <doc>...</doc> từ 1 file hoặc thư mục nhiều file, "
            "xuất JSONL (mỗi doc là một object)."
        )
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--input-file",
        help="Đường dẫn tới file chứa các khối <doc>...</doc>.",
    )
    src.add_argument(
        "--input-dirs",
        nargs="+",
        help="Một hoặc nhiều thư mục chứa các file cần quét.",
    )

    parser.add_argument(
        "--glob-pattern",
        default="*.txt",
        help="Mẫu file khi dùng --input-dirs (mặc định: *.txt). Ví dụ: * để lấy tất cả.",
    )
    parser.add_argument(
        "--no-subdirs",
        action="store_true",
        help="Không quét thư mục con khi dùng --input-dirs (mặc định có quét).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Thư mục để ghi file .jsonl đầu ra.",
    )
    parser.add_argument(
        "--prefix",
        default="doctxt",
        help="Tiền tố tên file .jsonl (mặc định: doctxt).",
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=50000,
        help="Số bản ghi tối đa mỗi file .jsonl trước khi tách file mới (mặc định: 50000).",
    )
    parser.add_argument(
        "--summary-chars",
        type=int,
        default=1024,
        help="Số ký tự đầu của nội dung <doc> làm summary (mặc định: 1024).",
    )
    parser.add_argument(
        "--title-max-chars",
        type=int,
        default=120,
        help="Giới hạn ký tự cho title trích từ tag (mặc định: 120).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ in thử 1-2 bản ghi, không ghi file.",
    )
    return parser.parse_args()


DOC_TITLE_RE = re.compile(r'title="(.*?)"')


def extract_title_from_opening_tag(line: str) -> Optional[str]:
    match = DOC_TITLE_RE.search(line)
    return match.group(1) if match else None


def iter_doc_blocks(file_path: Path) -> Generator[Tuple[Optional[str], str], None, None]:
    inside = False
    buffer: List[str] = []
    title_in_tag: Optional[str] = None

    with file_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not inside:
                if "<doc" in line:
                    inside = True
                    buffer.clear()
                    title_in_tag = extract_title_from_opening_tag(line)
                continue
            else:
                if "</doc>" in line:
                    content = "".join(buffer)
                    yield (title_in_tag, content)
                    buffer.clear()
                    inside = False
                    title_in_tag = None
                else:
                    buffer.append(line)


def read_first_n_chars(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    return text[:limit]


def collect_files(input_dirs: List[str], pattern: str, recursive: bool) -> List[Path]:
    collected: List[Path] = []
    normalized_pattern = pattern or "*"
    for directory in input_dirs:
        base = Path(directory)
        if not base.exists() or not base.is_dir():
            continue
        if recursive:
            for root, _dirs, files in os.walk(base):
                root_path = Path(root)
                for name in files:
                    if fnmatch.fnmatch(name, normalized_pattern):
                        collected.append(root_path / name)
        else:
            for entry in base.iterdir():
                if entry.is_file() and fnmatch.fnmatch(entry.name, normalized_pattern):
                    collected.append(entry)
    return sorted(set(p.resolve() for p in collected))


def write_sharded_jsonl(
    records: Iterable[dict],
    output_dir: Path,
    prefix: str,
    max_records_per_file: int,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)

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


def generate_records_for_file(
    file_path: Path,
    summary_chars: int,
    title_max_chars: int,
) -> Generator[dict, None, None]:
    for title_in_tag, content in iter_doc_blocks(file_path):
        raw_title = (title_in_tag or "").strip()
        if not raw_title:
            # fallback: dùng tên file khi thiếu title trong tag
            raw_title = file_path.stem
        title = raw_title[:title_max_chars] if title_max_chars > 0 else raw_title
        summary = read_first_n_chars(content, summary_chars)
        yield {"title": title, "summary": summary}


def main() -> None:
    args = parse_args()

    if args.input_file:
        files = [Path(args.input_file)]
    else:
        files = collect_files(args.input_dirs, args.glob_pattern, recursive=not args.no_subdirs)

    files = [p for p in files if p.exists() and p.is_file()]
    if not files:
        raise FileNotFoundError("Không tìm thấy file đầu vào hợp lệ.")

    if args.dry_run:
        # In thử 2 record đầu từ file đầu tiên
        first = files[0]
        for i, rec in enumerate(
            generate_records_for_file(first, args.summary_chars, args.title_max_chars)
        ):
            print(json.dumps(rec, ensure_ascii=False))
            if i >= 1:
                break
        return

    total = write_sharded_jsonl(
        (
            rec
            for f in files
            for rec in generate_records_for_file(f, args.summary_chars, args.title_max_chars)
        ),
        output_dir=Path(args.output_dir),
        prefix=args.prefix,
        max_records_per_file=args.max_records_per_file,
    )

    print(f"Đã ghi {total} doc vào thư mục '{args.output_dir}'. Tiền tố: '{args.prefix}'.")


if __name__ == "__main__":
    main()
