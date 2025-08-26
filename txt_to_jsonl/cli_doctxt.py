import argparse
import json
import re
import os
import fnmatch
from pathlib import Path
from typing import Generator, Iterable, List, Optional, Tuple

from .cli import (
    build_title,
    ensure_output_dir,
    write_sharded_jsonl,
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Đọc file .txt chứa nhiều khối <doc>...</doc> (1 file hoặc cả thư mục) và xuất JSONL: "
            "mỗi <doc> là một object (title, summary)."
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
        help="Một hoặc nhiều thư mục chứa các file cần quét (hỗ trợ đệ quy).",
    )

    parser.add_argument(
        "--glob-pattern",
        default="*.txt",
        help="Mẫu file cần lấy khi dùng --input-dirs (mặc định: *.txt). Ví dụ: * để lấy tất cả.",
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
        default="dataset_doctxt",
        help="Tiền tố tên file .jsonl (mặc định: dataset_doctxt).",
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
        help="Số ký tự đầu tiên của nội dung <doc> làm summary (mặc định: 1024).",
    )
    parser.add_argument(
        "--title-source",
        choices=["filename", "fixed"],
        default="filename",
        help=(
            "Nguồn title fallback khi khối <doc> không có title: filename (tên file) hoặc fixed ('Document')."
        ),
    )
    parser.add_argument(
        "--title-max-chars",
        type=int,
        default=20,
        help="Giới hạn ký tự cho title (mặc định: 20).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ hiển thị 1-2 bản ghi ví dụ, không ghi file.",
    )
    return parser.parse_args(argv)


def extract_title_from_opening_tag(line: str) -> Optional[str]:
    match = re.search(r'title="(.*?)"', line)
    if match:
        return match.group(1)
    return None


def iter_doc_blocks(file_path: Path) -> Generator[Tuple[Optional[str], str], None, None]:
    """Yield (title_in_tag, content_text) for each <doc>...</doc> block."""
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


def read_first_n_chars_from_text(text: str, limit: int) -> str:
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


def generate_records_for_file(
    file_path: Path,
    summary_chars: int,
    title_source: str,
    title_max_chars: int,
) -> Generator[dict, None, None]:
    fallback_title_filename = build_title(file_path, "filename", title_max_chars)
    fallback_title_fixed = "Document"

    for title_in_tag, content in iter_doc_blocks(file_path):
        if title_in_tag and title_in_tag.strip():
            raw_title = title_in_tag.strip()
        else:
            raw_title = fallback_title_filename if title_source == "filename" else fallback_title_fixed

        title = raw_title[:title_max_chars] if title_max_chars > 0 else raw_title
        summary = read_first_n_chars_from_text(content, summary_chars)

        yield {
            "title": title,
            "summary": summary,
        }


def run(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    if args.input_file:
        files = [Path(args.input_file)]
    else:
        files = collect_files(args.input_dirs, args.glob_pattern, recursive=not args.no_subdirs)

    files = [p for p in files if p.exists() and p.is_file()]
    if not files:
        raise FileNotFoundError("Không tìm thấy file đầu vào hợp lệ.")

    if args.dry_run:
        first = files[0]
        count = 0
        for rec in generate_records_for_file(
            first,
            summary_chars=args.summary_chars,
            title_source=args.title_source,
            title_max_chars=args.title_max_chars,
        ):
            print(json.dumps(rec, ensure_ascii=False))
            count += 1
            if count >= 2:
                break
        return

    output_dir = ensure_output_dir(args.output_dir)
    total = write_sharded_jsonl(
        (
            rec
            for f in files
            for rec in generate_records_for_file(
                f,
                summary_chars=args.summary_chars,
                title_source=args.title_source,
                title_max_chars=args.title_max_chars,
            )
        ),
        output_dir=output_dir,
        prefix=args.prefix,
        max_records_per_file=args.max_records_per_file,
    )

    print(
        f"Đã ghi {total} doc vào thư mục '{output_dir}'. Tiền tố: '{args.prefix}'."
    )

if __name__ == "__main__":
    run()
