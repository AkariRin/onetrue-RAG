import json
from pathlib import Path


def main() -> None:
    openie_dir = Path("./openie")

    print("OpenIE JSON分块工具")
    print()

    # 获取输入
    input_filename = input("请输入OpenIE JSON文件名: ").strip()
    num_chunks = int(input("请输入分块数量: ").strip())

    # 验证输入
    input_path = openie_dir / input_filename
    if not input_path.exists():
        print(f"错误: 文件 {input_path} 不存在")
        return

    if num_chunks <= 0:
        print("错误: 分块数量必须大于0")
        return

    # 读取JSON文件
    print(f"正在读取文件: {input_filename}")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 获取文档
    docs = data.get("docs", [])
    total_docs = len(docs)

    if total_docs == 0:
        print("错误: JSON文件中没有文档")
        return

    if num_chunks > total_docs:
        print(f"警告: 分块数量({num_chunks})大于文档数({total_docs})，调整为{total_docs}")
        num_chunks = total_docs

    # 计算分块参数
    base_chunk_size = total_docs // num_chunks
    remainder = total_docs % num_chunks

    print(f"总文档数: {total_docs}")
    print(f"分块数量: {num_chunks}")
    print(f"基础分块大小: {base_chunk_size}")
    if remainder > 0:
        print(f"前{remainder}块各多1个文档")

    # 分割并写入文件
    print("\n开始写入分块文件...")
    file_stem = input_filename.rsplit('.', 1)[0]
    doc_index = 0

    for chunk_id in range(num_chunks):
        chunk_size = base_chunk_size + (1 if chunk_id < remainder else 0)
        chunk_data = docs[doc_index : doc_index + chunk_size]
        doc_index += chunk_size

        output_filename = f"{file_stem}-part{chunk_id + 1}.json"
        output_path = openie_dir / output_filename

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({"docs": chunk_data}, f, ensure_ascii=False, indent=4)

        print(f"✓ 已生成: {output_filename} ({len(chunk_data)} 个文档)")

    print(f"\n✓ 分块完成! 共生成 {num_chunks} 个文件")


if __name__ == "__main__":
    main()
