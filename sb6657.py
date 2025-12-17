import requests
import json
import sys
import time
from pathlib import Path


def main():

    print("sb6657烂梗数据爬取工具")
    print()

    # 获取用户输入的停止ID
    while True:
        try:
            stop_id_input = input("请输入停止ID（爬取到此ID时停止，不包含此项）: ").strip()
            stop_id = int(stop_id_input)

            if stop_id < 0:
                print("错误: 停止ID必须大于等于 0，请重新输入")
                print()
                continue

            break
        except ValueError:
            print("错误: 请输入有效的整数")
            print()
        except KeyboardInterrupt:
            print("\n\n程序已退出")
            sys.exit(0)

    print()

    # 获取标签映射
    print("正在获取标签映射...")
    tag_mapping_url = "https://hguofichp.cn:10086/machine/dictList"
    tag_mapping = {}

    try:
        data = None
        for attempt in range(3):
            try:
                response = requests.get(tag_mapping_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except (requests.RequestException, json.JSONDecodeError) as e:
                if attempt < 2:
                    print(f"请求失败: {e}，重试中...")
                    time.sleep(1)
                else:
                    raise

        if data.get("code") == 200:
            items = data.get("data", [])
            for item in items:
                dict_value = item.get("dictValue", "")
                dict_label = item.get("dictLabel", "")
                if dict_value:
                    tag_mapping[dict_value] = dict_label
            print(f"成功获取 {len(tag_mapping)} 个标签映射")
        else:
            print(f"错误: 获取标签映射失败，响应码: {data.get('code')}", file=sys.stderr)
            print("程序已退出")
            sys.exit(1)
    except Exception as e:
        print(f"错误: 获取标签映射失败: {e}", file=sys.stderr)
        print("程序已退出")
        sys.exit(1)

    print()

    # 开始爬取数据
    page_num = 1
    all_items = []

    print(f"开始爬取数据，停止ID: {stop_id}")

    while True:
        print(f"正在获取第 {page_num} 页...")

        # 获取指定页码的数据
        url = "https://hguofichp.cn:10086/machine/Page"
        params = {
            "pageNum": page_num,
            "pageSize": 100
        }

        try:
            data = None
            for attempt in range(3):
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    break
                except (requests.RequestException, json.JSONDecodeError) as e:
                    if attempt < 2:
                        print(f"请求失败: {e}，重试中...")
                        time.sleep(1)
                    else:
                        raise

            if data.get("code") != 200:
                print(f"错误: 获取第 {page_num} 页失败，响应码: {data.get('code')}", file=sys.stderr)
                print("程序已退出")
                sys.exit(1)

        items = data.get("data", {}).get("list", [])

        if not items:
            print(f"第 {page_num} 页没有数据，停止爬取")
            break

        # 处理当前页的数据
        found_stop_id = False
        for item in items:
            item_id = item.get("id")

            if item_id == stop_id:
                print(f"找到停止ID {stop_id}，停止爬取")
                found_stop_id = True
                break

            all_items.append(item)

        if found_stop_id:
            break

        page_num += 1

    # 统计数量
    amount = len(all_items)

    # 创建openie输出目录
    output_dir = Path("openie")
    output_dir.mkdir(exist_ok=True)

    # 处理数据并分段输出为JSON
    print()
    print("[统计结果]")
    print(f"共获取 {amount} 个烂梗")
    print()
    print("正在生成OpenIE JSON文档...")

    # 处理标签映射和三元组生成
    processed_items = []
    total_entity_chars = 0
    total_entities = 0

    for item in all_items:
        item_id = item.get("id", "")
        barrage = item.get("barrage", "")
        tags = item.get("tags", "")

        # 将标签字符串映射为标签名称
        mapped_tags_list = ["烂梗"]  # 默认添加"烂梗"标签
        if tags:
            tag_list = tags.split(",")
            for tag_item in tag_list:
                tag_item = tag_item.strip()
                if tag_item in tag_mapping:
                    mapped_value = tag_mapping[tag_item]
                    # 如果映射值为空，停止程序
                    if not mapped_value:
                        print(f"\n错误: 标签 '{tag_item}' 的映射值为空", file=sys.stderr)
                        print("程序已退出")
                        sys.exit(1)
                    mapped_tags_list.append(mapped_value)
                else:
                    # 未知标签，停止程序
                    print(f"\n错误: 遇到未知标签 '{tag_item}'", file=sys.stderr)
                    print("程序已退出")
                    sys.exit(1)

        # 统计实体字符长度和单词数
        for entity in mapped_tags_list:
            total_entity_chars += len(entity)
            total_entities += 1

        # 移除弹幕内容中的换行符，替换为空格
        barrage = barrage.replace("\n", " ").replace("\r", " ").strip()

        # 转义XML中不合法的字符为数值字符引用
        # XML有效字符范围：
        # - #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
        # 超出范围的字符转换为 &#xNNNN; 形式
        if barrage:
            result = []
            for char in barrage:
                code = ord(char)
                # 检查字符是否在XML合法范围内
                if (code == 0x9 or code == 0xA or code == 0xD or
                    (0x20 <= code <= 0xD7FF) or
                    (0xE000 <= code <= 0xFFFD) or
                    (0x10000 <= code <= 0x10FFFF)):
                    result.append(char)
                else:
                    # 将超出范围的字符转换为XML数值字符引用
                    result.append(f'&#x{code:X};')
            barrage = ''.join(result)

        # 生成三元组：[标签名, 包含烂梗, 烂梗内容]
        triples = []
        for tag_name in mapped_tags_list:
            if not (tag_name == "烂梗"):
                triples.append([tag_name, "包含烂梗", barrage])

        # 构建OpenIE格式的对象
        openie_item = {
            "idx": f"sb6657-{item_id}",
            "passage": barrage,
            "extracted_entities": mapped_tags_list,
            "extracted_triples": triples
        }

        processed_items.append(openie_item)

    # 计算平均实体字符长度和单词数
    avg_ent_chars = round(total_entity_chars / total_entities, 1) if total_entities > 0 else 0

    # 按1000条分段输出JSON文件
    segment_size = 1000
    total_files = (len(processed_items) + segment_size - 1) // segment_size

    for segment_idx in range(total_files):
        start_idx = segment_idx * segment_size
        end_idx = min(start_idx + segment_size, len(processed_items))

        segment_items = processed_items[start_idx:end_idx]
        idx_str = segment_items[0]["idx"]
        idx_parts = idx_str.split("-")
        first_item_id = idx_parts[-1]  # 获取最后一个部分作为ID

        output_file = output_dir / f"sb6657-{first_item_id}.json"

        print(f"正在写入文件: {output_file}")

        segment_output = {
            "docs": segment_items,
            "avg_ent_chars": avg_ent_chars,
            "avg_ent_words": 1.0
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(segment_output, f, ensure_ascii=False, indent=2)

    print(f"完成！共生成 {total_files} 个JSON文件，数据已保存到 openie 目录")


if __name__ == "__main__":
    main()
