import requests
import json
import sys
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
        response = requests.get(tag_mapping_url, timeout=30)
        response.raise_for_status()
        data = response.json()

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
    except requests.RequestException as e:
        print(f"错误: 获取标签映射失败: {e}", file=sys.stderr)
        print("程序已退出")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"错误: 解析标签映射JSON失败: {e}", file=sys.stderr)
        print("程序已退出")
        sys.exit(1)

    print()

    # 开始爬取数据
    page_num = 1
    all_items = []
    found_stop_id = False

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
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"错误: 请求第 {page_num} 页失败: {e}", file=sys.stderr)
            print("程序已退出")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"错误: 解析第 {page_num} 页JSON失败: {e}", file=sys.stderr)
            print("程序已退出")
            sys.exit(1)

        if data.get("code") != 200:
            print(f"错误: 获取第 {page_num} 页失败，响应码: {data.get('code')}", file=sys.stderr)
            print("程序已退出")
            sys.exit(1)

        items = data.get("data", {}).get("list", [])

        if not items:
            print(f"第 {page_num} 页没有数据，停止爬取")
            break

        # 处理当前页的数据
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
    for item in all_items:
        item_id = item.get("id", "")
        barrage = item.get("barrage", "")
        tags = item.get("tags", "")

        # 将标签字符串映射为标签名称
        mapped_tags_list = []
        if tags:
            tag_list = tags.split(",")
            for tag in tag_list:
                tag = tag.strip()
                if tag in tag_mapping:
                    mapped_value = tag_mapping[tag]
                    # 如果映射值为空，停止程序
                    if not mapped_value:
                        print(f"\n错误: 标签 '{tag}' 的映射值为空", file=sys.stderr)
                        print("程序已退出")
                        sys.exit(1)
                    mapped_tags_list.append(mapped_value)
                else:
                    # 未知标签，停止程序
                    print(f"\n错误: 遇到未知标签 '{tag}'", file=sys.stderr)
                    print("程序已退出")
                    sys.exit(1)

        # 移除弹幕内容中的换行符，替换为空格
        barrage = barrage.replace("\n", " ").replace("\r", " ").strip()

        # 生成三元组：[标签名, 包含烂梗, 烂梗内容]
        triples = []
        for tag_name in mapped_tags_list:
            triples.append([tag_name, "包含烂梗", barrage])

        # 构建OpenIE格式的对象
        openie_item = {
            "idx": f"sb6657-{item_id}",
            "passage": barrage,
            "extracted_entities": mapped_tags_list,
            "triples": triples
        }

        processed_items.append(openie_item)

    # 按1000条分段输出JSON文件
    segment_size = 1000
    total_files = (len(processed_items) + segment_size - 1) // segment_size

    for segment_idx in range(total_files):
        start_idx = segment_idx * segment_size
        end_idx = min(start_idx + segment_size, len(processed_items))

        segment_items = processed_items[start_idx:end_idx]
        first_item_id = segment_items[0]["idx"].split("-")[1]  # 获取第一个烂梗的ID

        output_file = output_dir / f"sb6657-{first_item_id}.json"

        print(f"正在写入文件: {output_file}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(segment_items, f, ensure_ascii=False, indent=2)

    print(f"完成！共生成 {total_files} 个JSON文件，数据已保存到 openie 目录")


if __name__ == "__main__":
    main()
