#!C:\Users\indeg\AppData\Local\Programs\Python\Python313\python.exe
import os
import json
import re
from colorama import init, Fore, Style
import gc
import sys
import traceback
from tqdm import tqdm

# 增加递归限制
sys.setrecursionlimit(10000)

# 初始化colorama
init()

def process_json_file(file_path, output_files, chunk_size=1024*1024):
    """处理单个JSON文件"""
    try:
        print(f"\n处理文件: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            # 使用生成器逐块读取文件
            buffer = ""
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                buffer += chunk
                # 查找最后一个完整的JSON对象
                last_complete = buffer.rfind('}')
                if last_complete != -1:
                    process_chunk(buffer[:last_complete+1], output_files)
                    buffer = buffer[last_complete+1:]
                
                # 清理内存
                gc.collect()
        
        # 处理剩余的buffer
        if buffer:
            process_chunk(buffer, output_files)
            
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {str(e)}")
        traceback.print_exc()

def process_chunk(content, output_files):
    """处理文件内容块"""
    for keyword in output_files.keys():
        # 使用更高效的正则表达式
        pattern = r'\b' + re.escape(keyword) + r'\b'
        for match in re.finditer(pattern, content, re.IGNORECASE):
            # 向前查找最近的左花括号
            start_pos = content.rfind('{', 0, match.start())
            if start_pos != -1:
                # 查找匹配的右花括号
                count = 1
                pos = start_pos + 1
                while count > 0 and pos < len(content):
                    if content[pos] == '{':
                        count += 1
                    elif content[pos] == '}':
                        count -= 1
                    pos += 1
                
                if count == 0:
                    # 提取完整的JSON对象
                    json_obj = content[start_pos:pos]
                    try:
                        # 验证JSON
                        json.loads(json_obj)
                        # 写入到对应的输出文件
                        output_files[keyword].write(json_obj + '\n')
                    except json.JSONDecodeError:
                        continue

def main():
    """主函数"""
    try:
        print("程序开始运行...")
        print(f"Python版本: {sys.version}")
        print(f"当前工作目录: {os.getcwd()}")
        
        # 配置参数
        input_dir = "D:/FARES Clean/2021/q2"
        output_dir = "D:/FARES Clean/keyword_data"
        
        print(f"检查输入目录: {input_dir}")
        if not os.path.exists(input_dir):
            print(f"错误: 输入目录不存在: {input_dir}")
            return
        
        print("输入目录存在，继续处理...")
        print(f"检查输入目录中的文件...")
        json_files = []
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
        
        if not json_files:
            print(f"错误: 在目录 {input_dir} 中没有找到JSON文件")
            return
        
        print(f"找到 {len(json_files)} 个JSON文件")
        print(f"输出目录将创建在: {output_dir}")
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 为每个关键词创建输出文件
        output_files = {}
        keywords = ["Metformin", "Propranolol", "Captopril", "Atorvastatin", 
                   "Sertraline", "Valproate", "Prednisone", "Amiodarone"]
        
        for keyword in keywords:
            filename = os.path.join(output_dir, f"{keyword}_data.json")
            output_files[keyword] = open(filename, 'a', encoding='utf-8')
            print(f"创建输出文件: {filename}")
        
        # 使用tqdm显示进度
        for file_path in tqdm(json_files, desc="处理文件"):
            process_json_file(file_path, output_files)
        
        # 关闭所有输出文件
        for keyword, file in output_files.items():
            file.close()
            print(f"已关闭文件: {keyword}_data.json")
        
        print("程序运行完成")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        print("错误详情:")
        traceback.print_exc()
    finally:
        # 确保所有文件都被关闭
        for file in output_files.values():
            try:
                file.close()
            except:
                pass

if __name__ == "__main__":
    main()