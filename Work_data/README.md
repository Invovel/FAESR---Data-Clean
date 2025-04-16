# 药物不良反应数据提取工具

## 功能说明

本工具用于从FAERS（FDA Adverse Event Reporting System）数据中提取特定药物的相关信息。主要提取以下字段：

1. `drugindication`：药物适应症
2. `literaturereference`：文献引用
3. `route`：给药途径（以`[`开始，`]`结束的数组格式）
4. `substance_name`：药物成分名称（以`[`开始，`]`结束的数组格式）

## 使用方法

1. 确保输入目录包含需要处理的JSON文件
2. 运行脚本：`python data_marker_no_color.py`
3. 输出文件将保存在指定目录中，每个药物一个文件

## 输出格式

输出为JSON格式，每行一条记录，包含以下字段（如果存在）：
```json
{
    "drugindication": "...",
    "literaturereference": "...",
    "route": ["...", "..."],
    "substance_name": ["...", "..."]
}
```

## 注意事项

1. `route`和`substance_name`字段以数组形式存储，使用`[`和`]`作为边界
2. 如果某个字段在原始数据中不存在，则不会出现在输出中
3. 程序会自动处理大文件，采用分块读取方式以节省内存
4. 支持处理包含多个JSON对象的文件

## 错误处理

- 程序会自动跳过无法解析的JSON数据
- 所有错误信息会被记录并显示
- 确保输出目录具有写入权限

## 输出文件
程序会在输出目录中为每个关键词创建一个 JSON 文件，格式为 `{关键词}_data.json`，例如：
- Metformin_data.json
- Propranolol_data.json
- Captopril_data.json
- Atorvastatin_data.json
- Sertraline_data.json
- Valproate_data.json
- Prednisone_data.json
- Amiodarone_data.json

## 更新历史
- 2024-03-21: 初始版本，处理 8 个关键词
    输入目录为 `D:\FARES Clean\2024\q2` 
    处理的关键词
    - Metformin (二甲双胍)
    - Propranolol (普萘洛尔)
    - Captopril (卡托普利)
    - Atorvastatin (阿托伐他汀)
    - Sertraline (舍曲林)
    - Valproate (丙戊酸盐)
    - Prednisone (泼尼松)
    - Amiodarone (胺碘酮)

- 2024-04-10: 更新输入目录为 `D:\FARES Clean\2024\q2`
- 2024-04-10: 更新输入目录为 `D:\FARES Clean\2024\q3`
- 2024-04-10: 更新输入目录为 `D:\FARES Clean\2023\q1` 
- 2024-04-11: 更新输入目录为 `D:\FARES Clean\2023\q2`
- 2024-04-11: 更新输入目录为 `D:\FARES Clean\2023\q3` 