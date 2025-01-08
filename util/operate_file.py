import asyncio
import re
import os
import json
import aiofiles

serve_num=5




# 异步添加文件
async def add_file(file_path, properties_dict, server_id):
    full_file_path = f'{server_id}/{file_path}'
    if os.path.exists(full_file_path):
        return
    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
    try:
        async with aiofiles.open(full_file_path, 'w') as file:
            # print(f'9999{properties_dict}==={type(properties_dict)}')
            await file.write(properties_dict)  # 异步写入空内容
            # await file.flush()  # 异步刷新缓冲区
            # # 使用 run_in_executor 来在单独线程中运行同步的 fsync 操作
            # loop = asyncio.get_event_loop()
            # await loop.run_in_executor(None, os.fsync, file.fileno())  # 强制同步到磁盘
            if os.path.exists(full_file_path):
                pass
                # print(f"{server_id}文件写入成功。")
            else:
                print("文件写入失败。")
    except Exception as e:
        print('出错了', e)


# 异步删除文件
async def delete_file(file_path, server_id):
    if not os.path.exists(f'{server_id}/{file_path}'):
        return
    try:
        os.remove(f'{server_id}/{file_path}')
        print(f"文件 {file_path} 删除成功。")
    except Exception as e:
        print(f"删除文件时出错: {e}")


# 异步更新文件
async def update_file(file_path, properties_dict, server_id):
    if not os.path.exists(f'{server_id}/{file_path}'):
        return
    full_file_path = f'{server_id}/{file_path}'
    try:
        async with aiofiles.open(full_file_path, 'r') as file:
            old_content=await file.read()
            file_content = json.loads(old_content)
            if isinstance(file_content,str):
                file_content = json.loads(file_content)
    except FileNotFoundError:
        print(f"Error: The file {full_file_path} does not exist.")
        return
    except json.JSONDecodeError:
        print(f"Error: The file {full_file_path} is not a valid JSON file.")
        return
    except Exception as e:
        print(f'file_content转换失败：{e}')

    if isinstance(file_content,dict):
        file_content.update(properties_dict)
        content=json.dumps(file_content)
        try:
            async with aiofiles.open(full_file_path, 'w') as file:
                await file.write(content)  # 异步写入更新后的内容
        except Exception as e:
            print(f"更新文件时出错: {e}")
