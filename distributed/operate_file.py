
from operate import add, find, delete, update
import re
serve_num=5
def increment_server_name(server_name):
    # 使用正则表达式找到所有的数字
    numbers = re.findall(r'\d+', server_name)
    # 假设最后一个数字是需要递增的部分
    if numbers:
        last_number = (int(numbers[-1]) + 1)%serve_num
        # 替换掉原来的数字部分
        new_server_name = re.sub(r'\d+', str(last_number), server_name, count=1)
    else:
        # 如果没有找到数字，返回原字符串
        new_server_name = server_name
    return new_server_name

def add_file(file_path,properties_dict,server_id):
    add(f'{server_id}/{file_path}',properties_dict)
def get_file(file_path,properties,server_id):
    find(f'{server_id}/{file_path}',properties)
def delete_file(file_path, server_id):
    delete(f'{server_id}/{file_path}')
def update_file(file_path,properties_dict,server_id):
    update(f'{server_id}/{file_path}',properties_dict)