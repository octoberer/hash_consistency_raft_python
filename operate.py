import json
import os
import random

from lru import lru_cache
k = 1
m = 60
my_lru_cache = lru_cache(k)
my_lru_cache.schedule_exit(m)
all_operation=['add','update','get','delete']
def add(file_path,properties_dict):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    try:
        with open(file_path, 'w') as file:
            file.write(json.dumps(properties_dict))  # 写入空内容，因为这里只是创建文件
            file.flush()  # 刷新缓冲区到磁盘
            os.fsync(file.fileno())  # 强制同步数据到磁盘
            if os.path.exists(file_path):
                print("文件写入成功。")
            else:
                print("文件写入失败。")
    except Exception as e:
        print('出错了',e)
    print(f'{file_path}新增成功')
def delete(file_path):
    # 删除内存里的数据
    my_lru_cache.deleteFile(file_path)
    print(f'{file_path}删除成功')
def update(file_path,properties_dict):
    my_lru_cache.updateFile(file_path, properties_dict)
    res=json.dumps(properties_dict)
    return res
def find(file_path,properties):
    file_content=my_lru_cache.visitFile(file_path)
    # print(type(file_content),file_content)
    if not isinstance(file_content, dict):
        file_content=json.loads(file_content)
    temp=[]
    # print('file_content',file_content)
    try:
        for key in properties:
            temp.append(f'{key}={file_content[key]}')
        temp.append(f'id={file_content["id"]}')
    except Exception as e:
        print('出错了,键',e,'不存在在该文件里')
    # print('find', temp, file_content_dict)
    return '  '.join(temp)

def schedule_operate(operate_str):
    op,rest_str=operate_str.split(' ',1)
    if op not in all_operation:
        print('错误操作字符串，不符合定义规范')
        return
    res = rest_str.split(' ', 1)
    directory=res[0]
    key_str=res[1] if len(res)>1 else None
    projectName, objectName, file_id = directory.split('.')
    file_path = f'{projectName}/{objectName}/{file_id}.txt'

    if op=='update' or op=='add':
        # 检查文件是否存在
        file_property_list = key_str.split(',')
        properties_dict = {}
        for prop in file_property_list:
            key_value = prop.split("=")
            if len(key_value) == 2:  # 确保分割后有两个元素
                properties_dict[key_value[0]] = key_value[1]
            properties_dict['projectName']=projectName
            properties_dict['objectName'] = objectName
            # properties_dict['id'] = file_id
        if os.path.exists(file_path):
            return update(file_path,properties_dict)
        else:
            return add(file_path,properties_dict)
    if os.path.exists(file_path):
        if op == 'get':
            properties = key_str.split(',')
            return find(file_path, properties)
        elif op == 'delete':
            return delete(file_path)
    else:
        print(f'文件不存在，无效{op}操作')

if __name__ == '__main__':
    # my_dict={"projectName":"A","objectName":"User","Id":"122212","age":"13","name":"heyuan"}
    # print(json_str)
    first_names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
    # for i in range(0,50):
    #     print(i)
    #     i=i+1
    #     first = random.choice(first_names)
    #     last = random.choice(last_names)
    #     print(schedule_operate(f"add A.user.{str(i).zfill(3)} name={first} {last},age={random.randint(0, 100)}"))
    # operate_list=["add A.user.002 name=我是第一个,age=90","get A.user.001 name,age","get A.user.002 name,age","update A.user.002 school=1757,age=14","get A.user.001 name,age","get A.user.002 name,age,school","delete A.user.002"]
    # i=1
    for i in range(0,50):
        print(schedule_operate(f"get A.user.{str(i).zfill(3)} name,age"))
        if random.randint(0, 50)>40:
            first = random.choice(first_names)
            last = random.choice(last_names)
            print('更新了',schedule_operate(f"update A.user.{str(random.randint(0, 50)).zfill(3)} name={first} {last},age={random.randint(0, 100)}"))
    for i in range(0,50):
        print(schedule_operate(f"get A.user.{str(random.randint(0, 50)).zfill(3)} name,age"))
        if random.randint(0, 50)>40:
            schedule_operate(f"delete A.user.{str(random.randint(0, 50)).zfill(3)}")

    # for op_str in operate_list:
    #     print(i)
    #     i=i+1
    #     print(schedule_operate(op_str))
    # for key in my_lru_cache.frequency_cache_dict:
    #     print(my_lru_cache.frequency_cache_dict[key])
# 查询语句
"get projectName.objectName.id property1,property2"
# 新增语句
"add projectName.objectName.id property1=value1,property2=value2"
# 删除语句
"delete projectName.objectName.id"
# 更新语句
"update projectName.objectName.id property1=value1,property2=value2"
