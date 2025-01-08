
all_operation=['add','update','get','delete']

def resolve_client(operate_str):
    try:
        op, rest_str = operate_str.split(' ', 1)
        if op not in all_operation:
            return {"operational": False, "tip": '错误请求方法，不符合定义规范'}
        res = rest_str.split(' ', 1)
        directory = res[0]
        key_str = res[1] if len(res) > 1 else None
        # print('directory',directory)
        projectName, objectName, file_id = directory.split('.')
        file_path = f'{projectName}/{objectName}/{file_id}.txt'
        if op == 'update' or op == 'add':
            # 检查文件是否存在
            file_property_list = key_str.split(',')
            properties_dict = {}
            for prop in file_property_list:
                key_value = prop.split("=")
                if len(key_value) == 2:  # 确保分割后有两个元素
                    properties_dict[key_value[0]] = key_value[1]
            return {"operational": True, "op": op, "file_path": file_path, "properties_dict": properties_dict}
        if op == 'get':
            properties = key_str.split(',')
            return {"operational": True,"op":op,"file_path":file_path, "properties":properties}
        elif op == 'delete':
            return {"operational": True,"op":op,"file_path":file_path}
    except Exception as e:
        print(f'resolve_client错误：{e}')
