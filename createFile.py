import os
import random
import string
def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_files(directory, num_files, file_size_kb):
    os.makedirs(directory, exist_ok=True)
    for i in range(num_files):
        file_path = os.path.join(directory, f'file_{i}.txt')
        with open(file_path, 'w') as file:
            file.write(generate_random_string(file_size_kb * 1024))
        print(f'Created {file_path}')


create_files('C:/Users/19486/Downloads/generate_files', 100000, 100)