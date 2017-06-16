import os

if __name__ == '__main__':
    count = 0
    filtered_files = []
    root_dir = ''
    for root, dirs, files in os.walk('/home/yche/Data/reshensai'):
        filtered_files = filter(lambda file_name: 'xa' in file_name, files)
        root_dir = root
        break
    print filtered_files
    print root_dir

    for file_name in filtered_files:
        new_name = str(ord(file_name[-1]) - ord('a') + 1) + '.txt'
        prev_path = root_dir + os.sep + file_name
        new_path = root_dir + os.sep + new_name
        shell_script = ' '.join(['mv', prev_path, new_path])
        os.system(shell_script)
