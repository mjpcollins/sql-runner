import glob
from importlib import import_module


def get_parameters(request):
    settings = import_module(request['params_import_path']).settings
    return settings


def get_scripts_to_run(request):
    fs = get_full_sql_files_list(request)
    file_structure = get_folder_structure(fs, request["sql_file_path"])
    return file_structure


def get_full_sql_files_list(request):
    sql_folder = request["sql_file_path"]
    all_files_in_sql_folder = glob.glob(f'{sql_folder}**',
                                        recursive=True)
    return all_files_in_sql_folder


def get_folder_structure(all_files, root_folder):
    all_sql_files = [f for f in all_files
                     if '.sql' == f[-4:]]
    top_level_folders = [f for f in all_files
                         if f not in all_sql_files and f != root_folder]
    ordered_files = []
    for folder_name in top_level_folders:
        files = [file for file in all_sql_files
                 if folder_name in file]
        if files:
            files.sort()
            ordered_files.append([folder_name, files])
    ordered_files.sort(key=lambda x: x[0])
    return [file[-1] for file in ordered_files]
