import json
import os
from glob import glob

"""
скрипт для преобразования набора json
в нормальный список json
"""


def parse_sequential_jsons(data):
    objects = []
    obj_str = ""
    depth = 0  # Счетчик для определения, когда объект начинается и заканчивается

    for char in data:
        if char == "{":
            if depth == 0:
                obj_str = char  # Начинаем новый объект
            else:
                obj_str += char
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                obj_str += char
                objects.append(json.loads(obj_str))
                obj_str = ""  # Сброс текущей строки объекта после завершения парсинга
            else:
                obj_str += char
        else:
            if depth > 0:
                obj_str += char

    return objects


out_dir = "./testing_output/full_tests_norm/"

list_files = glob("./testing_output/full_tests/*json")

for file in list_files:
    data = ""
    with open(file, "r") as f:
        data = f.read()
    if data != "":
        parsed_json_objects = parse_sequential_jsons(data)
        dir, filename_ext = os.path.split(file)
        filename_name = os.path.splitext(filename_ext)
        filename = filename_name[0] + "_norm" + filename_name[1]
        with open(
            os.path.join(out_dir, filename), mode="w", encoding="utf-8"
        ) as out_file:
            json.dump(parsed_json_objects, out_file, indent=2)
