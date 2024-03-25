import argparse
import logging
import os
import sys
from typing import Iterator

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from model_pipeline import IndexedCombinations

logger = logging.getLogger(__name__)
fmt = logging.Formatter(
    fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
shell_handler = logging.StreamHandler(sys.stdout)
shell_handler.setLevel(logging.DEBUG)
shell_handler.setFormatter(fmt)
logger.addHandler(shell_handler)
logger.setLevel(logging.DEBUG)


def get_next_item(
    combinations_file: str, state_file: str
) -> Iterator[IndexedCombinations]:
    try:
        df_configs = pd.read_json(combinations_file)
    except Exception as e:
        raise e

    start_index = 0
    if os.path.exists(state_file):
        try:
            with open(state_file) as f:
                start_index = int(f.read())
        except Exception as e:
            logger.error(e)
            start_index = 0

    for i in range(start_index, df_configs.shape[0]):
        config_dict = df_configs.iloc[i].to_dict()
        config = IndexedCombinations(**config_dict)
        with open(state_file, mode="w") as f:
            count_simbols = f.write(str(config.index))
            if count_simbols == 0:
                raise Exception(f"Can't write state file {state_file}")
        yield config

    os.remove(state_file)


def gen_template(environment: Environment, combination: IndexedCombinations):
    template = environment.get_template(combination.template)

    queries: list = []
    if combination.read_percent != 0:
        queries.append(
            {
                "percent": combination.read_percent,
                "line": combination.queries[combination.read_query_id].query,
            }
        )
    if combination.write_percent != 0:
        queries.append(
            {
                "percent": combination.write_percent,
                "line": combination.queries[combination.write_query_id].query,
            }
        )

    if len(queries) > 1:
        mixed = True
        if queries[0].get("percent") > queries[1].get("percent"):
            queries[0], queries[1] = queries[1], queries[0]
        queries[1]["percent"] = queries[1]["percent"] + queries[0]["percent"]
        queries[1]["percent"] = queries[1]["percent"] * 2 - 100
        queries[0]["percent"] = queries[0]["percent"] * 2 - 100

    elif len(queries) == 0:
        raise Exception("No query for templ_sql")
    else:
        mixed = False

    return template.render(scale=combination.size, mixed=mixed, queries=queries)


def main(args):
    try:
        environment = Environment(loader=FileSystemLoader(args.templates))
        if os.path.exists(args.output):
            raise Exception("output dir exists")
        else:
            os.mkdir(args.output)
        for combination in get_next_item(args.pipeline, "./state"):
            logger.debug(combination)
            template = gen_template(environment, combination)
            filename_split = os.path.splitext(combination.template)
            filename_name = os.path.splitext(filename_split[0])
            filename = filename_name[0] + f"_{combination.index}" + filename_name[1]
            with open(
                os.path.join(args.output, filename), mode="w", encoding="utf-8"
            ) as message:
                message.write(template)
                print(f"... wrote {filename}")

    except Exception as e:
        logger.error(e)
        return 1


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--templates",
        type=str,
        required=True,
        default="./templates",
        help="путь до папки шаблонов запросов",
    )
    parser.add_argument(
        "-p",
        "--pipeline",
        type=str,
        required=True,
        default="./full_pipeline.json",
        help="путь до файла наборов на тестирование",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        default="./temp_sql",
        help="путь до папки с временными файлами sql скриптов",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
