import argparse
import logging
import sys
import os

from jinja2 import Environment, FileSystemLoader
import pandas as pd

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


def main(args):
    try:
        df_configs = pd.read_json(args.pipeline)
    except Exception as e:
        logger.error(e)
        return 1

    config_dict = df_configs.iloc[5].to_dict()
    config = IndexedCombinations(**config_dict)
    logger.debug(config)

    environment = Environment(loader=FileSystemLoader(args.templates))
    template = environment.get_template(config.template)

    queries: list = []
    if config.read_percent != 0:
        queries.append(
            {
                "percent": config.read_percent,
                "line": config.queries[config.read_query_id].query,
            }
        )
    if config.write_percent != 0:
        queries.append(
            {
                "percent": config.write_percent,
                "line": config.queries[config.write_query_id].query,
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
        raise Exception("No query for templste")
    else:
        mixed = False

    content = template.render(scale=config.size, mixed=mixed, queries=queries)
    filename_split = os.path.splitext(config.template)
    filename_name = os.path.splitext(filename_split[0])
    filename = filename_name[0] + f"_{config.index}" + filename_name[1]
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)
        print(f"... wrote {filename}")


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
        default="./pipeline.json",
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
