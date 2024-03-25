import argparse
import logging
import sys
from typing import List
from pandas import DataFrame
from pydantic import TypeAdapter, ValidationError
import pandas as pd
import itertools


from model_pipeline import (
    CombinationsSettings,
    InputTmpl,
)

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
    ta = TypeAdapter(List[InputTmpl])
    try:
        with open(args.pipeline) as f:
            pipeline = ta.validate_json(f.read())
    except ValidationError as e:
        logger.error(e)
        return 1

    logger.debug(pipeline)

    combinations_items_list: List[CombinationsSettings] = []

    for item in pipeline:
        r_w_percents_match = list(zip(item.read.percent, item.write.percent))
        gen_template_params_list = list(
            itertools.product(item.size, item.connections, r_w_percents_match)
        )

        combinations = [
            CombinationsSettings(
                size=size,
                connections=connections,
                read_percent=r_w_p[0],
                write_percent=r_w_p[1],
                queries=item.queries,
                read_query_id=item.read.query,
                write_query_id=item.write.query,
                template=item.template,
            )
            for size, connections, r_w_p in gen_template_params_list
        ]
        combinations_items_list = combinations_items_list + combinations

    ta = TypeAdapter(List[CombinationsSettings])

    combinations_df: DataFrame = pd.DataFrame(ta.dump_python(combinations_items_list))
    combinations_df["pgpool"] = False
    combinations_df_copy = combinations_df.copy()
    combinations_df_copy["pgpool"] = True
    combinations_df = pd.concat([combinations_df, combinations_df_copy])
    combinations_df = combinations_df.sort_values(
        by=["size", "pgpool"], ignore_index=True
    )
    indexed_combinations_df = combinations_df.reset_index()

    # logger.debug(f"\n {indexed_combinations_df.head()}")
    logger.debug(f"count configurations: {indexed_combinations_df.shape[0]}")

    indexed_combinations_df.to_json(path_or_buf=args.output, orient="records", indent=2)

    return 0


def parse_args():
    parser = argparse.ArgumentParser()
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
        default="./full_pipeline.json",
        help="путь до файла с результирующими наборами конфигураций и id",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
