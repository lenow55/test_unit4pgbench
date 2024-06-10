from glob import glob
import json
import os

import pandas as pd

out_dir = "./testing_output/"


def get_need_query_part(queries_dicted_list):
    query_0 = ""
    query_1 = ""
    try:
        query_0 = queries_dicted_list[0]["query"]
    except IndexError:
        pass
    try:
        query_1 = queries_dicted_list[1]["query"]
    except IndexError:
        pass
    return pd.Series([query_0, query_1])


list_dfs = []
list_files = glob("./testing_output/full_tests_norm/*json")
for file in list_files:
    tmp_df = pd.read_json(file)
    tmp_df.set_index("index", inplace=True)
    tmp_df[["query_0", "query_1"]] = tmp_df["queries"].apply(get_need_query_part)
    list_dfs.append(tmp_df)
    # print(json.dumps(tmp_df.iloc[0:5].to_dict(orient="records"), indent=2))


df_concat = pd.concat(list_dfs, ignore_index=True)

subset = [
    "query_0",
    "query_1",
    "size",
    "connections",
    "read_percent",
    "write_percent",
    "read_query_id",
    "write_query_id",
    "template",
]


def group_get_compare(group):
    base_row = group.iloc[0]
    # print(base_row)
    new_row = base_row[
        [
            "size",
            "connections",
            "read_percent",
            "write_percent",
            "write_query_id",
            "read_query_id",
            "template",
        ]
    ]
    # print(new_row)
    if base_row["pgpool"]:
        try:
            second_row = group.iloc[1]
            new_row["queries_no_p"] = second_row["queries"]
            new_row["tps_no_p"] = second_row["tps"]
            new_row["process_file_no_p"] = second_row["process_file"]

            new_row["queries_p"] = base_row["queries"]
            new_row["tps_p"] = base_row["tps"]
            new_row["process_file_p"] = base_row["process_file"]

        except IndexError:
            new_row["queries_p"] = base_row["queries"]
            new_row["tps_p"] = base_row["tps"]
            new_row["process_file_p"] = base_row["process_file"]

            new_row["queries_no_p"] = ""
            new_row["tps_no_p"] = ""
            new_row["process_file_no_p"] = ""
    else:
        try:
            second_row = group.iloc[1]
            new_row["queries_no_p"] = base_row["queries"]
            new_row["tps_no_p"] = base_row["tps"]
            new_row["process_file_no_p"] = base_row["process_file"]

            new_row["queries_p"] = second_row["queries"]
            new_row["tps_p"] = second_row["tps"]
            new_row["process_file_p"] = second_row["process_file"]

        except IndexError:
            new_row["queries_no_p"] = base_row["queries"]
            new_row["tps_no_p"] = base_row["tps"]
            new_row["process_file_no_p"] = base_row["process_file"]

            new_row["queries_p"] = ""
            new_row["tps_p"] = ""
            new_row["process_file_p"] = ""

    new_row["count_in_group"] = len(group)
    # print(new_row)
    # print(type(new_row))
    return new_row


# сгруппировать по повторяющимся полностью колонкам в subset и поставить id группы
df_concat["group_id"] = df_concat.groupby(subset).ngroup()

count_groups = df_concat["group_id"].max()

list_series_concated = []
for i in range(0, count_groups + 1):
    group = df_concat[df_concat["group_id"] == i]
    # print(group)
    list_series_concated.append(group_get_compare(group).to_dict())

print(list_series_concated[:2])
print(type(list_series_concated[0]))
res_df_concated = pd.DataFrame(list_series_concated)

print(json.dumps(res_df_concated.iloc[0:2].to_dict(orient="records"), indent=2))

# print(df_concat["group_id"].value_counts().value_counts())

# print(
#     json.dumps(
#         df_concat.loc[df_concat["group_id"] == 17].to_dict(orient="records"), indent=2
#     )
# )

# print(new_dataframe.iloc[0:2].to_dict())
# print(json.dumps(new_dataframe.iloc[0:2].to_dict(), indent=2))
#
# print(new_dataframe.loc[new_dataframe["count_in_group"] > 2].to_dict(orient="records"))
#
res_df_concated.to_json(
    os.path.join(out_dir, "full_compared_df.json"), orient="records", indent=2
)
