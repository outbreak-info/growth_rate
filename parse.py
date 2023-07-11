import datetime
import os

import numpy as np
import pandas as pd

# Used for testing random sample keep the header, then take only 1% of lines
# if random from [0,1] interval is greater than 0.01 the row will be skipped
# p = 0.01
# df = pd.read_csv(
#     "grs.csv.gz",
#     index_col=0,
#     header=0,
#     skiprows=lambda i: i > 0 and random.random() > p,
# )


def load_data(data_folder):
    json_path = os.path.join(data_folder, "grs.csv.gz")
    df = pd.read_csv(json_path, index_col=0, header=0)
    df = df[
        [
            "date",
            "loc",
            "lin",
            "N_7",
            "deltaN_7",
            "N_prev_7",
            "deltaN_prev_7",
            "Prevalence_7",
            "deltaPrevalence_7",
            "G_7",
            "deltaG_7",
        ]
    ]

    df["Prevalence_7_percentage"] = df["Prevalence_7"] * 100
    df["deltaPrevalence_7_percentage"] = df["deltaPrevalence_7"] * 100
    df["G_7_linear"] = (np.exp(df["G_7"]) - 1) * 100
    df["deltaG_7_linear"] = df["deltaG_7"] * np.exp(df["G_7"]) * 100
    df["snr"] = abs(df["G_7_linear"] / df["deltaG_7_linear"])
    df["invDeltaG_7"] = 1 / abs(df["deltaG_7"])
    df["confidenceInterval95"] = df["deltaG_7"] * np.exp(df["G_7"]) * 1.96 * 100
    df["confidenceInterval80"] = df["deltaG_7"] * np.exp(df["G_7"]) * 1.28 * 100
    df["confidenceInterval65"] = df["deltaG_7"] * np.exp(df["G_7"]) * 0.93 * 100
    df["confidenceInterval50"] = df["deltaG_7"] * np.exp(df["G_7"]) * 0.67 * 100
    df["confidenceInterval35"] = df["deltaG_7"] * np.exp(df["G_7"]) * 0.45 * 100
    df["confidenceInterval20"] = df["deltaG_7"] * np.exp(df["G_7"]) * 0.25 * 100
    df["confidenceInterval5"] = df["deltaG_7"] * np.exp(df["G_7"]) * 0.06 * 100

    # change date to datetime
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    # last 90 days and fillna with empty string
    df = df[df.date >= (datetime.datetime.now() - pd.to_timedelta("90day"))]
    # group by location and lineage
    df = df.groupby(["loc", "lin"]).agg(lambda x: list(x))
    # convert date back to string in format YYYY-MM-DD
    df["date"] = df["date"].apply(lambda x: [i.strftime("%Y-%m-%d") for i in x])
    df = df.transpose()
    # get loop over columns in dataframe
    for loc_lin, series in df.items():
        record = {}
        # list of dictionaries
        values = []
        # expand series into dataframe so we can convert to list of dict
        values_df = pd.DataFrame([pd.Series(x) for x in series], index=df.index).fillna(0)
        # change to dict
        values_dict = values_df.to_dict()
        for key, value in values_dict.items():
            values.append(value)

        record["_id"] = loc_lin[0] + "_" + loc_lin[1]
        record["location"] = loc_lin[0]
        record["lineage"] = loc_lin[1]
        record["values"] = values

        yield record


def custom_data_mapping(cls):
    return {
        "date": {"type": "date"},
        "location": {"type": "keyword"},
        "lineage": {"type": "keyword"},
        "values": {
            "properties": {
                "confidenceInterval5": {"type": "double"},
                "confidenceInterval20": {"type": "double"},
                "confidenceInterval35": {"type": "double"},
                "confidenceInterval50": {"type": "double"},
                "confidenceInterval65": {"type": "double"},
                "confidenceInterval80": {"type": "double"},
                "confidenceInterval95": {"type": "double"},
                "date": {"type": "date"},
                "deltaG_7": {"type": "double"},
                "deltaG_7_linear": {"type": "double"},
                "deltaN_7": {"type": "double"},
                "deltaN_prev_7": {"type": "double"},
                "deltaPrevalence_7": {"type": "double"},
                "deltaPrevalence_7_percentage": {"type": "double"},
                "G_7": {"type": "double"},
                "G_7_linear": {"type": "double"},
                "invDeltaG_7": {"type": "double"},
                "N_7": {"type": "double"},
                "N_prev_7": {"type": "double"},
                "Prevalence_7": {"type": "double"},
                "Prevalence_7_percentage": {"type": "double"},
                "snr": {"type": "double"},
            }
        },
    }
