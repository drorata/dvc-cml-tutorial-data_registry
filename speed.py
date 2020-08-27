import datetime
import pickle
from typing import List

import fire
import pandas as pd
import requests


def get_speed_data_last_N_days(
    N: int,
    link_ids: List[int] = [4616226, 4616220],
    limit: int = 1_000_000,
    output_filename: str = None,
):
    last_date = datetime.datetime.now() - datetime.timedelta(1)
    first_date = last_date - datetime.timedelta(N + 1)
    last_date_str = datetime.datetime.strftime(last_date, "%Y-%m-%d")
    first_date_str = datetime.datetime.strftime(first_date, "%Y-%m-%d")
    dfs = [
        pd.DataFrame(
            requests.get(
                f"https://data.cityofnewyork.us/resource/i4gi-tjb9.json?"
                f"$select=link_id,speed,data_as_of&$where=data_as_of between "
                f"'{first_date_str}' and '{last_date_str}'"
                f"&link_id={link_id}&$limit={limit}"
            ).json()
        )
        for link_id in link_ids
    ]
    if any([df.shape[0] == limit for df in dfs]):
        raise ValueError("Limit seems to be too low. Not all values returned.")
    df = pd.concat(dfs, axis=0)
    df["ts"] = pd.to_datetime(df.data_as_of)
    df["speed"] = df.speed.astype("float")

    if output_filename is None:
        output_filename = f"speed_last_{N}_days.pkl"
    with open(output_filename, "wb") as f:
        pickle.dump(df.resample("D", on="ts")["speed"].mean(), f)


if __name__ == "__main__":
    fire.Fire(get_speed_data_last_N_days)
