import os
import csv
import random
import logging
from typing import List

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

SOURCE_DIR = os.getenv(
    "SOURCE_DIR", os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
)


def generate_from_csv(file_name: str) -> List[dict]:
    records = []
    with open(f"{SOURCE_DIR}/{file_name}", encoding="utf-8") as csv_file:
        csvReader = csv.DictReader(csv_file)
        for rows in csvReader:
            records.append(rows)
    return records


def get_product_map(file_name: str = "products.csv"):
    prod_map = {}
    for product in generate_from_csv(file_name):
        prod_map[product["id"]] = {
            "retail_price": product["retail_price"],
            "department": product["department"].lower(),
            "category": product["category"].lower(),
        }
    return prod_map


def get_location(
    *,
    country: str = "*",
    state: str = "*",
    postal_code: str = "*",
    location_data: list = generate_from_csv("world_pop.csv"),
) -> dict:
    """
    Returns random location based off specified distribution

    country = '*' OR country = 'USA' OR country={'USA':.75,'UK':.25}
    state = '*' OR state = 'California' OR state={'California':.75,'New York':.25}
    postal_code = '*' OR postal_code = '95060' OR postal_code={'94117':.75,'95060':.25}
    type checking is used to provide flexibility of inputs to function (ie. can be dict with proportions, or could be single string value)
    """
    universe = []
    if postal_code != "*":
        if isinstance(postal_code, str):
            universe += list(
                filter(lambda row: row["postal_code"] == postal_code, location_data)
            )
        elif isinstance(postal_code, dict):
            universe += list(
                filter(
                    lambda row: row["postal_code"] in postal_code.keys(), location_data
                )
            )
    if state != "*":
        if isinstance(state, str):
            universe += list(filter(lambda row: row["state"] == state, location_data))
        elif isinstance(state, dict):
            universe += list(
                filter(lambda row: row["state"] in state.keys(), location_data)
            )
    if country != "*":
        if isinstance(country, str):
            universe += list(
                filter(lambda row: row["country"] == country, location_data)
            )
        elif isinstance(country, dict):
            universe += list(
                filter(lambda row: row["country"] in country.keys(), location_data)
            )
    if len(universe) == 0:
        universe = location_data

    total_pop = sum([int(loc["population"]) for loc in universe])

    for loc in universe:
        loc["population"] = int(loc["population"])
        if isinstance(postal_code, dict):
            if loc["postal_code"] in postal_code.keys():
                loc["population"] = postal_code[loc["postal_code"]] * total_pop
        if isinstance(state, dict):
            if loc["state"] in state.keys():
                loc["population"] = (
                    state[loc["state"]]
                    * (
                        loc["population"]
                        / sum(
                            [
                                loc2["population"]
                                for loc2 in universe
                                if loc["state"] == loc2["state"]
                            ]
                        )
                    )
                    * total_pop
                )
        if isinstance(country, dict):
            if loc["country"] in country.keys():
                loc["population"] = (
                    country[loc["country"]]
                    * (
                        loc["population"]
                        / sum(
                            [
                                loc2["population"]
                                for loc2 in universe
                                if loc["country"] == loc2["country"]
                            ]
                        )
                    )
                    * total_pop
                )

    loc = random.choices(
        universe, weights=[loc["population"] / total_pop for loc in universe]
    )[0]
    return {
        "city": loc["city"],
        "state": loc["state"],
        "postal_code": loc["postal_code"],
        "country": loc["country"],
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
    }
