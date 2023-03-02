"""
    Date: 2023-02-24
    Author: Andrew Lindstrom
    Purpose:
        TriMet specfic GTFS parser
"""
import os

import logging
logging.basicConfig(
    level=10
)

import pandas as pd

from gtfs import GTFS, DATA_PATH, OUTPUT_PATH

class TriMet(GTFS):
    """container for TriMet specific operations
    ~ should be things like defining light rail vs. bus routes,
    doing analysis on specific routes
    """

    def __init__(
        self, 
        zip_path: os.PathLike = None
    ) -> None:
        super().__init__(zip_path)
        self.date = pd.to_datetime(
            os.path.basename(zip_path).split(".")[0][-10:],
            format="%Y_%m_%d"
        )
        self.route_summary = {}

    # assign stop ridership weights
    def stop_ridership(
        self,
        input_df: pd.DataFrame,
        fetch_data: bool = True,
        date: str = None
    ) -> pd.DataFrame:
        """
        """
        stop_data_path = os.path.join(
            DATA_PATH,
            "stop_level_ridership_data.csv"
        )
        if not os.path.isfile(stop_data_path):
            if fetch_data:
                from pdf_parser import main as pdf_main
                pdf_main()
            else:
                raise FileNotFoundError("Ridership data not found")
        stop_data = pd.read_csv(stop_data_path)
        if not date:
            sf = stop_data[
                pd.to_datetime(stop_data["date"]) == self.date
            ]
        else:
            sf = stop_data[
                pd.to_datetime(stop_data["date"]) == pd.to_datetime(date)
            ]
        if sf.empty:
            logging.warning(
                f"Did not find {self.date} in stop level data, using aggregate"
            )
            sf = stop_data.groupby(
                by="stop_id",
                as_index=False
            ).mean()
        s1 = sf[["stop_id","total_boardings"]]
        s1.columns = ["stop_id_1","stop_1_boardings"]
        s2 = sf[["stop_id","total_boardings"]]
        s2.columns = ["stop_id_2","stop_2_boardings"]
        df = input_df.merge(
            s1,
            on="stop_id_1",
            how="left"
        )
        df = df.merge(
            s2,
            on="stop_id_2",
            how="left"
        )
        pd.options.mode.chained_assignment = None
        df["weight"] = (df["stop_1_boardings"] * df["stop_2_boardings"]) / df["travel_time"]
        pd.options.mode.chained_assignment = "warn"
        return df

    def timetable_quality(
        self,
        route_id,
        **kwargs
    ) -> float:
        """get the one number for a route - calls self.summary"""
        # for more detail on methodology, see the gtfs.py file
        # or https://caltrain-hsr.blogspot.com/2010/07/metrics-that-matter.html
        if self.route_summary.get(route_id) is None:
            df = self.summary(
                route_id=route_id,
                sample_size=kwargs.get("sample_size",4)
            )
            self.route_summary[route_id] = df
        else:
            df = self.route_summary[route_id]
        # parse by direction
        res = {}
        for direction_id in df["direction_id"].unique():
            sf = df[df["direction_id"] == direction_id]
            sf = self.stop_ridership(sf,date=kwargs.get("ridership_date"))
            score = sf["weight"].mean()
            res[direction_id] = score
        return res
        return sf

def test():
    data_files = [
        'trimet_gtfs_2014_01_07.zip', 'trimet_gtfs_2021_01_07.zip', 
        'trimet_gtfs_2019_01_11.zip', 'trimet_gtfs_2020_01_03.zip', 
        'trimet_gtfs_2023_01_11.zip', 'trimet_gtfs_2022_01_03.zip'
    ]
    routes = [9,]
    score_data = []
    all_data = []
    for data_file in data_files:
        path = os.path.join(
            DATA_PATH, data_file
        )
        date = pd.to_datetime(data_file[12:22],format="%Y_%m_%d")
        logging.info(f"Starting analysis for {date}")
        tm = TriMet(path)
        for route in routes:
            logging.info(f"Starting route analysis for {route}")
            if route not in tm.routes["route_id"].unique() and str(route) not in tm.routes["route_id"].unique():
                print(tm.routes["route_id"].unique())
                logging.info(
                    f"Route {route} not found in dataset for {date}"
                )
                continue
            sample = 2
            tq = tm.timetable_quality(
                route_id=route,
                sample_size=sample,
                ridership_date="2023-01-11"
            )
            score_data.append(
                {
                    "date":date,
                    "route_id":route,
                    "direction_0_score":tq.get(0),
                    "direction_1_score":tq.get(1)
                }
            )
            full_data = tm.route_summary[route]

            full_data["date"] = date
            all_data.append(full_data)

    df = pd.concat(all_data)
    sf = pd.DataFrame(score_data)

    df.to_csv(
        os.path.join(OUTPUT_PATH, "full number  trimet data.csv"),
        index=False
    )
    sf.to_csv(
        os.path.join(OUTPUT_PATH, "scored number 9 trimet data.csv"),
        index=False
    )
    return

def main():
    """Process Driver."""
    data_files = [
        'trimet_gtfs_2014_01_07.zip', 'trimet_gtfs_2021_01_07.zip', 
        'trimet_gtfs_2019_01_11.zip', 'trimet_gtfs_2020_01_03.zip', 
        'trimet_gtfs_2023_01_11.zip', 'trimet_gtfs_2022_01_03.zip'
    ]
    routes_to_analyze = [
        2, 4, 6, 8, 9, 12, 14, 15,
        20, 33, 56, 57, 58, 72, 75, 
        17, 19, 90, 100, 190, 200, 290
    ]
    lr_routes = {90,100,190,200,290}
    score_data = []
    all_data = []
    for data_file in data_files:
        path = os.path.join(
            DATA_PATH, data_file
        )
        date = pd.to_datetime(data_file[12:22],format="%Y_%m_%d")
        logging.info(f"Starting analysis for {date}")
        tm = TriMet(path)
        for route in routes_to_analyze:
            logging.info(f"Starting route analysis for {route}")
            if route not in tm.routes["route_id"].unique() and str(route) not in tm.routes["route_id"].unique():
                logging.info(
                    f"Route {route} not found in dataset for {date}"
                )
                continue
            if route in lr_routes:
                sample = 1
            else:
                sample = 1
            tq = tm.timetable_quality(
                route_id=route,
                sample_size=sample,
                ridership_date="2023-01-11"
            )
            full_data = tm.route_summary[route]
            score_data.append(
                {
                    "date":date,
                    "route_id":route,
                    "direction_0_score":tq.get(0),
                    "direction_1_score":tq.get(1)
                }
            )
            full_data["date"] = date
            all_data.append(full_data)

    df = pd.concat(all_data)
    sf = pd.DataFrame(score_data)

    df.to_csv(
        os.path.join(OUTPUT_PATH, "full trimet data.csv"),
        index=False
    )
    sf.to_csv(
        os.path.join(OUTPUT_PATH, "scored trimet data.csv"),
        index=False
    )
    return True

if __name__ == "__main__":
    main()