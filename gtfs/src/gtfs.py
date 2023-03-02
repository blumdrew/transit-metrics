"""
    Author: Andrew Lindstrom
    Date: 2023-02-02
    GTFS parser
"""
import os
import itertools
import logging

from zipfile import ZipFile

import pandas as pd
import numpy as np

DATA_PATH = os.path.join(
    os.path.dirname(
        os.path.dirname(
            __file__
        )
    ),
    "data"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(
        os.path.dirname(
            __file__
        )
    ),
    "output"
)


class GTFS(object):
    """container, etc."""
    files_to_read = {
        "agency.txt","stops.txt","routes.txt",
        "trips.txt","calendar_dates.txt","stop_times.txt",
        "shapes.txt"
    }
    def __init__(
        self,
        zip_path: os.PathLike = None
    ) -> None:
        if not zip_path:
            self.zip_path = os.path.join(
                DATA_PATH,
                "trimet_gtfs_2023_01_11.zip"
            )
        else:
            self.zip_path = zip_path
        # forward declaration for the linter
        self.agency = pd.DataFrame()
        self.stops = pd.DataFrame()
        self.routes = pd.DataFrame()
        self.trips = pd.DataFrame()
        self.calendar_dates = pd.DataFrame()
        self.stop_times = pd.DataFrame()
        self.shapes = pd.DataFrame()
    
        self._read_data()
        
    def _read_data(self):
        """read data, done on init"""
        tmp_path = os.path.join(
            os.path.dirname(self.zip_path), 
            f"tmp_path_{os.path.basename(self.zip_path).split('.zip')[0]}"
        )
        os.makedirs(tmp_path, exist_ok=True)
        with ZipFile(self.zip_path, 'r') as zf:
            for f in zf.filelist:
                if f.filename not in self.files_to_read:
                    continue
                attr_name = f.filename.split(".")[0]
                if not os.path.isfile(os.path.join(tmp_path, f.filename)):
                    fl = zf.extract(f.filename, tmp_path)
                    logging.debug(
                        f"Extracted data for {attr_name}"
                    )
                else:
                    fl = os.path.join(tmp_path, f.filename)
                    logging.debug(
                        f"Did not extract data for {attr_name}, file was already found"
                    )
                df = pd.read_csv(fl)
                self.__setattr__(attr_name, df)
        return

    def run_times(
        self,
    ) -> pd.DataFrame:
        """get run times min/max for each route"""
        df = self.stop_times
        df["arrival_time_num"] = df["arrival_time"].astype(str).apply(
            lambda x: int(x.split(":")[0]) + float(x.split(":")[1])/60 + float(x.split(":")[2])/3600
        )
        gf = df.groupby(
            by=["trip_id"],
            as_index=False
        ).agg(
            {
                "arrival_time_num":("min","max"),
                "shape_dist_traveled":"max"
            }
        )
        gf.columns = ["trip_id","start_time","end_time","distance"]
        gf = gf.merge(
            self.trips[["trip_id","route_id","direction_id","service_id"]],
            on="trip_id"
        )
        gf = gf.merge(
            self.routes[["route_id","route_short_name","route_long_name"]],
            on="route_id"
        )
        gf["trip_time"] = gf["end_time"] - gf["start_time"]
        
        return gf
        #gf.to_csv(os.path.join(os.path.dirname(__file__),"time.csv"),index=False)

    def route_frequencies(
        self
    ) -> pd.DataFrame:
        """get the route frequencies"""
        df = self.stop_times
        df = df.merge(
            self.trips[["trip_id","route_id","direction_id","service_id"]],
            on="trip_id"
        )
        df = df.merge(
            self.routes[["route_id","route_short_name","route_long_name"]],
            on="route_id"
        )
        df["departure_time_num"] = df["departure_time"].astype(str).apply(
            lambda x: int(x.split(":")[0]) + float(x.split(":")[1])/60 + float(x.split(":")[2])/3600
        )
        # need to look stop-wise, service_id-wise
        # to see headway at each stop, for each route
        df.sort_values(
            by=["route_id","direction_id","stop_id","service_id","departure_time_num"],
            inplace=True
        )
        df["prior_departure_time_num"] = df["departure_time_num"].shift(1)
        df["prior_departure_time_num"] = np.where(
            (df["route_id"].shift(1) == df["route_id"])
            & (df["stop_id"].shift(1) == df["stop_id"])
            & (df["service_id"].shift(1) == df["service_id"]),
            df["prior_departure_time_num"],
            np.nan
        )
        df["headway"] = df["departure_time_num"] - df["prior_departure_time_num"]
        # define a "day" thing
        df["during_day"] = np.where(
            (df["departure_time_num"] > 7)
            & (df["departure_time_num"] <= 20),
            1,
            np.nan
        )
        df["during_day_headway"] = df["during_day"] * df["headway"]
        df["during_day_trip"] = df["during_day"] * df["trip_id"]
        df["route_short_name"].fillna(df["route_long_name"],inplace=True)
        # group em on up
        gf = df.groupby(
            by=[
                "route_id","route_short_name","route_long_name",
                "direction_id","service_id"
            ],
            as_index=False
        ).agg(
            {
                "headway":"mean",
                "during_day_headway":"mean",
                "trip_id":"nunique",
                "during_day_trip":"nunique",
                "departure_time_num":("min","max")
            }
        )
        return gf

    # want to judge gtfs data by a variety of different metrics
    def summary(
        self,
        route_id = None,
        sample_size: int = 4
    ) -> pd.DataFrame:
        """tbd what I am doing here"""
        # summary by route, with a bunch of different metrics
        # using the "4 metrics that matter" from caltrain-hsr blog
        # doing Origin/Desination pair analysis
        # consider doing just a smattering of destinations
        # note that Clem does a ridership weighted for o/d pairs, but
        # I don't really have access to good ridership data at the moment
        # in the future, I'd like to write something to parse the 
        # pdfs that trimet publishes
        df = self.stop_times

        df = df.merge(
            self.trips[
                ["trip_id","route_id","direction_id","service_id"]
            ],
            on="trip_id"
        )
        if route_id:
            if route_id in self.routes['route_id'].unique():
                df = df[df["route_id"] == route_id]
            else:
                df = df[df["route_id"] == str(route_id)]
            if df.empty:
                logging.critical(
                    f"Route id {route_id} not found in {self.routes['route_id'].unique()}"
                )
                raise ValueError(f"Route id {route_id} not found")


        # determine what service_id is "typical weekday"
        cf = self.calendar_dates
        cf["date"] = pd.to_datetime(cf["date"],format="%Y%m%d")
        cf["day_of_week"] = cf["date"].dt.weekday
        weekdays = cf[
            cf["day_of_week"].isin({1,2,3,4})
        ]
        # reduce to first weekday service, since that should be fine enough
        pd.options.mode.chained_assignment = None
        weekdays["service_type"] = weekdays["service_id"].str[0]
        pd.options.mode.chained_assignment = "warn"
        first_weekday = weekdays.groupby(
            by=["service_type"],
            as_index=False
        )[["date"]].min()
        weekdays = weekdays.merge(
            first_weekday,
            on=["service_type","date"],
            how="inner"
        )
        df = df[df["service_id"].isin(weekdays["service_id"])]

        gf = df.groupby(
            by=["route_id","direction_id","service_id"],
            as_index=False
        ).count()
        gf = gf[["route_id","direction_id","service_id"]]

        df["arrival_time"] = df["arrival_time"].astype(str).apply(
            lambda x: int(x.split(":")[0]) + float(x.split(":")[1])/60 + float(x.split(":")[2])/3600
        )
        df["departure_time"] = df["departure_time"].astype(str).apply(
            lambda x: int(x.split(":")[0]) + float(x.split(":")[1])/60 + float(x.split(":")[2])/3600
        )
        # sort for headway
        df.sort_values(
            by=["route_id","direction_id","stop_id","service_id","departure_time"],
            inplace=True
        )
        df["prior_trip_departure_time"] = df["departure_time"].shift(1)
        df["prior_trip_departure_time"] = np.where(
            (df["route_id"].shift(1) == df["route_id"])
            & (df["stop_id"].shift(1) == df["stop_id"])
            & (df["service_id"].shift(1) == df["service_id"]),
            df["prior_trip_departure_time"],
            np.nan
        )
        df["headway"] = df["departure_time"] - df["prior_trip_departure_time"]

        # sort, get travel time between all stops
        df.sort_values(
            by=["route_id","direction_id","service_id","trip_id","stop_sequence"],
            inplace=True
        )
        df["prior_departure_time"] = df["departure_time"].shift(1)
        df["prior_departure_time"] = np.where(
            (df["route_id"].shift(1) == df["route_id"])
            & (df["direction_id"].shift(1) == df["direction_id"])
            & (df["service_id"].shift(1) == df["service_id"]),
            df["prior_departure_time"],
            np.nan
        )
        df["travel_time"] = df["departure_time"] - df["prior_departure_time"]

    
        # loop over route/directions/service, find o/d pairs
        overall_data = []
        for idx in gf.index:
            d = gf.loc[idx]
            trips = df[
                (df["route_id"] == d["route_id"])
                & (df["service_id"] == d["service_id"])
                & (df["direction_id"] == d["direction_id"])
            ]
            # sample of stop ids - there may be too many, especially on routes like the 20
            l = trips["stop_id"].unique()[::sample_size]
            stops = itertools.combinations(l, 2)
            for stop_pair in stops:
                s1, s2 = stop_pair
                sf1 = trips[trips["stop_id"] == s1]
                sf2 = trips[trips["stop_id"] == s2]
                sf = sf1.merge(
                    sf2,
                    on="trip_id",
                    suffixes=["_1","_2"]
                )
                travel_time = sf["arrival_time_2"] - sf["departure_time_1"]
                if (travel_time < 0).any():
                    # remove the wrong-direction travel ones
                    continue
                
                # metric 1 = Best Trip Time
                # metric 2 = Typical Trip Time
                # metric 3 = Typical Gap Between Vehicles
                # metric 4 = Maximum Gap Between Vehicles
                overall_data.append(
                    {
                        "route_id": d["route_id"],
                        "service_id":d["service_id"],
                        "direction_id":d["direction_id"],
                        "stop_id_1": s1,
                        "stop_id_2":s2,
                        "Best Trip Time":travel_time.min(),
                        "Typical Trip Time":travel_time.mean(),
                        "Typical Headway":sf1["headway"].mean(),
                        "Maximum Headway":sf1["headway"].max()
                    }
                )
        res = pd.DataFrame(overall_data)
        # this is the weighting Clem uses on the caltrain blog, I may revise it later
        # it is just a representation of a typical trip time
        res["travel_time"] = (
            0.7*res["Typical Trip Time"]
            + 0.3*res["Best Trip Time"]
            + 0.2*res["Typical Headway"]
            + 0.1*res["Maximum Headway"]
        ) * 60
        return res
        
    #TODO - rename this function
    def assign_vehicle_id(
        self
    ) -> pd.DataFrame:
        """Assign a vehicle id to determine how many
        total vehicles are needed to run a given route
        note that this is inexact, I am unsure if TriMet 
        does or does not share vehicles between routes on a given
        service pattern"""
        # start with trip id min/max times
        df = self.stop_times
        df = df.merge(
            self.trips[["trip_id","service_id","route_id","direction_id","shape_id"]],
            on="trip_id"
        )
        df["arrival_time_num"] = df["arrival_time"].astype(str).apply(
            lambda x: int(x.split(":")[0]) + float(x.split(":")[1])/60 + float(x.split(":")[2])/3600
        )
        df["departure_time_num"] = df["departure_time"].astype(str).apply(
            lambda x: int(x.split(":")[0]) + float(x.split(":")[1])/60 + float(x.split(":")[2])/3600
        )
        df = df.groupby(
            by=["trip_id","service_id","route_id","direction_id","shape_id"],
            as_index=False
        ).agg(
            {
                "arrival_time_num":"min",
                "departure_time_num":"max"
            }
        )
        df.columns = [
            "trip_id","service_id","route_id","direction_id","shape_id",
            "trip_start_time","trip_end_time"
        ]
        # add shape dist traveled max
        sf = self.shapes.groupby(
            by="shape_id"
        )[["shape_dist_traveled"]].max()
        df = df.merge(
            sf,
            on="shape_id"
        )
        # foot to mile conversion
        df["shape_dist_traveled"] = df["shape_dist_traveled"] / 5280
        df["average_speed"] = df["shape_dist_traveled"] / (
            df["trip_end_time"] - df["trip_start_time"]
        )
        # sort before this!
        df.sort_values(
            by=[
                "route_id","direction_id","service_id",
                "direction_id","service_id"
            ],
            inplace=True
        )
        df["prior_trip_start_time"] = df["trip_start_time"].shift(1)
        df["prior_trip_start_time"] = np.where(
            (df["route_id"].shift(1) == df["route_id"])
            & (df["direction_id"].shift(1) == df["direction_id"])
            & (df["service_id"].shift(1) == df["service_id"]),
            df["prior_trip_start_time"],
            np.nan
        )
        df["headway"] = df["trip_start_time"] - df["prior_trip_start_time"]
        
        """# i still can't think of a better way than a loop
        # sigh
        df["active_vehicles"] = None
        for idx in df.index:
            d = df.loc[idx]
            df.loc[idx, "active_vehicles"] = (
                (df["service_id"] == d["service_id"])
                & (df["route_id"] == d["route_id"])
                & (df["direction_id"] == d["direction_id"])
                & (df["trip_start_time"] < d["trip_end_time"])
                & (df["trip_end_time"] > d["trip_start_time"])
            ).sum()"""

        gf = df.groupby(
            by=["route_id","service_id","direction_id"],
            as_index=False
        ).agg(
            {
                "headway":("min","max","median"),
                "average_speed":("min","max","median")
            }
        )

        return df, gf
        

if __name__ == "__main__":
    tm = GTFS()
    #print(tm.trips)
    #print(tm.routes)
    #tm.route_frequencies()
    #tm.run_times()
    r = tm.summary(route_id=9, sample_size=2)
    print(r)
    print(r[r["Best Trip Time"] < 0])