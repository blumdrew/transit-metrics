"""
    Date: 2023-02-27
    Purpose: TriMet ridership parser
    Author: Andrew Lindstrom
"""

import os
import re
import logging

import PyPDF2
import pandas as pd

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

def parse_data(
    file_path
) -> pd.DataFrame:
    """Parse all table data out of input pdf"""
    pdf = PyPDF2.PdfReader(file_path)
    cols = [
        "monthly_lifts","total_boardings",
        "offs","ons","stop_id"
    ]
    all_data = []
    logging.info("Starting extraction...")
    for page in pdf.pages:
        text = page.extract_text()
        for line in text.split('\n'):
            if not re.findall(r"\|", line):
                continue
            line_rev = line[::-1]
            data = [
                int(line_rev[a.span()[0]:a.span()[1]][::-1]) for a in re.finditer(r"(\d)+", line_rev)
            ]
            data = data[:5]
            all_data.append(data)

    df = pd.DataFrame(
        data=all_data,columns=cols
    )
    df = df[list(reversed(cols))]
    return df


def main():
    """Process driver"""

    list_of_pdfs = [
        os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH) if f.endswith(".pdf")
    ]
    data = []
    for file_path in list_of_pdfs:
        fname = os.path.basename(file_path)
        date = pd.to_datetime(
            fname.split(".")[0][-10:],
            format="%Y_%m_%d"
        )
        df = parse_data(file_path=file_path)
        df["date"] = date
        data.append(df)

    df = pd.concat(data)
    df.to_csv(
        os.path.join(
            DATA_PATH,
            "stop_level_ridership_data.csv"
        ),
        index=False
    )

if __name__ == "__main__":
    main()