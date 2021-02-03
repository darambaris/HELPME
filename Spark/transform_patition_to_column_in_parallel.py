# This context was applied to transform the extraction date (yyyy/mm/dd) to extracted_at column (yyyy-mm-dd 00:00:00)
# Example: s3://bucket/table_name/2021/02/03 -> df with extracted_at column = 2021-02-03 00:00:00
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    substring,
    regexp_replace,
    input_file_name,  # returns a string column for the file
    unix_timestamp,
    unix_timestamp,
    from_unixtime,
)

from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool as Pool

YEAR = ["2021", "2020", "2019"]
MONTH = [str(x).rjust(2, "0") for x in range(1, 3)]
DAY = [str(x).rjust(2, "0") for x in range(1, 32)]
S3_PATH = "__YOUR_S3_PATH__"


def fill_days(year: str, month: str, day: str) -> None:
    S3_FULL_PATH = f"{S3_PATH}/{year}/{month}/{day}"
    try:
        # read all parquet files from specific yyyy/mm/dd
        df = spark.read.format("parquet").load(f"{S3_FULL_PATH}/*.parquet")

        # creates extracted_at column from date path
        df = df.withColumn(
            "extracted_at",
            from_unixtime(  # covert unixtime to timestamp
                unix_timestamp(
                    substring(
                        regexp_replace(input_file_name(), f"{S3_PATH}/", ""), 1, 10
                    ),
                    "yyyy/MM/dd",
                )
            ),
        )
        # overwrite dataframe with the new column
        df.write.mode("overwrite").parquet(f"{S3_FULL_PATH}/")
        print(f"files updated from {S3_FULL_PATH}/")

    # some dates don't have paths.
    except AnalysisException as err:
        print(err)


if __name__ == "__main__":
    N = cpu_count()
    for y in YEAR:
        for m in MONTH:
            # uses partial for functions with multiple params
            func = partial(fill_days, y, m)
            # parallelize the days
            with Pool(processes=N) as p:
                results = p.map(func, DAY)
