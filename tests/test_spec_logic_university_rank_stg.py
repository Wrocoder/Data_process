import pytest
from pyspark.sql import SparkSession

from helper.spec.univers_rank_spec import spec_logic


@pytest.fixture
def spark():
    spark = SparkSession.builder \
        .appName("test_spec_logic") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    # yield spark
    # spark.stop()
    return spark


@pytest.fixture
def input_df(spark):
    data = [
        ("University A", "Location 1", "1,000", 10, 50, "1:1"),
        ("University B", "Location 2", "2,000", 20, 40, "2:1"),
        ("University C", "Location 1", "3,000", 30, 60, "3:1"),
    ]

    columns = ["University_name", "Location", "Number_of_Studnet",
               "Number_of_student_per_staffs", "International_Student",
               "Female_:_male_ratio"]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def expected_df(spark):
    data = [
        ("University A", "Location 1", 1000, 10, 50, "1:1", 4000, 0.0),
        ("University C", "Location 1", 3000, 30, 60, "3:1", 4000, 1.0),
        ("University B", "Location 2", 2000, 20, 40, "2:1", 2000, 0.0),
    ]

    columns = ["University_name", "Location", "Number_of_students",
               "Number_of_student_per_staffs", "International_student_prctg",
               "Female_:_male_ratio", "sum_std", "percent_rank_std"]

    return spark.createDataFrame(data, columns)


def test_spec_logic(input_df, expected_df):
    result_df = spec_logic(input_df)
    unnecessary_cols = ['today_ts', 'today_date']
    assert result_df.drop(*unnecessary_cols).collect() == expected_df.collect()
