from pyspark.sql import Window
from pyspark.sql.functions import desc, regexp_replace, col, trim, row_number, percent_rank, sum, round


def spec_logic(df):
    window_rate_by_std = Window.partitionBy("Location").orderBy(desc("Number_of_students"))
    window_prc = Window.partitionBy("Location").orderBy("Number_of_students")
    window_sum_std = Window.partitionBy("Location")

    renamed_df = df.select([col(column).alias(column.replace(' ', '_')) for column in df.columns]) \
        .withColumn('Number_of_students', regexp_replace('Number_of_Studnet', ',', '').cast('int')) \
        .withColumn('University_name', trim(col('University_name'))) \
        .withColumnRenamed('locationLocation', 'Location') \
        .withColumnRenamed('International_Student', 'International_student_prctg') \
        .withColumn("cnt_std_place", row_number().over(window_rate_by_std)) \
        .withColumn("percent_rank_std", round(percent_rank().over(window_prc), 3)) \
        .withColumn("sum_std", sum(col('Number_of_students')).over(window_sum_std)) \
        .select(col('University_name'), col('Location'), col('Number_of_students'),
                col('Number_of_student_per_staffs'), col('International_student_prctg'),
                col('Female_:_male_ratio'), col('sum_std'), col('percent_rank_std'))

    return renamed_df
