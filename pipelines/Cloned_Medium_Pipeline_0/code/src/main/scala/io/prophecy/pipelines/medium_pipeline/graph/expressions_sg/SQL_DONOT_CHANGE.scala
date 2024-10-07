package io.prophecy.pipelines.medium_pipeline.graph.expressions_sg

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.medium_pipeline.udfs.UDFs._
import io.prophecy.pipelines.medium_pipeline.udfs.Rules._
import io.prophecy.pipelines.medium_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.medium_pipeline.graph.expressions_sg.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SQL_DONOT_CHANGE {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    in0.createOrReplaceTempView("in0")
    context.spark.sql(
      """SELECT cast(any(col1) FILTER (WHERE col2 = 1) as string) as c1 FROM VALUES (false, 1), (false, 2), (true, 2), (NULL, 1) AS tab(col1, col2)
UNION
SELECT cast(any(col) as string) as c1 FROM VALUES (true), (false), (false) AS tab(col)
UNION
SELECT cast(approx_count_distinct(col1) as string) as c1 FROM VALUES (1), (1), (2), (2), (3) tab(col1)
UNION
SELECT cast(approx_count_distinct(col1) FILTER(WHERE col2 = 10) as string) as c1 FROM VALUES (1, 10), (1, 10), (2, 10), (2, 10), (3, 10), (1, 12) AS tab(col1, col2)
UNION
SELECT cast(approx_percentile(col, array(0.5, 0.4, 0.1), 100) as string) as c1 FROM VALUES (0), (1), (2), (10) AS tab(col)
UNION
SELECT cast(approx_percentile(DISTINCT col, 0.5, 100) as string) as c1 FROM VALUES (0), (6), (6), (7), (9), (10) AS tab(col)
UNION
SELECT cast(array_agg(col) as string) as c1 FROM VALUES (1), (2), (NULL), (1) AS tab(col)
UNION
SELECT cast(array_agg(DISTINCT col) as string) as c1 FROM VALUES (1), (2), (NULL), (1) AS tab(col)
UNION
SELECT cast(avg(col) as string) as c1 FROM VALUES (1), (2), (3) AS tab(col)
UNION
SELECT cast(try_avg(col) as string) as c1 FROM VALUES (10), (20) AS tab(col)
UNION
SELECT cast(bit_and(col) as string) as c1 FROM VALUES (3), (5) AS tab(col)
UNION
SELECT cast(bit_and(col) FILTER(WHERE col < 6) as string) as c1 FROM VALUES (3), (5), (6) AS tab(col)
UNION
SELECT cast(bit_or(col) as string) as c1 FROM VALUES (3), (5) AS tab(col)
UNION
SELECT cast(bit_or(col) FILTER(WHERE col < 8) as string) as c1 FROM VALUES (3), (5), (8) AS tab(col)
UNION
SELECT cast(bit_xor(col) as string) as c1 FROM VALUES (3), (3), (5) AS tab(col)
UNION
SELECT cast(bool_and(col) as string) as c1 FROM VALUES (true), (true), (true) AS tab(col)
UNION
SELECT cast(bool_or(col) as string) as c1 FROM VALUES (true), (false), (false) AS tab(col)
UNION
SELECT cast(collect_list(col) as string) as c1 FROM VALUES (1), (2), (NULL), (1) AS tab(col)
UNION
SELECT cast(collect_set(col) as string) as c1 FROM VALUES (1), (2), (NULL), (1) AS tab(col)
UNION
SELECT cast(corr(c1, c2) as string) as c1 FROM VALUES (3, 2), (3, 3), (3, 3), (6, 4) as tab(c1, c2)
UNION
SELECT cast(corr(DISTINCT c1, c2) FILTER(WHERE c1 != c2) as string) as c1 FROM VALUES (3, 2), (3, 3), (3, 3), (6, 4) as tab(c1, c2)
UNION
SELECT cast(count(*) as string) as c1 FROM VALUES (NULL), (5), (5), (20) AS tab(col)
UNION
SELECT cast(count(*) as string) as c1 FROM VALUES (NULL), (5), (5), (20) AS tab(col)
UNION
SELECT cast(count(col) FILTER(WHERE col < 10) as string) as c1 FROM VALUES (NULL), (5), (5), (20) AS tab(col)
UNION
SELECT cast(count_if(col % 2 = 0) as string) as c1 FROM VALUES (NULL), (0), (1), (2), (2), (3) AS tab(col)
UNION
SELECT cast(covar_pop(c1, c2) as string) as c1 FROM VALUES (1,1), (2,2), (2,2), (3,3) AS tab(c1, c2)
UNION
SELECT cast(covar_samp(c1, c2) as string) as c1 FROM VALUES (1,1), (2,2), (2, 2), (3,3) AS tab(c1, c2)
UNION
SELECT cast(every(col) as string) as c1 FROM VALUES (true), (true), (true) AS tab(col)
UNION
SELECT cast(first(col, true) as string) as c1 FROM VALUES (NULL), (5), (20) AS tab(col)
UNION
SELECT cast(first_value(col) as string) as c1 FROM VALUES (10), (5), (20) AS tab(col)
UNION
SELECT cast(kurtosis(col) as string) as c1 FROM VALUES (-10), (-20), (100), (100), (1000) AS tab(col)
UNION
SELECT cast(last(col) as string) as c1 FROM VALUES (10), (5), (20) AS tab(col)
UNION
SELECT cast(last_value(col) as string) as c1 FROM VALUES (10), (5), (20) AS tab(col)
UNION
SELECT cast(max(col) as string) as c1 FROM VALUES (10), (50), (20) AS tab(col)
UNION
SELECT cast(max_by(x, y) as string) as c1 FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y)
UNION
SELECT cast(mean(DISTINCT col) as string) as c1 FROM VALUES (1), (1), (2), (NULL) AS tab(col)
UNION
SELECT cast(min(col) as string) as c1 FROM VALUES (10), (50), (20) AS tab(col)
UNION
SELECT cast(min_by(x, y) as string) as c1 FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y)
UNION
SELECT cast(percentile(col, 0.3) as string) as c1 FROM VALUES (0), (10), (10) AS tab(col)
UNION
SELECT cast(percentile_approx(col, 0.5, 100) as string) as c1 FROM VALUES (0), (6), (7), (9), (10), (10), (10) AS tab(col)
UNION
SELECT cast(percentile_cont(array(0.5, 0.4, 0.1)) WITHIN GROUP (ORDER BY col) as string) as c1 FROM VALUES (0), (1), (2), (10) AS tab(col)
UNION
SELECT cast(percentile_disc(array(0.5, 0.4, 0.1)) WITHIN GROUP (ORDER BY col) as string) as c1 FROM VALUES (0), (1), (2), (10) AS tab(col)
UNION
SELECT cast(regr_avgx(y, x) as string) as c1 FROM VALUES (1, 2), (2, 3), (2, 3), (null, 4), (4, null) AS T(y, x)
UNION
SELECT cast(regr_avgy(y, x) as string) as c1 FROM VALUES (1, 2), (2, 3), (2, 3), (null, 4), (4, null) AS T(y, x)
UNION
SELECT cast(regr_count(y, x) as string) as c1 FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS t(y, x)
UNION
SELECT cast(regr_r2(y, x) as string) as c1 FROM VALUES (1, 2), (2, 3), (2, 3), (null, 4), (4, null) AS T(y, x)
UNION
SELECT cast(skewness(col) as string) as c1 FROM VALUES (-10), (-20), (100), (1000), (1000) AS tab(col)
UNION
SELECT cast(some(col) as string) as c1 FROM VALUES (true), (false), (false) AS tab(col)
UNION
SELECT cast(std(col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(stddev(col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(stddev_pop(DISTINCT col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(stddev_samp(DISTINCT col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(sum(col) as string) as c1 FROM VALUES (NULL), (NULL) AS tab(col)
UNION
SELECT cast(try_avg(DISTINCT col) as string) as c1 FROM VALUES (1), (1), (2) AS tab(col)
UNION
SELECT cast(try_sum(col) as string) as c1 FROM VALUES (NULL), (10), (15) AS tab(col)
UNION
SELECT cast(var_pop(col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(var_samp(col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(variance(col) as string) as c1 FROM VALUES (1), (2), (3), (3) AS tab(col)
UNION
SELECT cast(a as string) as c1 FROM (SELECT a,b,dense_rank() OVER(PARTITION BY a ORDER BY b),rank() OVER(PARTITION BY a ORDER BY b),row_number() OVER(PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, b, ntile(2) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, b, percent_rank(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A1', 3), ('A1', 6), ('A1', 7), ('A1', 7), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a,b,dense_rank() OVER(PARTITION BY a ORDER BY b),rank() OVER(PARTITION BY a ORDER BY b),row_number() OVER(PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, b, cume_dist() OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, b, lag(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, b, lead(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, b, nth_value(b, 2) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b))
UNION
SELECT cast(num as string) as c1 FROM (SELECT explode(map(1, 'a', 2, 'b')) AS (num, val), 'Spark')
UNION
SELECT cast(elem as string) as c1 FROM (SELECT explode_outer(array(10, 20)) AS elem, 'Spark')
UNION
SELECT cast(a as string) as c1 FROM (SELECT a, session_window.start, session_window.end, count(*) as cnt FROM VALUES ('A1', '2021-01-01 00:00:00'),('A1', '2021-01-01 00:04:30'),('A1', '2021-01-01 00:10:00'),('A2', '2021-01-01 00:01:00'),('A2', '2021-01-01 00:04:30') AS tab(a, b) GROUP by a, session_window(b, CASE WHEN a = 'A1' THEN '5 minutes' WHEN a = 'A2' THEN '1 minute' ELSE '10 minutes' END) ORDER BY a, start)
UNION
SELECT cast(window as string) as c1 FROM (SELECT window, min(val), max(val), count(val) FROM VALUES (TIMESTAMP'2020-08-01 12:20:21', 17),(TIMESTAMP'2020-08-01 12:20:22', 12),(TIMESTAMP'2020-08-01 12:23:10',  8),(TIMESTAMP'2020-08-01 12:25:05', 11),(TIMESTAMP'2020-08-01 12:28:59', 15),(TIMESTAMP'2020-08-01 12:30:01', 23),(TIMESTAMP'2020-08-01 12:30:15',  2),(TIMESTAMP'2020-08-01 12:35:22', 16) AS S(stamp, val) GROUP BY window(stamp, '2 MINUTES 30 SECONDS', '30 SECONDS', '15 SECONDS'))
UNION
SELECT cast(name as string) as c1 FROM (SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY cube(name, age))
UNION
SELECT cast(name as string) as c1 FROM (SELECT name, grouping(name), sum(age) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY cube(name))
UNION
SELECT cast(name as string) as c1 FROM (SELECT name, age, grouping_id(name, age),conv(cast(grouping_id(name, age) AS STRING), 10, 2),avg(height) FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height) GROUP BY cube(name, age))
UNION
SELECT cast(col1 as string) as c1 FROM (SELECT spark_partition_id() as col1, t.* FROM range(0, -5, -1, 2) AS t)
UNION
SELECT cast(col1 as string) as c1 FROM (SELECT spark_partition_id() as col1, t.* FROM range(5) AS t)
UNION
SELECT cast(col1 as string) as c1 FROM (SELECT 'hello' as col1, stack(2, 1, 2, 3) AS (first, second), 'world')
UNION
SELECT cast(window as string) as c1 FROM (SELECT window, min(val), max(val), count(val) FROM VALUES (TIMESTAMP'2020-08-01 12:20:21', 17),(TIMESTAMP'2020-08-01 12:20:22', 12),(TIMESTAMP'2020-08-01 12:23:10',  8),(TIMESTAMP'2020-08-01 12:25:05', 11),(TIMESTAMP'2020-08-01 12:28:59', 15),(TIMESTAMP'2020-08-01 12:30:01', 23),(TIMESTAMP'2020-08-01 12:30:15',  2),(TIMESTAMP'2020-08-01 12:35:22', 16) AS S(stamp, val) GROUP BY window(stamp, '2 MINUTES 30 SECONDS', '30 SECONDS', '15 SECONDS'))"""
    )
  }

}
