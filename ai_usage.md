# AI Usage Log

**Arpita Debnath | MT25018**

**Total review time: 330 minutes (5.5h)**

---

## A1


AI tool/model: Claude (Sonnet 5)

Prompt (verbatim):
I need help understanding my BDA asssignment. I am new to these technologies and i need help in undretsanding in details about spark,duckdb and mapreduce.help me understand by strting from task-1 and hwo to implement these technologies.

Did the response contain code? Yes

Code used: Partial

Review time: 90 minutes

What I checked: Went through the mapper/reducer logic, the Spark scripts, and the SQL files, then ran each one on real data to make sure nothing broke.

Changes I made:  Modified the coding logic and confirmed by re-running and comparing output.

How I verified it: Ran `scripts/task1_verify.py`, which checks all three systems against each other. Came back PASS on all 256 groups.

---

## A2

AI tool/model: Claude (Sonnet 5)

Prompt (verbatim):
What does Task-2 say.I need you to help me understand taks-2 in details.Why. do we have to do warmup and 3 runs for both of them?

Did the response contain code? Yes

Code used: Partial

Review time: 30 minutes

What I checked: Ran it, saw the warm-up + 3 runs print correctly for all three systems.

Changes I made: None here directly 
How I verified it: Read the terminal output myself.

---

## A3

AI tool/model: Claude (Sonnet 5)
Prompt (verbatim):
can you help me understand Task 2 ? using Spark and DuckDB on taxi 2m parquet file. do query A with all columns and query B with only the four required columns. add. the warmup and 3 timing runs save the explain plans  Also explain what is the column pruning and filter pushdown part.

Did the response contain code? Yes

Code used: Partial

Review time: 60 minutes

What I checked: Opened both explain files. Query B lists 4 columns, Query A lists all 20, filter shows up at the scan step in both.

Changes I made: Trimmed the docstring later.

How I verified it: Read the actual explain files and the printed timings.

---

## A4

AI tool/model: Claude (Sonnet 5)
Can you help me understand Task 3 according to the assignment?  

Did the response contain code? Yes

Code used: Partial

Review time: 45 minutes

What I checked: Compared both systems' top-5 zones matched. Checked the plan for `BroadcastHashJoin`.

Changes I made: removed an extra debug output file later.

How I verified it: Compared the printed results and read the plan myself.

---

## A5

AI tool/model: Claude (Sonnet 5)

Prompt (verbatim):
In task-5,I need to reuse one SparkSession across all so the timings to reflect actual query execution rather than repeated startup costs.


Did the response contain code? Yes

Code used: Partial

Review time: 35 minutes

What I checked: Reran the timing after the fix.Spark dropped from ~5s to under 0.3s, matched what Task 5 already showed.

Changes I made: made some code changes and reviewed it to output the cached and uncached scenarios in the output

How I verified it: Before and after comparison of real numbers.

---

## A6

AI tool/model: Claude (Sonnet 5)

Prompt (verbatim):
all is fine till now. Now I want you to strictly go through the assignment again and verify every line from the assignment requirements . Check if i am missing any requiremment tell me which requirement i missed.

Did the response contain code? Yes

Code used: Partial

Review time: 30 minutes

What I checked: Rebuilt `timings.csv` and counted 79 rows, no duplicates.

Changes I made: modified the .py files and rechecked scripts and ran the outputs again and fixed errors faced while running pyspark

How I verified it: Ran every timing script again myself and checked the CSV by hand.

---

## A7

AI tool/model: Claude (Sonnet 5)

Prompt (verbatim):
can you help me understand how to do Task 4 as per the assignment? in this i need to run the Task 1 aggregation on the 100k, 500k, 1M and 2M csv files using mapreduce, spark and duckdb and add 1 warmup and 3 measured runs  also handle the 900-second timeout properly. 

Did the response contain code? Yes

Code used: Partial

Review time: 40 minutes

What I checked: Ran the scaling script across all four dataset sizes and confirmed 36 rows landed in `timings.csv`, and checked the generated plot against the raw numbers.

Changes I made: Asked for a few plot tweaks and made few modifications for generating correct timing data.

How I verified it: Looked at the plot and the CSV side by side and cross-checked a couple of medians.

---