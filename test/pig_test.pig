-- Smoke test for the Pig installation. Run from the repository root:
--   pig -x local test/pig_test.pig
data = LOAD 'test/sample.csv'
USING PigStorage(',')
AS (step:int, type:chararray, amount:double);

DUMP data;
