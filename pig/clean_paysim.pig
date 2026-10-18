-- FraudLens Part 1: PaySim data cleaning with Apache Pig.
--
-- Required parameters (supplied by scripts/run_pig.sh from config/fraudlens.env):
--   RAW_PATH  raw PaySim CSV in HDFS
--   OUT_PATH  output directory for the cleaned data (must not exist)
-- Optional parameters (defaults suit a small single-node VM; run_pig.sh derives them from the config):
--   MAP_MB / REDUCE_MB / AM_MB        YARN container sizes in MB
--   MAP_HEAP_MB / REDUCE_HEAP_MB / AM_HEAP_MB   JVM heap inside each container (about 80 percent)
--   PARALLEL                          number of reducers
--
-- Manual run (always in MapReduce mode against HDFS):
--   pig -x mapreduce -param RAW_PATH=hdfs:///fraudlens/raw/<file>.csv \
--       -param OUT_PATH=hdfs:///fraudlens/processed/paysim_cleaned \
--       -f pig/clean_paysim.pig

%default MAP_MB 1024;
%default REDUCE_MB 1024;
%default AM_MB 1024;
%default MAP_HEAP_MB 820;
%default REDUCE_HEAP_MB 820;
%default AM_HEAP_MB 820;
%default PARALLEL 2;

-- Small containers so the job fits on a low-memory VM and does not stall waiting for YARN memory.
SET job.name 'FraudLens-Pig-Clean';
SET default_parallel $PARALLEL;
SET mapreduce.map.memory.mb $MAP_MB;
SET mapreduce.map.java.opts '-Xmx${MAP_HEAP_MB}m';
SET mapreduce.reduce.memory.mb $REDUCE_MB;
SET mapreduce.reduce.java.opts '-Xmx${REDUCE_HEAP_MB}m';
SET yarn.app.mapreduce.am.resource.mb $AM_MB;
SET yarn.app.mapreduce.am.command-opts '-Xmx${AM_HEAP_MB}m';

raw_data = LOAD '$RAW_PATH'
USING PigStorage(',')
AS (
    step:chararray,
    type:chararray,
    amount:chararray,
    nameOrig:chararray,
    oldbalanceOrg:chararray,
    newbalanceOrig:chararray,
    nameDest:chararray,
    oldbalanceDest:chararray,
    newbalanceDest:chararray,
    isFraud:chararray,
    isFlaggedFraud:chararray
);

-- Remove the header and records with invalid numeric values.
-- The numeric pattern also accepts scientific notation (for example 1.0E7):
-- large amounts and balances are written that way in the raw file, and the
-- previous pattern silently dropped every row containing one of them.
valid_data = FILTER raw_data BY
    step matches '[0-9]+' AND
    amount matches '[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?' AND
    oldbalanceOrg matches '[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?' AND
    newbalanceOrig matches '[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?' AND
    oldbalanceDest matches '[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?' AND
    newbalanceDest matches '[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?' AND
    isFraud matches '[01]' AND
    isFlaggedFraud matches '[01]' AND
    type IS NOT NULL AND
    nameOrig IS NOT NULL AND
    nameDest IS NOT NULL;

-- Convert numeric strings and derive balance-change features.
enriched_data = FOREACH valid_data GENERATE
    (int)step AS step,
    type AS type,
    (double)amount AS amount,
    nameOrig AS nameOrig,
    (double)oldbalanceOrg AS oldbalanceOrg,
    (double)newbalanceOrig AS newbalanceOrig,
    ((double)newbalanceOrig - (double)oldbalanceOrg) AS originBalanceChange,
    nameDest AS nameDest,
    (double)oldbalanceDest AS oldbalanceDest,
    (double)newbalanceDest AS newbalanceDest,
    ((double)newbalanceDest - (double)oldbalanceDest) AS destBalanceChange,
    (int)isFraud AS isFraud,
    (int)isFlaggedFraud AS isFlaggedFraud;

-- Remove exact duplicate records.
clean_data = DISTINCT enriched_data;

-- Store cleaned data in HDFS (no header, comma separated, 13 columns).
STORE clean_data INTO '$OUT_PATH' USING PigStorage(',');
