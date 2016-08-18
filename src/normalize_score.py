
script_details = ("normalize_score.py",0.5)

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import time
import sys
import os

import json
ascontext=None
if len(sys.argv) > 1 and sys.argv[1] == "-test":
    import os
    sc = SparkContext('local')
    sqlCtx = SQLContext(sc)
    # get an input dataframe with sample data by looking in working directory for file DRUG1N.json
    wd = os.getcwd()
    df = sqlCtx.load("file://"+wd+"/DRUG1N.json","json").repartition(4)
    # specify predictors and target
    fields = ["Na", "K", "Age"]
    suffix = "_NORM"
    modelpath = "/tmp/DrugDM.json"
else:
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    df = ascontext.getSparkInputData()
    fields = map(lambda x: x.strip(),"%%fields%%".split(","))
    suffix = '%%suffix%%'

from pyspark.sql.types import StructField, StructType, DoubleType
from pyspark.sql.functions import lit
output_schema = StructType(df.schema.fields + [StructField(field+suffix, DoubleType(), nullable=True) for field in fields])

if ascontext:
    if ascontext.isComputeDataModelOnly():
        ascontext.setSparkOutputSchema(output_schema)
        sys.exit(0)
    else:
        model = json.loads(ascontext.getModelContentToString("model.metadata"))
else:
    model = json.loads(open(modelpath,"r").read())


outdf = df
for field in fields:
    if field in model:
        norm_function = lambda x: lit(None).cast(DoubleType())
        dm_field = model[field]
        if "max" in dm_field:
            max = dm_field["max"]
            min = dm_field["min"]
            if max > min:
                norm_function = lambda x: (x-min)/(max-min)
        if "mean" in dm_field:
            mean = dm_field["mean"]
            sd = dm_field["sd"]
            norm_function = lambda x: (x-mean)/sd
    outdf = outdf.withColumn(field+suffix,norm_function(outdf[field]))
if ascontext:
    ascontext.setSparkOutputData(outdf)
else:
    print(outdf.take(10))