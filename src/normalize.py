script_details = ("normalize.py",0.5)

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import min, max, mean, pow, lit, col
import time
import sys
import os
import math

import json
import sys

class ModelBuildReporter(object):

    def __init__(self,sc):
        self.sc = sc
        self.start_time = time.time()
        self.indent = 0

    def report(self,training_record_count,partition_count,predictors,datamodel,target=None,model_type=None,settings=[]):
        end_time = time.time()
        items = []
        if model_type:
            items.append(("Model Type",model_type))
        items += settings
        items.append(("Environment","",[("Spark Version",self.sc.version),("Spark User",self.sc.sparkUser()),("Python",sys.version.replace(os.linesep,"")),("Script",str(script_details))]))
        training_details = [("Records",training_record_count),("Partitions",partition_count),("Elapsed Time (sec)",int(end_time-self.start_time))]
        try:
            applicationId = sc.applicationId
            training_details.append(("Application Id",applicationId))
        except:
            pass
        items.append(("Training Details","",training_details))

        if target:
            items.append(("Target Field",target,DataModelTools.getFieldInformation(datamodel,target)))
        if predictors:
            predictor_list = []
            for predictor in predictors:
                predictor_list.append((predictor,"",DataModelTools.getFieldInformation(datamodel,predictor)))
            items.append(("Predictors",len(predictors),predictor_list))

        s = ""
        s += "Training Summary"+os.linesep
        s += os.linesep
        s += self.format(items)
        s += os.linesep+os.linesep
        return s

    def format(self,items):
        s = ""
        if items:
            keylen = 0
            for item in items:
                key = item[0]
                if len(key) > keylen:
                    keylen = len(key)
            for item in items:
                key = item[0]
                val = item[1]
                s += "    "*self.indent + (key + ":").ljust(keylen+2," ") + str(val) + os.linesep
                if len(item) == 3:
                    self.indent += 1
                    s += self.format(item[2])
                    self.indent -= 1
        return s


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
    suffix = "_NORMALIZED"
    modelpath = "/tmp/DrugDM.json"
    # method = "minmax"
    method = "zscore"
else:
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    df = ascontext.getSparkInputData()
    fields = map(lambda x: x.strip(),"%%fields%%".split(","))
    suffix = '%%suffix%%'
    method = '%%method%%'



datamodel = {}

if method == "minmax":
    ustats = df.select([min(col(f)) for f in fields]+[max(col(f)) for f in fields]).collect()[0]

else:
    ustats = df.select([mean(col(f)) for f in fields]+[mean(pow(col(f),lit(2))) for f in fields]).collect()[0]

mbr = ModelBuildReporter(sc)

build_report = mbr.report(df.count(),df.rdd.getNumPartitions(),
    None,{},None,"data prep",
    settings=[("Algorithm","Normalization",[("method",method),("fields",fields),("suffix",suffix)])])

print(build_report)

model = {}
num_fields = len(fields)
for idx in range(0,num_fields):
    field = fields[idx]
    if method == "minmax":
        model[field] = { "min":ustats[idx],"max":ustats[num_fields+idx] }
    else:
        mean = ustats[idx]
        meansq = ustats[num_fields+idx]
        sd = math.sqrt(meansq - mean*mean)
        model[field] = { "mean":mean, "sd":sd }

modeljson = json.dumps(model)


print("Normalisation Summary")
print("")
if method == "minmax":
    print("%030s %010s %010s"%("Field","Min","Max"))
else:
    print("%030s %010s %010s"%("Field","Mean","Std Deviation"))

for field in fields:
    if method == "minmax":
        print("%030s %010f %010f"%(field,model[field]["min"],model[field]["max"]))
    else:
        print("%030s %010f %010f"%(field,model[field]["mean"],model[field]["sd"]))

if ascontext:
    ascontext.setModelContentFromString("model.metadata",modeljson)
else:
    open(modelpath,"w").write(modeljson)