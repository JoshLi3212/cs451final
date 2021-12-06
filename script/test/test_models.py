from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidatorModel

from sklearn.metrics import confusion_matrix
import numpy as np

INPUTS = 9
MODELS = [
    "logistic_regression",
    "decision_tree",
    "random_forest",
    "gradient_boosted_trees",
    "multilayer_perceptron",
    "factorization_machine"
]
NUM_MODELS = len(MODELS)


def column_to_array(column):
    return np.array([row[0] for row in column.collect()])


def print_confusion_matrix(y_true, y_pred):
    cm = confusion_matrix(y_true, y_pred)
    print(cm)
    print(f"Accuracy: {(cm[0][0]+cm[1][1])/cm.sum()}"
        f", Precision: {cm[1][1]/(cm[1][1]+cm[0][1])}"
        f", Recall: {cm[1][1]/(cm[1][1]+cm[1][0])}")


def eval_model(prediction):
    evaluator = BinaryClassificationEvaluator()
    area_under_ROC = evaluator.evaluate(prediction)
    area_under_PR = evaluator.evaluate(prediction, {evaluator.metricName: "areaUnderPR"})
    print(f"Area under ROC: {area_under_ROC}, Area under PR: {area_under_PR}")


conf = SparkConf()
conf.set('spark.logConf', 'true')

spark = SparkSession.builder.config(conf=conf).appName("Train models").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("test.csv", inferSchema=True)
num_columns = len(df.columns)

df = df.withColumnRenamed("_c0", "label")
assembler = VectorAssembler(inputCols = [f"_c{n}" for n in range(num_columns - INPUTS, num_columns)], outputCol="features")
df = assembler.transform(df)
test = df.select("label", "features")

models = [CrossValidatorModel.load(f"models/{model}") for model in MODELS]

y_true = column_to_array(test.select("label"))
returns = column_to_array(df.select("_c1"))

y_total_vote = np.zeros_like(y_true, dtype=np.float)
y_total_prob = np.zeros_like(y_true, dtype=np.float)

for model in models:
    prediction = model.transform(test)
    y_pred = column_to_array(prediction.select("prediction"))
    y_total_vote += y_pred

    vote_returns = y_pred*returns
    print(f"Total returns for vote: {vote_returns.sum()}")

majority = NUM_MODELS/2
y_total_vote = np.array([int(y >= majority) for y in y_total_vote])
print_confusion_matrix(y_true, y_total_vote)

vote_returns = y_total_vote*returns
print(f"Total returns for vote: {vote_returns.sum()}")


# Output returns graph

import matplotlib.pyplot as plt
from datetime import datetime, timedelta

y = [1+x for x in vote_returns]
for i in range(1, len(y)):
    y[i] *= y[i-1]
base_date = datetime(year=2021, month=11, day=1)
x = [base_date + timedelta(hours=i) for i in range(len(y))]
plt.plot(x, y)
plt.xlabel("Time")
plt.ylabel("Returns")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("returns.pdf")

y = []
with open("test.csv", "r") as f:
    for line in f:
        label, ret, rest = line.split(",", 2)
        y.append(1 + float(ret))

for i in range(1, len(y)):
    y[i] *= y[i-1]
plt.plot(x, y)
plt.savefig("comparison.pdf")
