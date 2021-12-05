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


def prob_to_array(column):
    return np.array([row[0][0] for row in column.collect()])


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
y_total_vote = np.zeros_like(y_true, dtype=np.float)
y_total_prob = np.zeros_like(y_true, dtype=np.float)

for model in models:
    prediction = model.transform(test)
    y_pred = column_to_array(prediction.select("prediction"))
    y_prob = prob_to_array(prediction.select("probability"))
    print_confusion_matrix(y_true, y_pred)
    eval_model(prediction)

    y_total_vote += y_pred
    y_total_prob += y_prob

majority = (NUM_MODELS+1)//2
y_total_vote = np.array([y//majority for y in y_total_vote])
print_confusion_matrix(y_true, y_total_vote)

y_total_prob = np.array([int(y >= 0.5*NUM_MODELS) for y in y_total_prob])
print_confusion_matrix(y_true, y_total_prob)

returns = column_to_array(df.select("_c1"))
vote_returns = y_total_vote*returns
prob_returns = y_total_prob*returns
print(f"Total returns for vote: {vote_returns.sum()}")
print(f"Total returns for probability: {prob_returns.sum()}")
