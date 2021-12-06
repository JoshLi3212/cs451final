import os
import shutil

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier, FMClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

SEED = 451
INPUTS = 9
NUM_MODELS = 6
FOLDS = 5

MAX_ITERS = [10, 100, 200]
REG_PARAMS = [0, 0.1, 0.2]
ELASTIC_NET_PARAMS = [0, 0.5, 1]

MAX_DEPTHS = [5, 10, 20]
IMPURITIES = ["gini", "entropy"]

STEP_SIZES = [0.001, 0.01, 0.05, 0.1]

def cross_validate_save(model, paramGrid, model_path):
    crossval = CrossValidator(
        estimator=model,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(metricName="areaUnderPR"),
        numFolds=FOLDS,
        seed=SEED
    )
    model = crossval.fit(train)
    model.save(model_path)

conf = SparkConf()
conf.set('spark.logConf', 'true')

spark = SparkSession.builder.config(conf=conf).appName("Train models").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("train.csv", inferSchema=True)
num_columns = len(df.columns)

df = df.withColumnRenamed("_c0", "label")
assembler = VectorAssembler(inputCols = [f"_c{n}" for n in range(num_columns - INPUTS, num_columns)], outputCol="features")
df = assembler.transform(df)
train = df.select("label", "features")

model_path = "models/logistic_regression"
if not os.path.exists(model_path):
    lr = LogisticRegression()
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.maxIter, MAX_ITERS) \
        .addGrid(lr.regParam, REG_PARAMS) \
        .addGrid(lr.elasticNetParam, ELASTIC_NET_PARAMS) \
        .build()
    cross_validate_save(lr, paramGrid, model_path)
    print("Logistic regression model saved")
else:
    print("Logistic regression model already exists")

model_path = "models/decision_tree"
if not os.path.exists(model_path):
    dt = DecisionTreeClassifier(seed=SEED)
    paramGrid = ParamGridBuilder() \
        .addGrid(dt.maxDepth, MAX_DEPTHS) \
        .addGrid(dt.impurity, IMPURITIES) \
        .build()
    cross_validate_save(dt, paramGrid, model_path)
    print("Decision tree model saved")
else:
    print("Decision tree model already exists")

model_path = "models/random_forest"
if not os.path.exists(model_path):
    rf = RandomForestClassifier(seed=SEED)
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.maxDepth, MAX_DEPTHS) \
        .addGrid(rf.impurity, IMPURITIES) \
        .build()
    cross_validate_save(rf, paramGrid, model_path)
    print("Random forest model saved")
else:
    print("Random forest model already exists")

model_path = "models/gradient_boosted_trees"
if not os.path.exists(model_path):
    gbt = GBTClassifier(seed=SEED)
    paramGrid = ParamGridBuilder() \
        .addGrid(gbt.maxIter, MAX_ITERS) \
        .addGrid(gbt.stepSize, STEP_SIZES) \
        .build()
    cross_validate_save(gbt, paramGrid, model_path)
    print("Gradient boosted trees model saved")
else:
    print("Gradient boosted trees model already exists")

model_path = "models/multilayer_perceptron"
if not os.path.exists(model_path):
    mlp = MultilayerPerceptronClassifier(layers=[INPUTS, 10, 20, 20, 10, 2], seed=SEED)
    paramGrid = ParamGridBuilder() \
        .addGrid(mlp.maxIter, MAX_ITERS + [500]) \
        .addGrid(mlp.stepSize, STEP_SIZES) \
        .build()
    cross_validate_save(mlp, paramGrid, model_path)
    print("Multilayer perceptron model saved")
else:
    print("Multilayer perceptron model already exists")

model_path = "models/factorization_machine"
if not os.path.exists(model_path):
    fmc = FMClassifier(seed=SEED)
    paramGrid = ParamGridBuilder() \
        .addGrid(fmc.regParam, REG_PARAMS) \
        .build()
    cross_validate_save(fmc, paramGrid, model_path)
    print("Factorization machine model saved")
else:
    print("Factorization machine model already exists")