from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier, FMClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from sklearn.metrics import confusion_matrix

def spark_setup():
    conf = SparkConf()
    conf.set('spark.logConf', 'true')

    spark = SparkSession.builder.config(conf=conf).appName("Train models").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_train_validate(spark, file):
    spark = spark_setup()

    df = spark.read.csv(file, inferSchema=True)
    df = df.withColumnRenamed("_c0", "label")
    assembler = VectorAssembler(inputCols = [f"_c{n}" for n in range(1, 10)], outputCol="features")
    df = assembler.transform(df)
    df = df.select("label", "features")
    return df.randomSplit([0.8, 0.2], seed=451)

y_total = None
def eval_model(model, validate):
    global y_total
    prediction = model.transform(validate)
    y_predict = prediction.select("prediction").collect()
    if y_total is None:
        y_total = [y[0] for y in y_predict]
    else:
        for i, y in enumerate(y_predict):
            y_total[i] += y[0]

    cm = confusion_matrix(y_true, y_predict)
    print(cm)
    print(f"Accuracy: {(cm[0][0]+cm[1][1])/cm.sum()}"
        f", Precision: {cm[1][1]/(cm[1][1]+cm[0][1])}"
        f", Recall: {cm[1][1]/(cm[1][1]+cm[1][0])}")
    print()


spark = spark_setup()
train, validate = get_train_validate(spark, "train.csv")
y_true = validate.select("label").collect()

lr = LogisticRegression(maxIter=100)
lr_model = lr.fit(train)
eval_model(lr_model, validate)

rf = RandomForestClassifier(maxDepth=30, seed=451)
rf_model = rf.fit(train)
eval_model(rf_model, validate)

gbt = GBTClassifier(maxIter=100, seed=451)
gbt_model = gbt.fit(train)
eval_model(gbt_model, validate)

mlp = MultilayerPerceptronClassifier(layers=[9, 10, 20, 10, 2], maxIter=100, seed=451)
mlp_model = mlp.fit(train)
eval_model(mlp_model, validate)

fmc = FMClassifier(seed=451)
fmc_model = fmc.fit(train)
eval_model(fmc_model, validate)

y_true = [y[0] for y in y_true]
y_total = [y//3 for y in y_total]

cm = confusion_matrix(y_true, y_total)
print(cm)
print(f"Accuracy: {(cm[0][0]+cm[1][1])/cm.sum()}"
      f", Precision: {cm[1][1]/(cm[1][1]+cm[0][1])}"
      f", Recall: {cm[1][1]/(cm[1][1]+cm[1][0])}")
