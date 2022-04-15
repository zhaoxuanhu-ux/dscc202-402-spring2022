# Databricks notebook source
# MAGIC %md
# MAGIC # Putting it all together: Managing the Machine Learning Lifecycle
# MAGIC 
# MAGIC Create a workflow that includes pre-processing logic, the optimal ML algorithm and hyperparameters, and post-processing logic.
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC In this course, we've primarily used Random Forest in `sklearn` to model the Airbnb dataset.  In this exercise, perform the following tasks:
# MAGIC <br><br>
# MAGIC 0. Create custom pre-processing logic to featurize the data
# MAGIC 0. Try a number of different algorithms and hyperparameters.  Choose the most performant solution
# MAGIC 0. Create related post-processing logic
# MAGIC 0. Package the results and execute it as its own run
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Web browser: Chrome
# MAGIC - A cluster configured with **8 cores** and **DBR 7.0 ML**

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
# MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# Adust our working directory from what DBFS sees to what python actually sees
working_path = workingDir.replace("dbfs:", "/dbfs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-processing
# MAGIC 
# MAGIC Take a look at the dataset and notice that there are plenty of strings and `NaN` values present. Our end goal is to train a sklearn regression model to predict the price of an airbnb listing.
# MAGIC 
# MAGIC 
# MAGIC Before we can start training, we need to pre-process our data to be compatible with sklearn models by making all features purely numerical. 

# COMMAND ----------

import pandas as pd

airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/sf-listings-correct-types.parquet").toPandas()

display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC In the following cells we will walk you through the most basic pre-processing step necessary. Feel free to add additional steps afterwards to improve your model performance.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First, convert the `price` from a string to a float since the regression model will be predicting numerical values.

# COMMAND ----------

airbnbDF["price"]

# COMMAND ----------

# TODO
airbnbDF["price"] = airbnbDF["price"].apply(lambda x: x.replace("$","").replace(",","")).astype(float)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at our remaining columns with strings (or numbers) and decide if you would like to keep them as features or not.
# MAGIC 
# MAGIC Remove the features you decide not to keep.

# COMMAND ----------

airbnbDF.info()

# COMMAND ----------

# MAGIC %md Firstly, I plot the correlation heatmap to see which numerical features should be droped.

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
 
plt.figure(figsize=(10,8))
sns.heatmap(airbnbDF.corr(), cmap = "coolwarm")
plt.show()

# COMMAND ----------

airbnbDF.corr()["price"]

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the correlation and my own intuition, I would just retain latitude, accomodates, bathrooms, bedrooms, beds, number of reviews and 
# MAGIC review_scores_rating

# COMMAND ----------

num_cols = ['latitude','accommodates','bathrooms', 'bedrooms', 'beds','number_of_reviews', 'review_scores_rating','price']

# COMMAND ----------

categorical_columns = [col for col in airbnbDF.columns if airbnbDF[col].dtype=='object']
categorical_columns

# COMMAND ----------

airbnbDF = airbnbDF[num_cols+categorical_columns]

# COMMAND ----------

for i in categorical_columns:
    print(airbnbDF[i].unique())

# COMMAND ----------

# MAGIC %md
# MAGIC Since these categorical variables are not ordinal, we just create label encoder for host_is_superhost and instant_bookable variables and 
# MAGIC create dummy for the remaining categorical variables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For the string columns that you've decided to keep, pick a numerical encoding for the string columns. Don't forget to deal with the `NaN` entries in those columns first.

# COMMAND ----------

# TODO
airbnbDF['host_is_superhost'] = [1 if i == "t" else 0 if i=="f" else None for i in airbnbDF["host_is_superhost"]]
airbnbDF['instant_bookable'] = [1 if i == "t" else 0 if i=="f" else None for i in airbnbDF["instant_bookable"]]

# COMMAND ----------

airbnbDF = pd.get_dummies(airbnbDF)
 
X = airbnbDF.drop("price", axis=1)
Y = airbnbDF['price']

# COMMAND ----------

# MAGIC %md
# MAGIC Then I use KNN to impute the missing values

# COMMAND ----------

from sklearn.impute import KNNImputer
 
imputer = KNNImputer(n_neighbors=3) 
 
X = imputer.fit_transform(X)

# COMMAND ----------

# MAGIC %md
# MAGIC Before we create a train test split, check that all your columns are numerical. Remember to drop the original string columns after creating numerical representations of them.
# MAGIC 
# MAGIC Make sure to drop the price column from the training data when doing the train test split.

# COMMAND ----------

# TODO
from sklearn.model_selection import train_test_split
X_train, X_test, Y_train, Y_test = train_test_split(X,Y,test_size = 0.3, random_state = 42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model
# MAGIC 
# MAGIC After cleaning our data, we can start creating our model!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Firstly, if there are still `NaN`'s in your data, you may want to impute these values instead of dropping those entries entirely. Make sure that any further processing/imputing steps after the train test split is part of a model/pipeline that can be saved.
# MAGIC 
# MAGIC In the following cell, create and fit a single sklearn model.

# COMMAND ----------

# MAGIC %md
# MAGIC Now I would just simply use linear regression to train my model.

# COMMAND ----------

# TODO
from sklearn.linear_model import LinearRegression
lr = LinearRegression()
lr.fit(X_train, Y_train)

# COMMAND ----------

# MAGIC %md
# MAGIC Pick and calculate a regression metric for evaluating your model.

# COMMAND ----------

# TODO
import numpy as np
y_pred = lr.predict(X_test)
y_pred

from sklearn import metrics
np.sqrt(metrics.mean_squared_error(Y_test, y_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Log your model on MLflow with the same metric you calculated above so we can compare all the different models you have tried! Make sure to also log any hyperparameters that you plan on tuning!

# COMMAND ----------

# TODO
import mlflow.sklearn
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_squared_error
with mlflow.start_run(run_name="Basic elastic net experiment") as run:
  # Create model, train it, and create predictions
  en = ElasticNet(random_state=42)
  en.fit(X_train, Y_train)
  predictions = en.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(en, "elastic-net-model")
  
  # Create metrics
  mse = mean_squared_error(Y_test, predictions)
  print(f"mse: {mse}")
  
  # Log metrics
  mlflow.log_metric("mse", mse)
  
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  print(f"Inside MLflow Run with run_id `{runID}` and experiment_id `{experimentID}`")

# COMMAND ----------

def log_elastic_net(experimentID, run_name, params, X_train, X_test, Y_train, Y_test):
  import os
  import matplotlib.pyplot as plt
  import mlflow.sklearn
  import seaborn as sns
  from sklearn.linear_model import ElasticNet
  from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
  import tempfile

  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    # Create model, train it, and create predictions
    en = ElasticNet(**params)
    en.fit(X_train, Y_train)
    predictions = en.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(en, "elastic-net-model")

    # Log params
    mlflow.log_params(params)

    # Create metrics
    mse = mean_squared_error(Y_test, predictions)
    mae = mean_absolute_error(Y_test, predictions)
    r2 = r2_score(Y_test, predictions)

    # Log metrics
    mlflow.log_metrics({"mse": mse, "mae": mae, "r2": r2})
    
    # Create plot
    fig, ax = plt.subplots()

    sns.residplot(predictions, Y_test, lowess=True)
    plt.xlabel("Predicted values for Price ($)")
    plt.ylabel("Residual")
    plt.title("Residual Plot")

    # Log residuals using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="residuals-", suffix=".png")
    temp_name = temp.name
    try:
      fig.savefig(temp_name)
      mlflow.log_artifact(temp_name, "residuals.png")
    finally:
      temp.close() # Delete the temp file
      
    display(fig)
    return run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change and re-run the above 3 code cells to log different models and/or models with different hyperparameters until you are satisfied with the performance of at least 1 of them.

# COMMAND ----------

params = {
  "alpha": 0.8,
  "max_iter": 1000,
  "l1_ratio": 0.5
}

log_elastic_net(experimentID, "Second Run", params, X_train, X_test, Y_train, Y_test)

# COMMAND ----------

params = {
  "alpha": 0.6,
  "max_iter": 1500,
  "l1_ratio": 0.3
}

log_elastic_net(experimentID, "Third Run", params, X_train, X_test, Y_train, Y_test)

# COMMAND ----------

params = {
  "alpha": 1,
  "max_iter": 500,
  "l1_ratio": 0.8
}

log_elastic_net(experimentID, "Fourth Run", params, X_train, X_test, Y_train, Y_test)

# COMMAND ----------

from  mlflow.tracking import MlflowClient

client = MlflowClient()


# COMMAND ----------

runs = client.search_runs(experimentID, order_by=["attributes.start_time desc"])
for i in runs:
    print(i.data.metrics)

# COMMAND ----------

model_uri = "runs:/"+runs[1].info.run_id+"/elastic-net-model"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Look through the MLflow UI for the best model. Copy its `URI` so you can load it as a `pyfunc` model.

# COMMAND ----------

# TODO
import mlflow.pyfunc
en_pyfunc_model = mlflow.pyfunc.load_pyfunc(model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-processing
# MAGIC 
# MAGIC Our model currently gives us the predicted price per night for each Airbnb listing. Now we would like our model to tell us what the price per person would be for each listing, assuming the number of renters is equal to the `accommodates` value. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Fill in the following model class to add in a post-processing step which will get us from total price per night to **price per person per night**.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://www.mlflow.org/docs/latest/models.html#id13" target="_blank">the MLFlow docs for help.</a>

# COMMAND ----------

# TODO

class Airbnb_Model(mlflow.pyfunc.PythonModel):

    def __init__(self, model):
        self.model = model
    
    def predict(self, context, model_input):
        y_pred = self.model.predict(model_input)
        accomodates = model_input[:,1]
        result = []
        for i in range(y_pred.shape[0]):
            result.append(y_pred[i]/accomodates[i])
        return result

# COMMAND ----------

# MAGIC %md
# MAGIC Construct and save the model to the given `final_model_path`.

# COMMAND ----------

en = ElasticNet(alpha=0.6,max_iter=1500,l1_ratio=0.3)
en.fit(X_train, Y_train)
en_mse = mean_squared_error(Y_test, en.predict(X_test))

# COMMAND ----------

# TODO
final_model_path =  f"{working_path}/final-model-final-new"
dbutils.fs.rm(final_model_path, True)
en_postprocess_model = Airbnb_Model(model = en)
mlflow.pyfunc.save_model(path=final_model_path.replace("dbfs:", "/dbfs"), python_model=en_postprocess_model)

# COMMAND ----------

# MAGIC %md
# MAGIC Load the model in `python_function` format and apply it to our test data `X_test` to check that we are getting price per person predictions now.

# COMMAND ----------

# TODO
loaded_postprocess_model = mlflow.pyfunc.load_pyfunc(final_model_path.replace("dbfs:", "/dbfs"))

# Apply the model
loaded_postprocess_model.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Packaging your Model
# MAGIC 
# MAGIC Now we would like to package our completed model! 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC First save your testing data at `test_data_path` so we can test the packaged model.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** When using `.to_csv` make sure to set `index=False` so you don't end up with an extra index column in your saved dataframe.

# COMMAND ----------

test_data_path = f"{working_path}/test_data.csv"
pd.DataFrame(X_test).to_csv(test_data_path, index = False)
prediction_path = f"{working_path}/predictions.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC First we will determine what the project script should do. Fill out the `model_predict` function to load out the trained model you just saved (at `final_model_path`) and make price per person predictions on the data at `test_data_path`. Then those predictions should be saved under `prediction_path` for the user to access later.
# MAGIC 
# MAGIC Run the cell to check that your function is behaving correctly and that you have predictions saved at `demo_prediction_path`.

# COMMAND ----------

# TODO
import click
import mlflow.pyfunc
import pandas as pd

@click.command()
@click.option("--final_model_path", default="", type=str)
@click.option("--test_data_path", default="", type=str)
@click.option("--prediction_path", default="", type=str)
def model_predict(final_model_path, test_data_path, prediction_path):
    model = mlflow.pyfunc.load_pyfunc(final_model_path.replace("dbfs:", "/dbfs"))
    predictions = model.predict(np.array(pd.read_csv(test_data_path)))
    pd.DataFrame(predictions).to_csv(prediction_path, index=False)
# test model_predict function    
demo_prediction_path = f"{working_path}/predictions.csv"

from click.testing import CliRunner
runner = CliRunner()
result = runner.invoke(model_predict, ['--final_model_path', final_model_path, 
                                       '--test_data_path', test_data_path,
                                       '--prediction_path', demo_prediction_path], catch_exceptions=True)

assert result.exit_code == 0, "Code failed" # Check to see that it worked
print("Price per person predictions: ")
print(pd.read_csv(demo_prediction_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will create a MLproject file and put it under our `workingDir`. Complete the parameters and command of the file.

# COMMAND ----------

# TODO
dbutils.fs.put(f"{workingDir}/MLproject", 
'''
name: Capstone-Project

conda_env: conda.yaml

entry_points:
  main:
    parameters:
      final_model_path: {type: str, default: "/dbfs/user/zhu21@ur.rochester.edu/mlflow/99_putting_it_all_together_psp/final-model-final-new"}
      test_data_path: {type: text, default: "/dbfs/user/zhu21@ur.rochester.edu/mlflow/99_putting_it_all_together_psp/test_data.csv"}
      prediction_path: {type: text, default: "/dbfs/user/zhu21@ur.rochester.edu/mlflow/99_putting_it_all_together_psp/predictions.csv"}
    command:  "python predict.py --final_model_path {final_model_path} --test_data_path {test_data_path} --prediction_path {prediction_path}"
'''.strip(), overwrite=True)

# COMMAND ----------

print(test_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC We then create a `conda.yaml` file to list the dependencies needed to run our script.
# MAGIC 
# MAGIC For simplicity, we will ensure we use the same version as we are running in this notebook.

# COMMAND ----------

import cloudpickle, numpy, pandas, sklearn, sys

version = sys.version_info # Handles possibly conflicting Python versions

file_contents = f"""
name: Capstone
channels:
  - defaults
dependencies:
  - python={version.major}.{version.minor}.{version.micro}
  - cloudpickle={cloudpickle.__version__}
  - numpy={numpy.__version__}
  - pandas={pandas.__version__}
  - scikit-learn={sklearn.__version__}
  - pip:
    - mlflow=={mlflow.__version__}
""".strip()

dbutils.fs.put(f"{workingDir}/conda.yaml", file_contents, overwrite=True)

print(file_contents)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will put the **`predict.py`** script into our project package.
# MAGIC 
# MAGIC Complete the **`.py`** file by copying and placing the **`model_predict`** function you defined above.

# COMMAND ----------

# TODO
dbutils.fs.put(f"{workingDir}/predict.py", 
'''
import click
import mlflow.pyfunc
import pandas as pd

# put model_predict function with decorators here
    
if __name__ == "__main__":
  model_predict()

'''.strip(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's double check all the files we've created are in the `workingDir` folder. You should have at least the following 3 files:
# MAGIC * `MLproject`
# MAGIC * `conda.yaml`
# MAGIC * `predict.py`

# COMMAND ----------

display( dbutils.fs.ls(workingDir) )

# COMMAND ----------

# MAGIC %md
# MAGIC Under **`workingDir`** is your completely packaged project.
# MAGIC 
# MAGIC Run the project to use the model saved at **`final_model_path`** to predict the price per person of each Airbnb listing in **`test_data_path`** and save those predictions under **`second_prediction_path`** (defined below).

# COMMAND ----------

# TODO
second_prediction_path = f"{working_path}/predictions-2.csv"
mlflow.projects.run(uri=working_path,
   parameters={
    "final_model_path": final_model_path,
    "test_data_path": test_data_path,
    "prediction_path": second_prediction_path
})

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to check that your model's predictions are there!

# COMMAND ----------

print("Price per person predictions: ")
print(pd.read_csv(second_prediction_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> All done!</h2>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
