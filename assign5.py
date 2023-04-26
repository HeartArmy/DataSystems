# import required libraries
from pyspark.sql import SparkSession

# initialize SparkSession
spark = SparkSession.builder.appName('Titanic').getOrCreate()

# load the Titanic dataset into a PySpark DataFrame
titanic_df = spark.read.csv("/Users/arham/Downloads/titanic.csv", header=True, inferSchema=True)

titanic_df.show(5)

# Count and retrieve 10 passengers who survived
survived_passengers = titanic_df.filter(titanic_df["Survived"] == 1)
print("Number of passengers who survived:", survived_passengers.count())
print("The first 10 passengers who survived:")
survived_passengers.show(10)

# Add a column called "IsChild" that contains "True" if the passenger's age is less than 18, and "False" otherwise
titanic_df = titanic_df.withColumn("IsChild", titanic_df["Age"] < 18)
titanic_df.show(5)

# Group the DataFrame by the "Pclass" (Ticket class) column and count the number of passengers in each class
passenger_counts = titanic_df.groupBy("Pclass").count()
passenger_counts.show()

# Rename the "Pclass" column to "PassengerClass" using the alias function
titanic_df = titanic_df.withColumnRenamed("Pclass", "PassengerClass")
titanic_df.show(5)

# Sort the DataFrame by the "Age" column in descending order
titanic_df = titanic_df.sort("Age", ascending=False)
titanic_df.show(5)

# Calculate the average age of passengers in each passenger class using the groupBy and agg functions
avg_age_per_class = titanic_df.groupBy("PassengerClass").agg({"Age": "avg"})
avg_age_per_class.show()

# Find the top 5 passengers with the highest fare
top_5_fares = titanic_df.sort("Fare", ascending=False).limit(5)
top_5_fares.show()

# Create a new DataFrame that contains the total fare collected per embarkation point (C = Cherbourg, Q = Queenstown, S = Southampton).
# Then, join this DataFrame with the Titanic dataset on the "Embarked" column
fare_by_embarkation = titanic_df.groupBy("Embarked").agg({"Fare": "sum"})
joined_df = titanic_df.join(fare_by_embarkation, "Embarked")
joined_df.show()

# Calculate the survival rate per class and gender
survival_rate = titanic_df.groupBy(["PassengerClass", "Sex"]).agg({"Survived": "mean"})
survival_rate.show()

# Save the modified DataFrame titanic_df to parquet format and read it back
titanic_df.write.parquet("titanic_modified.parquet")
titanic_df_from_parquet = spark.read.parquet("titanic_modified.parquet")
titanic_df_from_parquet.show(5)
