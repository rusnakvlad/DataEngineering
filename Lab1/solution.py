from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, size, expr, concat_ws
from pyspark.ml.feature import NGram, Tokenizer
from pyspark.ml import Pipeline
import re

# Create a Spark session
spark = SparkSession.builder.appName("GitHubNGrams").getOrCreate()

# Load the JSON data into a DataFrame
json_data = spark.read.json("10K.github.jsonl")

# Filter data for PushEvent type
push_events = json_data.filter(col("type") == "PushEvent")

# Define a function to extract and preprocess commit messages
def process_commit_messages(commit_messages):
    # Convert to lowercase and remove punctuation and underscores
    cleaned_messages = [re.sub(r'\W+', ' ', msg.lower()) for msg in commit_messages]
    return ' '.join(cleaned_messages)

# UDF for processing commit messages
process_commit_messages_udf = spark.udf.register("process_commit_messages", process_commit_messages)

# Apply transformations to the DataFrame
processed_data = push_events.withColumn(
    "processed_commits",
    process_commit_messages_udf(col("payload.commits.message"))
)

# Tokenize and apply NGram transformation
tokenizer = Tokenizer(inputCol="processed_commits", outputCol="tokenized_words")
ngram = NGram(n=3, inputCol=tokenizer.getOutputCol(), outputCol="ngrams_result")
pipeline = Pipeline(stages=[tokenizer, ngram])
model = pipeline.fit(processed_data)
result = model.transform(processed_data)

# Extract only the first five words from the n-grams
result = result.withColumn(
    "first_five_words",
    expr("slice(ngrams_result, 1, case when size(ngrams_result) >= 5 then 5 else size(ngrams_result) end)")
)

# Handle cases where 1-2 words are present
result = result.withColumn(
    "first_five_words",
    expr("case when size(tokenized_words) <= 2 then tokenized_words else first_five_words end")
)

# Convert the array of strings to a single string
result = result.withColumn("first_five_words", concat_ws(", ", col("first_five_words")))

# Save the result to a CSV file
result.select("actor.display_login", "first_five_words").coalesce(1).write.csv("output.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()