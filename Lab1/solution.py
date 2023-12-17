from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, size, expr, concat_ws, udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import NGram, Tokenizer
from pyspark.ml import Pipeline

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder.appName("GitHubNGrams").getOrCreate()

def load_json_data(spark, file_path):
    """Load JSON data into a DataFrame."""
    return spark.read.json(file_path)

def preprocess_commit_messages(commit_messages):
    """Preprocess commit messages."""
    # Convert to lowercase and remove punctuation and underscores
    return commit_messages.lower().translate(str.maketrans('', '', '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'))

def process_data(df):
    """Process the DataFrame and apply transformations."""
    preprocess_udf = udf(preprocess_commit_messages, StringType())
    
    # Tokenize and apply NGram transformation
    tokenizer = Tokenizer(inputCol="processed_commits", outputCol="tokenized_words")
    ngram = NGram(n=3, inputCol=tokenizer.getOutputCol(), outputCol="ngrams_result")
    pipeline = Pipeline(stages=[tokenizer, ngram])
    model = pipeline.fit(df)
    result = model.transform(df)

    # Extract only the first five words from the n-grams
    result = result.withColumn(
        "first_five_words",
        expr("slice(ngrams_result, 1, 5)")
    )

    # Convert the array of strings to a single string
    result = result.withColumn("first_five_words", concat_ws(", ", col("first_five_words")))

    return result

def main():
    spark = create_spark_session()

    try:
        json_data = load_json_data(spark, "10K.github.jsonl")
        push_events = json_data.filter(col("type") == "PushEvent")
        processed_data = push_events.withColumn(
            "processed_commits",
            preprocess_commit_messages(col("payload.commits.message"))
        )
        result = process_data(processed_data)
        result.select("actor.display_login", "first_five_words").coalesce(1).write.csv("output.csv", header=True, mode="overwrite")
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
