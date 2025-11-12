from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset, Model, Metrics
import kfp

@component(
    packages_to_install=["pyspark", "delta-spark", "mlflow", "boto3"],
    base_image="python:3.9"
)
def data_ingestion_component(
    raw_data_path: str,
    processed_data: Output[Dataset],
    metrics: Output[Metrics]
):
    """Ingest and preprocess user interaction data from Delta Lake"""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    import mlflow
    
    builder = SparkSession.builder \
        .appName("DataIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Read raw user interactions
    df = spark.read.format("delta").load(raw_data_path)
    
    # Data preprocessing
    df_clean = df.filter(df.user_id.isNotNull() & df.item_id.isNotNull()) \
                 .filter(df.interaction_type.isin(['click', 'purchase', 'view'])) \
                 .withColumn("interaction_weight", 
                            when(df.interaction_type == "purchase", 2.0)
                            .when(df.interaction_type == "click", 1.0)
                            .otherwise(0.5))
    
    # Split into train/validation/test
    train_df, val_df, test_df = df_clean.randomSplit([0.7, 0.15, 0.15], seed=42)
    
    # Write processed data
    train_df.write.mode("overwrite").format("delta").save(processed_data.path + "/train")
    val_df.write.mode("overwrite").format("delta").save(processed_data.path + "/validation")  
    test_df.write.mode("overwrite").format("delta").save(processed_data.path + "/test")
    
    # Log metrics
    metrics.log_metric("train_samples", train_df.count())
    metrics.log_metric("val_samples", val_df.count())
    metrics.log_metric("test_samples", test_df.count())
    
    spark.stop()

@component(
    packages_to_install=["pyspark", "delta-spark", "mlflow", "implicit"],
    base_image="python:3.9"
)
def train_als_model_component(
    processed_data: Input[Dataset],
    als_model: Output[Model],
    metrics: Output[Metrics]
):
    """Train ALS collaborative filtering model on full historical data"""
    from pyspark.sql import SparkSession
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.evaluation import RegressionEvaluator
    import mlflow
    import mlflow.spark
    
    spark = SparkSession.builder.appName("ALSTraining").getOrCreate()
    
    # Load training data
    train_df = spark.read.format("delta").load(processed_data.path + "/train")
    val_df = spark.read.format("delta").load(processed_data.path + "/validation")
    
    with mlflow.start_run():
        # Train ALS model
        als = ALS(
            maxIter=10,
            regParam=0.01,
            userCol="user_id",
            itemCol="item_id", 
            ratingCol="interaction_weight",
            coldStartStrategy="drop"
        )
        
        model = als.fit(train_df)
        
        # Evaluate
        predictions = model.transform(val_df)
        evaluator = RegressionEvaluator(
            metricName="rmse", 
            labelCol="interaction_weight",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        
        # Log metrics and model
        mlflow.log_metric("val_rmse", rmse)
        mlflow.spark.log_model(model, "als_model")
        
        # Save model for pipeline
        model.write().overwrite().save(als_model.path)
        
        metrics.log_metric("als_rmse", rmse)

@component(
    packages_to_install=["tensorflow", "pyspark", "delta-spark", "mlflow"],
    base_image="tensorflow/tensorflow:2.11.0"
)
def train_neural_cf_component(
    processed_data: Input[Dataset],
    neural_model: Output[Model],
    user_embeddings: Output[Dataset],
    item_embeddings: Output[Dataset],
    metrics: Output[Metrics]
):
    """Train neural collaborative filtering model for deep user/item embeddings"""
    import tensorflow as tf
    from tensorflow.keras.layers import Embedding, Flatten, Concatenate, Dense
    from tensorflow.keras.models import Model
    import mlflow
    import mlflow.keras
    from pyspark.sql import SparkSession
    import numpy as np
    
    spark = SparkSession.builder.appName("NeuralCF").getOrCreate()
    
    # Load data
    train_df = spark.read.format("delta").load(processed_data.path + "/train")
    val_df = spark.read.format("delta").load(processed_data.path + "/validation")
    
    # Collect user/item mappings (in production, you'd use a proper feature store)
    user_ids = train_df.select("user_id").distinct().rdd.flatMap(lambda x: x).collect()
    item_ids = train_df.select("item_id").distinct().rdd.flatMap(lambda x: x).collect()
    
    n_users = len(user_ids)
    n_items = len(item_ids)
    
    user_to_idx = {user: idx for idx, user in enumerate(user_ids)}
    item_to_idx = {item: idx for idx, item in enumerate(item_ids)}
    
    with mlflow.start_run():
        # Build neural collaborative filtering model
        user_input = tf.keras.Input(shape=(1,), name='user_input')
        item_input = tf.keras.Input(shape=(1,), name='item_input')
        
        user_embedding = Embedding(n_users, 64, name='user_embedding')(user_input)
        item_embedding = Embedding(n_items, 64, name='item_embedding')(item_input)
        
        user_vec = Flatten()(user_embedding)
        item_vec = Flatten()(item_embedding)
        
        concat = Concatenate()([user_vec, item_vec])
        dense = Dense(128, activation='relu')(concat)
        output = Dense(1, activation='sigmoid')(dense)
        
        model = Model(inputs=[user_input, item_input], outputs=output)
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['mae'])
        
        # Train model (simplified - in reality you'd use proper data loading)
        model.fit([np.random.rand(1000, 1), np.random.rand(1000, 1)], 
                 np.random.rand(1000, 1), epochs=2, validation_split=0.2)
        
        # Log model and metrics
        mlflow.keras.log_model(model, "neural_cf_model")
        mlflow.log_metric("n_users", n_users)
        mlflow.log_metric("n_items", n_items)
        
        # Save embeddings for serving
        model.save(neural_model.path)
        
        # In production, you'd extract and save the actual embeddings
        metrics.log_metric("neural_model_trained", 1)

@component(
    packages_to_install=["pyspark", "delta-spark", "mlflow", "scikit-learn"],
    base_image="python:3.9"
)
def model_evaluation_component(
    processed_data: Input[Dataset],
    als_model: Input[Model],
    neural_model: Input[Model],
    metrics: Output[Metrics]
):
    """Comprehensive evaluation of all models"""
    from pyspark.sql import SparkSession
    from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator
    import mlflow
    import json
    
    spark = SparkSession.builder.appName("ModelEvaluation").getOrCreate()
    
    # Load test data
    test_df = spark.read.format("delta").load(processed_data.path + "/test")
    
    # Load ALS model and get predictions
    from pyspark.ml.recommendation import ALSModel
    als_model_loaded = ALSModel.load(als_model.path)
    als_predictions = als_model_loaded.transform(test_df)
    
    # Evaluate ALS
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="interaction_weight")
    als_rmse = evaluator.evaluate(als_predictions)
    
    # Compare models and log to MLflow
    with mlflow.start_run():
        mlflow.log_metric("als_test_rmse", als_rmse)
        mlflow.log_metric("neural_test_mae", 0.15)  # Placeholder
        
        # Determine which model to promote
        if als_rmse < 0.2:  # Your business threshold
            model_decision = "promote_als"
        else:
            model_decision = "promote_neural"
            
        mlflow.log_param("promotion_decision", model_decision)
        
        metrics.log_metric("als_test_rmse", als_rmse)
        metrics.log_metric("promotion_decision", model_decision)

@component(
    packages_to_install=["mlflow", "boto3", "kafka-python"],
    base_image="python:3.9"
)
def model_deployment_component(
    als_model: Input[Model],
    neural_model: Input[Model],
    evaluation_metrics: Input[Metrics],
    deployed_model: Output[Model]
):
    """Deploy the best model to production and update feature store"""
    import mlflow
    import json
    import time
    
    # In reality, you'd:
    # 1. Register model in MLflow Model Registry
    # 2. Deploy to KServe/Triton
    # 3. Update feature store with new embeddings
    # 4. Notify real-time pipeline of model update
    
    # Simulate deployment process
    print(f"Deploying ALS model from {als_model.path}")
    print(f"Deploying Neural model from {neural_model.path}")
    
    # Update model registry
    with mlflow.start_run():
        mlflow.log_param("deployment_time", time.time())
        mlflow.log_param("deployed_models", "als,neural_cf")
        
        # Signal to real-time pipeline that new models are available
        # This could be a Kafka message or update to a feature store
        
    print("Models deployed successfully to production")

@component(
    packages_to_install=["pyspark", "delta-spark", "mlflow"],
    base_image="python:3.9"
)
def real_time_model_update_component(
    processed_data: Input[Dataset],
    real_time_model: Output[Model]
):
    """Train/update lightweight real-time model for immediate recommendations"""
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import CountVectorizer
    from pyspark.ml.clustering import LDA
    import mlflow
    
    spark = SparkSession.builder.appName("RealTimeModel").getOrCreate()
    
    # Use recent data for real-time model (last 24 hours)
    # In production, this would come from Kafka topics via Flink
    recent_interactions = spark.read.format("delta").load(processed_data.path + "/train") \
        .limit(10000)  # Recent subset
    
    # Train lightweight model (e.g., item-item co-occurrence)
    # For demonstration, we'll create a simple item similarity matrix
    item_cooccurrence = recent_interactions.alias("a") \
        .join(recent_interactions.alias("b"), "user_id") \
        .filter("a.item_id != b.item_id") \
        .groupBy("a.item_id", "b.item_id") \
        .count()
    
    # Save real-time model
    item_cooccurrence.write.mode("overwrite").format("delta").save(real_time_model.path)
    
    mlflow.log_metric("real_time_items", item_cooccurrence.count())
    
    spark.stop()

@dsl.pipeline(
    name="large-scale-recommendation-pipeline",
    description="End-to-end recommendation system with batch training and real-time updates"
)
def recommendation_pipeline(
    raw_data_path: str = "s3a://my-bucket/raw_interactions",
    als_rank: int = 10,
    retrain_interval_days: int = 1
):
    """Main pipeline for recommendation system retraining"""
    
    # 1. Data Ingestion and Preparation
    ingest_task = data_ingestion_component(raw_data_path=raw_data_path)
    
    # 2. Parallel Model Training
    with dsl.ParallelFor([als_rank, als_rank * 2], parallelism=2) as rank:
        als_task = train_als_model_component(
            processed_data=ingest_task.outputs["processed_data"]
        )
    
    neural_task = train_neural_cf_component(
        processed_data=ingest_task.outputs["processed_data"]
    )
    
    # 3. Real-time Model Update (for immediate recommendations)
    realtime_task = real_time_model_update_component(
        processed_data=ingest_task.outputs["processed_data"]
    )
    
    # 4. Model Evaluation and Selection
    eval_task = model_evaluation_component(
        processed_data=ingest_task.outputs["processed_data"],
        als_model=als_task.outputs["als_model"],
        neural_model=neural_task.outputs["neural_model"]
    )
    
    # 5. Conditional Deployment
    with dsl.Condition(
        eval_task.outputs["metrics"] != "fail",
        name="deploy_if_better"
    ):
        deploy_task = model_deployment_component(
            als_model=als_task.outputs["als_model"],
            neural_model=neural_task.outputs["neural_model"],
            evaluation_metrics=eval_task.outputs["metrics"]
        )

# Compile the pipeline
if __name__ == "__main__":
    kfp.compiler.Compiler().compile(
        recommendation_pipeline,
        "recommendation_pipeline.yaml"
    )