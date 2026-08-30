import tensorflow as tf
import numpy as np
from google.cloud import bigquery
from sklearn.model_selection import train_test_split

class TransitDaysPredictor(tf.Module):
    def __init__(self, model):
        super().__init__()
        self.model = model

    @tf.function(input_signature=[tf.TensorSpec(shape=[None, None], dtype=tf.float32, name="instances")])
    def serving_fn(self, instances):
        predictions = self.model(instances)
        # Reshape to standard 1D output for predictions if needed, or keep 2D
        # Returning dictionary bound to prediction
        return {"prediction": tf.reshape(predictions, [-1])}

def run():
    client = bigquery.Client()
    
    # Wait, the instruction says "Input: BQ dataset". Since this is pyspark dataproc,
    # we can either read with spark or bigquery client. Let's use bigquery client to keep it simple,
    # or pyspark if we need it. The instruction asks to train a standard Keras NN on the 4 features.
    
    query = """
        SELECT
            shipping_month,
            shipping_day,
            num_of_item,
            haversine_distance,
            transit_days
        FROM
            `your-project-id.mlops.training_dataset`
    """
    df = client.query(query).to_dataframe()
    df = df.dropna()
    
    X = df[['shipping_month', 'shipping_day', 'num_of_item', 'haversine_distance']].astype(np.float32).values
    y = df['transit_days'].astype(np.float32).values
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    normalizer = tf.keras.layers.Normalization(axis=-1)
    normalizer.adapt(X_train)
    
    model = tf.keras.Sequential([
        normalizer,
        tf.keras.layers.Dense(64, activation='relu'),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(1)
    ])
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    model.fit(X_train, y_train, epochs=10, validation_data=(X_test, y_test), batch_size=32)
    
    # Export model using custom tf.Module wrapper
    module = TransitDaysPredictor(model)
    tf.saved_model.save(
        module,
        "gs://your-bucket-name/models/tf_transit_days_model",
        signatures={"serving_default": module.serving_fn}
    )

if __name__ == '__main__':
    run()
