import boto3
import pandas as pd

s3 = boto3.client("s3")
obj = s3.get_object(Bucket="bhavikabucket22", Key="providers.csv")
df = pd.read_csv(obj['Body'])
print(df.head())
