import pandas as pd
import numpy as np
import os
from flask import Flask, request, jsonify, json
from azure.storage.blob import BlobServiceClient
from flask import Flask
from flask_cors import CORS
from flask_cors import cross_origin
from flask_caching import Cache

app = Flask(__name__)
cors = CORS(app)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

connection_string = "DefaultEndpointsProtocol=https;AccountName=dataq1059;AccountKey=75GB2xLlhCTrny55fKxWN7d8Rckjtl8GT4SIsG0Sojyq0Fy92TKn/oshpvK/XVCB2F01CVejn699+ASt1/FFOA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = "test"
container_client = blob_service_client.get_container_client(container_name)
blobs = container_client.list_blobs()

def viewproperties(df):
    df = df.dropna(axis = 0, how = 'all')
    columns = df.columns

    statValues = dict()

    for col in columns:
        nonBlankColumn = df[col].dropna()

        if nonBlankColumn.dtype == 'object':
            df[col] = pd.to_numeric(df[col], errors='ignore')

        if nonBlankColumn.dtype == 'float64':
            df[col] = df[col].fillna(1299999999999076)
            if (df[col] % 1 == 0).all():
                df[col] = df[col].astype("int64")
            else:
                df[col] = df[col].replace(1299999999999076, np.nan).astype(float)

        if nonBlankColumn.dtype == 'object':
                df[col] = pd.to_datetime(df[col], errors = 'ignore')

    for col in columns:
        nonBlankColumn = df[~df[col].isnull()][col]
        nonBlankColumn = nonBlankColumn[nonBlankColumn != 1299999999999076]

        if (len(nonBlankColumn.unique()) > 5) or (len(nonBlankColumn.unique()) == 0):
            categories = 'NA'
        else:
            categories = list(nonBlankColumn.unique())
        
        if nonBlankColumn.dtype == 'int64':
            statValues[col] = {'Data Type' : 'Integer',
                            'Categories' : categories,
                            'Minimum Value' : nonBlankColumn.min(),
                            '25th Percentile' : nonBlankColumn.quantile(.25),
                            '50th Percentile' : nonBlankColumn.quantile(.5),
                            '75th Percentile' : nonBlankColumn.quantile(.75),
                            'Maximum Value' : nonBlankColumn.max(),
                            'Mode' : "NA"}
            
        if nonBlankColumn.dtype == 'float64':
            statValues[col] = {'Data Type' : 'Decimal',
                            'Categories' : categories,
                            'Minimum Value' : nonBlankColumn.min(),
                            '25th Percentile' : nonBlankColumn.quantile(.25),
                            '50th Percentile' : nonBlankColumn.quantile(.5),
                            '75th Percentile' : nonBlankColumn.quantile(.75),
                            'Maximum Value' : nonBlankColumn.max(),
                            'Mode' : "NA"}
            
        if nonBlankColumn.dtype == 'datetime64[ns]':
            statValues[col] = {'Data Type' : 'Date/Time',
                            'Categories' : categories,
                            'Minimum Value' : nonBlankColumn.min(),
                            '25th Percentile' : nonBlankColumn.quantile(.25),
                            '50th Percentile' : nonBlankColumn.quantile(.5),
                            '75th Percentile' : nonBlankColumn.quantile(.75),
                            # '25th Percentile': "NA",
                            # '50th Percentile': "NA",
                            # '75th Percentile': "NA",
                            'Maximum Value' : nonBlankColumn.max(),
                            'Mode' : "NA"}
            
        if nonBlankColumn.dtype == 'object':
            statValues[col] = {'Data Type' : 'Text',
                            'Categories' : categories,
                            'Minimum Value' : 'NA',
                            '25th Percentile' : 'NA',
                            '50th Percentile' : 'NA',
                            '75th Percentile' : 'NA',
                            'Maximum Value' : 'NA',
                            'Mode' : "NA"}              
    return statValues

@app.route("/api/getFiles", methods=['POST'])
@cross_origin(origin='https://dq-tool-frtnd.azurewebsites.net/',headers=['Content-Type'])
@cache.cached(timeout = 10)


def getFiles():
    json_data = request.data  # Get the raw JSON data from the request's body
    data = json.loads(json_data)
    results = []
    selected_sheets = [sheet_name for file_data in data.values() for sheet_name, sheet_data in file_data.items() if sheet_data.get('selected')]
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob.name)
        excel_data = blob_client.download_blob().readall()
        for sheet_name in selected_sheets:
            df = pd.read_excel(excel_data, sheet_name=sheet_name, header=0)
            result = viewproperties(df)  # Get the result from viewproperties(df)
            results.append(result)                   
    return json.dumps(results, default = str)


if __name__ == "__main__":
    app.run(debug=True, host = "0.0.0.0")
