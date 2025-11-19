#!/bin/bash

# use localstack endpoint
#export AWS_ENDPOINT_URL="http://localhost:4566"

# Set AWS credentials for LocalStack
#export AWS_ACCESS_KEY_ID=test
#export AWS_SECRET_ACCESS_KEY=test
#export AWS_DEFAULT_REGION=us-east-1

# Start uvicorn with the environment variables set
uvicorn mgraph_ai_service_cache.fast_api.lambda_handler:app --reload --host 0.0.0.0 --port 10017