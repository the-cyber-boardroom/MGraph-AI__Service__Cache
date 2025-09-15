#!/bin/bash
uvicorn mgraph_ai_service_cache.fast_api.lambda_handler:app --reload --host 0.0.0.0 --port 10017