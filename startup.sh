#!/bin/bash

# Install Streamlit if not already installed
if ! command -v streamlit &> /dev/null
then
    echo "Streamlit not found, installing..."
    pip install streamlit
fi
pip install snowflake-connector-python
pip install snowflake-snowpark-python


# Run the Streamlit app
streamlit run stapp_metrics.py  --server.port 8000 --server.address 0.0.0.0

