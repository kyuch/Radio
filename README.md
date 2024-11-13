For full functionality, run "pip install -r requirements.txt" in project directory. 

Please run "aws configure" in terminal to specify AWS Credentials!!!!

Run processData.py in terminal: python3 processData.py 

Run analyzeData.py in terminal: python3 analyzeData.py 

Run bedrockAnalysis.py in terminal: python3 bedrockAnalysis.py

Add "-h" to command to view arguments that can be set

processData.py connects to a DX Cluster, collects all spotted callsigns from a provided spotter, 
enhances the data for each spotted callsign, and uploads the enhanced data to a SQLite database.

analyzeData.py collects callsign info from a SQLite database, analyzes the data into a pivot table, 
generates an HTML page with the table, and uploads it to an AWS S3 bucket.
