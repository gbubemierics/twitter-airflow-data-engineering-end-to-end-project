# Update the package list to make sure we install the latest available versions
sudo apt-get update

# Install pip for Python 3
# pip is the package manager used to install Python libraries
sudo apt install python3-pip

# Install Apache Airflow
# Airflow is used to schedule, monitor, and orchestrate data pipelines
sudo pip install apache-airflow

# Install pandas
# pandas is used for data manipulation and writing structured data like CSV files
sudo pip install pandas

# Install s3fs
# s3fs allows Python and pandas to read from and write to Amazon S3 using standard file paths
sudo pip install s3fs

# Install tweepy
# tweepy is a Python client for the Twitter API, used to extract tweets
sudo pip install tweepy
