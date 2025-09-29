FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && \
	apt-get install -y gcc libgomp1 git

RUN pip install pymysql numpy redis pandas pika python-dotenv lightgbm scipy tqdm ipaddress ip2location ip2proxy git+https://github.com/mjschultz/py-radix.git@v1.0.0 dnspython
