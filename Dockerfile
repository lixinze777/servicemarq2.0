FROM ubuntu

RUN apt-get update && apt-get install -y\
    git tmux vim wget unzip\
    python3.6 python3-pip\
    chromium-browser

COPY . /opt/cfp-mining/

RUN cd /opt/cfp-mining/ &&\
    pip3 install --no-cache-dir -r requirements.txt

RUN cd /opt/cfp-mining/ &&\
    wget https://chromedriver.storage.googleapis.com/79.0.3945.36/chromedriver_linux64.zip &&\
    unzip chromedriver_linux64.zip &&\
    rm chromedriver_linux64.zip
