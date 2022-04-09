FROM postgres:14

RUN apt-get update && apt-get install python3 -y postgresql-plpython3-14
RUN apt-get -y install python3-pip

RUN  apt-get clean && \
     rm -rf /var/cache/apt/* /var/lib/apt/lists/*

# Requirements installation
COPY requirements.txt /tmp/
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --requirement /tmp/requirements.txt