FROM jupyter/pyspark-notebook:8d32a5208ca1 as build

USER root

RUN pip install pandas==1.2.2 \
    && pip install pyarrow==2.0.0 \
    && pip install confluent-kafka==1.6.1 \
    && pip install Faker==7.0.1 \
    && pip install sseclient==0.0.27 \
    && pip install validators \
    && pip install tweepy \
    && pip install confluent-kafka \
	&& pip install spark-nlp \
	&& pip install scikit-spark \
	&& pip install scikit-learn \
	&& pip install scipy \
	&& pip install matplotlib \
	&& pip install pydot \
	&& pip install tensorflow==2.3.1 \
	&& pip install graphviz \ 
	&& pip install pyspark \
	&& pip install python-dotenv \
	&& pip install jupyterlab-execute-time \
	&& pip install wordcloud \
	&& pip install bertopic

RUN jupyter labextension install jupyterlab-plotly@4.14.3	
	
# Remove Java 11	
RUN apt-get -y purge openjdk-\*
RUN apt -y autoremove 

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;
# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> ~/.bashrc

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PYTHONPATH "${PYTHONPATH}:/home/jovyan/work"

RUN echo "export PYTHONPATH=/home/jovyan/work" >> ~/.bashrc

WORKDIR /home/jovyan/work

RUN jupyter server extension disable nbclassic