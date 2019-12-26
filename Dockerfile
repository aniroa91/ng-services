FROM bigdata-registry.local:5043/openjdk_8u191-jre-alpine3.8:v2

ENV TZ=Asia/Ho_Chi_Minh
WORKDIR /var/opt/
COPY target/universal/bigdata-play-churn-1.0-SNAPSHOT.zip /var/opt/bigdata-play-churn-1.0-SNAPSHOT.zip
RUN unzip /var/opt/bigdata-play-churn-1.0-SNAPSHOT.zip \
  && rm -f /var/opt/bigdata-play-churn-1.0-SNAPSHOT.zip \
  && mv /var/opt/bigdata-play-churn-1.0-SNAPSHOT /var/opt/bigdata-play-churn
WORKDIR /var/opt/bigdata-play-churn
CMD ["bin/bigdata-play-churn","-Dconfig.file=conf/prod/application.conf"]