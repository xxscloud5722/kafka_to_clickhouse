FROM ubuntu:24.04
ADD ./log_sync /app/log_sync
ENV TZ=Asia/Shanghai
RUN chmod +x /app/log_sync && export DEBIAN_FRONTEND=noninteractive && apt-get update -y && apt-get install -y tzdata && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && dpkg-reconfigure --frontend noninteractive tzdata
CMD ["sh", "-c", "/app/log_sync /app/config/config.yaml"]
