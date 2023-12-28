FROM ubuntu:24.04
ADD ./log_sync /app/log_sync
RUN chmod +x /app/log_sync
CMD ["sh", "-c", "/app/log_sync /app/config/config.yaml"]
