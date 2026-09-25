FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root
COPY jobs/ /opt/jobs/
RUN sed -i 's/\r$//' /opt/jobs/enviar.sh && chmod +x /opt/jobs/enviar.sh
USER spark
