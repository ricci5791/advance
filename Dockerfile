FROM datamechanics/spark:3.2.0-latest

#RUN pip install --upgrade pip

RUN pip install pyspark==3.2.0

COPY datasets/ datasets/

COPY /src .

CMD ["python", "main.py"]