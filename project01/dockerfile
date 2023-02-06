#Base image of python:3.9
FROM python:3.9

#Equivalent of "cd/app" from our host machine
WORKDIR /app

#Copy everything into /app
COPY . /app

#Installs the dependencies. Passes in a text file.
RUN pip install -r requirements.txt

#Runs with docker container
ENTRYPOINT ["python", "src/main.py"]