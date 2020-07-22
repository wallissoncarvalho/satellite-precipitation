FROM python:3.8-slim-buster

#SETTING THE PYTHON UNBUFFERED ENVIROMENT VARIABLE
#What this does is it tells python to run in unbuffered mode which is recommended when running Python within a Docker Containers
#The reason for this is that it doesn't allow Python to buffer the outputs
ENV PYTHONUNBUFFERED 1

#MAKING A DIRECTORY WITHIN OUR DOCKER IMAGE THAT WE CAN USE TO STORE THE APPLICATION SOURCE CODE
#CREATING AN EMPY FOLDER FOR THE CODE ON THE DOCKER IMAGE
RUN mkdir /code
#SWITCHING  TO THE CODE FOLDER AS THE DEFAULT DIRECTORY
WORKDIR /code
#COPYING FILES FROM THE LOCAL MACHINE TO THE CODE FOLDER IN THE DOCKER IMAGE
COPY ./ /code

#TAKING THE REQUIREMENTS FILE THAT WE'VE JUST COPIED AND IT INSTALLS USING PIP INTO THE DOCKER IMAGE
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install -r requirements.txt