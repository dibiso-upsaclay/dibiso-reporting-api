#FROM debian:latest
#
#RUN apt-get update && apt-get install -y texlive-full && apt-get clean
FROM texlive/texlive:TL2024-historic

# Install Python, pip and wget
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get install -y wget && \
     rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade --break-system-packages -r /code/requirements.txt

# copy python files
COPY ./app/*.py /code/app/

CMD ["fastapi", "run", "app/main.py", "--host", "0.0.0.0", "--port", "8000"]
