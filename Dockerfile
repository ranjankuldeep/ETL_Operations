# Start with the OpenJDK image that includes Java
FROM openjdk:11-slim

# Install Python and any required packages
RUN apt-get update && apt-get install -y python3 python3-pip procps && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy requirements file and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Set JAVA_HOME (already set for openjdk images) and PYTHONPATH
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set the default command to run the main.py in the Operation folder
CMD ["python3", "Operation/main.py"]

