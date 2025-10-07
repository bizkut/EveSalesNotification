# --- Builder Stage ---
# This stage is only for getting the git commit hash
FROM python:3.12-slim as builder

WORKDIR /app

# Install git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy the .git directory to get the commit hash
COPY .git ./.git

# Write the short commit hash to a file
RUN git rev-parse --short HEAD > /app/git_hash.txt


# --- Final Stage ---
# This is the final, clean image for the application
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the git hash from the builder stage
COPY --from=builder /app/git_hash.txt /app/git_hash.txt

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code to the working directory
COPY . .

# Command to run the application
CMD ["python", "bot.py"]