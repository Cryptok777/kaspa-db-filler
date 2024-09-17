FROM python:3.10-alpine

WORKDIR /app

RUN apk --no-cache add \
  git \
  gcc \
  libc-dev \
  build-base \
  linux-headers \
  libpq-dev \
  dumb-init

# Install pipenv
RUN pip install pipenv

# Copy only the Pipfile and Pipfile.lock first to leverage Docker cache
COPY Pipfile Pipfile.lock ./

# Install dependencies
RUN pipenv install --deploy --system

# Copy the rest of the application
COPY . .

# Use dumb-init as the entrypoint
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Command to run the application
CMD ["pipenv", "run", "python", "main.py"]