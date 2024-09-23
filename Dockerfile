FROM python:3.10-alpine

ARG REPO_DIR

EXPOSE 8000

ARG version
ENV VERSION=$version


RUN apk --no-cache add \
  git \
  gcc \
  libc-dev \
  build-base \
  linux-headers \
  libpq-dev \
  dumb-init

RUN pip install \
  pipenv

RUN addgroup -S -g 55746 api \
  && adduser -h /app -S -D -g '' -G api -u 55746 api

WORKDIR /app

USER api

COPY --chown=api:api . /app

RUN pipenv install --deploy

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD pipenv run python main.py