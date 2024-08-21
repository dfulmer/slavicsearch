FROM debian:bullseye

LABEL maintainer="dfulmer@umich.edu"

RUN apt-get update -yqq && apt-get install -yqq wget --no-install-recommends \
  build-essential\
  perl\
  cpanminus\
  libxml2-dev\
  libxslt1-dev\
  libyaz-dev\
  yaz

RUN wget --no-check-certificate http://ftp.indexdata.dk/pub/yaz/yaz-5.30.0.tar.gz
RUN tar -xzf yaz-5.30.0.tar.gz
RUN cd yaz-5.30.0 && ./configure && make && make install

RUN cpanm MARC::Batch
RUN cpanm MARC::Lint
RUN cpanm Net::Z3950::ZOOM
RUN cpanm Dotenv

ARG UNAME=app
ARG UID=1000
ARG GID=1000

WORKDIR /app
ENV PERL5LIB=/app/lib