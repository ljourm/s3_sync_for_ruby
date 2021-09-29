FROM ruby:2.7.4

ENV APP_ROOT /app

RUN mkdir $APP_ROOT
WORKDIR $APP_ROOT

COPY ./Gemfile $APP_ROOT
COPY ./Gemfile.lock $APP_ROOT

RUN bundle install
