FROM ruby:3.4 AS base
ENV RUBY_YJIT_ENABLE=1
WORKDIR /app

FROM base AS prod
COPY Gemfile .
COPY Gemfile.lock .
RUN bundle install
COPY . .
EXPOSE 3000
CMD ["bundle", "exec", "falcon", "serve", "--bind", "http://0.0.0.0:3000", "-n", "4"]
