# alpine, not debian: trixie ships librdkafka 2.8.0, too old for KIP-848
FROM php:8.4-cli-alpine
COPY --from=mlocati/php-extension-installer /usr/bin/install-php-extensions /usr/local/bin/
RUN install-php-extensions rdkafka
WORKDIR /app
