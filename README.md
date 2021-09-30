# koro

[![Go](https://github.com/nabeken/koro/actions/workflows/go.yml/badge.svg)](https://github.com/nabeken/koro/actions/workflows/go.yml)

`koro` is a simple DynamoDB Streams reader for Go.

## Motivation

In real environment, most of people prefers to Lambda function to read DynamoDB Streams because the lambda handles shards management. The lambda function itself is kept in stateless.
I'd want to have a similar setup in local environment (mainly for testing) but there is no Lambda runtime environment that works with DynamoDB Steams.

`koro` is built to allow to read DynamoDB Streams on DynamoDB Local with less hustle.

## Example

See [`stream_test.go`](stream_test.go) for the detail.
