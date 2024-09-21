# Timeseries BigTable Forecasting

## Overview

This is a code sample of how to use [BigTable Counters](https://cloud.google.com/blog/products/databases/distributed-counting-with-bigtable) to store time series data that can scale up to extremely large scale.  It also shows an example of how to pull the data and utilize [Prophet](https://facebook.github.io/prophet/) to forecast that data.

![Sample Screenshot](./images/screenshot.png)

## Prerequisites

### Python 3.7+
### Go 1.22.2+
### Gcloud setup

## Setup


### Install Python Dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Install Go Dependencies

```bash
go get
```

## Usage

### Spin up BigTable

```bash
make btcreate
```

### Write BigTable Data

```bash
go run btwrite.go  --project <PROJECT_NAME>
```
### Read BigTable Data

```bash
./readip.py  --project_id <PROJECT_NAME>
```

### Spin down BigTable

```bash
make btdelete
```
