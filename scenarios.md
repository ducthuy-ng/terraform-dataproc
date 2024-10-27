# Business Scenario

## What do our business do
- A B2B company, working with retailer

# Spark

## Introduction
What is Spark
Spark is an analytic - data processing engine

## Spark for business use case
- What are we intending to use Spark for
- A diagram representing data flow

## Estimation
- What are the benefits of this approach?
    - Separating between processing engine and storage layer
    - Can perform complex ETL before ingesting to BigQuery
- What are the drawbacks
    - May not perform well
    - Increase complexity in cost

# Big Data properties
(Chỗ này em làm rõ 4 đặc điểm big data)

## Provided data
- What's in the raw data?
    - Customer dimension
    - Product dimension
    - Transaction Headers & Transaction Line

## Data Volume
- Size of customer file: ? GB
- Size of product ...
- ....

## Data Velocity
- Update once per day

## Data Veracity
(Hơi mơ hồ, nhưng mà mình sẽ đề cập tới những cái checks, đảm bảo data sạch)
- Data Quality requires checks => writing SQL script
- Monitor consistently

## Value
- Use customer transactions => generate ML model => predict customer behaviors
- Visualize sales, KPI, business metrics