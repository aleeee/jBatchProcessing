# jBatchProcessing
tools to process resource intensive and large amount of data in parallel using spring batch

it reads companies profile from database and generates stock history csv file for each symbol.

```mermaid
graph LR
A[Job] --> B[Step]
B --> C{Partitioner}
C --> D[Worker]
C --> E[Worker]
