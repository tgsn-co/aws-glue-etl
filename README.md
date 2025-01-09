# aws-glue-etl

In this repository you will find the code for all AWS Glue jobs.

## Introduction

AWS Glue is a AWS managed service that helps you easily prepare and transform data from multiple sources. You can use it for analytics, machine learning, and application development. 

In turn, a AWS Glue job can be considered a task that has the ability to extract, transform and load data, making it ready for analysis. This task is a combination of commands, written in a programming language, that can either by Pyhton, SQL or any other language suppoorted by AWS Glue.

## Repository structure

You will find one folder per AWS Glue job. The name of each folder is the same as the name of the job created in the AWS Glue console. The naming convention is defined in https://www.notion.so/Best-practices-for-naming-AWS-objects-153e91ee003680718528daf8435860d3?pvs=4

As per the naming convention, the job name should contain a reference to the project it belongs to and the target layer the data is going to be saved at. The data infrastructure created across all projects follows a Medallion architecture, meaning that an end to end pipeline saves the different stages of data transformation in different layers. These layers are generally landing, bronze, silver and gold layers. For more information on the architecture refer to https://www.notion.so/Tech-stack-129e91ee003680108e00c421704ff7d8?pvs=4

## Important notes

There are different kinds of jobs in AWS Glue, namely notebook jobs, script jobs and visual jobs. Generally, the difference between them is the way the code is written, either in a notebook or script format, or using the visual editor provided by the AWS Glue console.

Notebook jobs do not support versioning controll directly from the AWS console. As such, the code written in a Notebook job, needs to be manually commited to this repo. To do so, follow the steps mentioned on the next sub-section.

Any other job types, as script and visual jobs, can be committed using the AWS console. The details of the repo need to be added to the versioning tab every time we log into AWS. The steps to commit the code in these cases is explained in the console.

## Commit Notebook jobs

1 - If you are committing a job for the first time, start by creating a folder in the repo with the same name as the job. 

2 - After saving the notebook job, click 'Download Notebook', the third button to the left of the 'Run' button.

3 - Save the downloaded file in the corresponding repo folder, overwriting it if a file already exists.

4 - Commit the new code to github with an informative message, to the develop branch. 

All jobs should be committed to the develop branch by default. Once the job is ready to be used in a production setting a pull request (PR) can be made to the main branch.
